// Copyright 2019 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @fileoverview Tentacles main class.
 */

'use strict';

const { nanoid } = require('nanoid');
const {
  firestore: { DataSource, Database, DEFAULT_DATABASE, getFirestoreDatabase },
  pubsub: { EnhancedPubSub },
  storage: { StorageFile },
  cloudfunctions: {
    ValidatedStorageFile,
    PubsubMessage,
    CloudFunction,
    MainFunctionOfStorage,
    moveAndProcessFile,
    validatedStorageTrigger,
  },
  utils: { getProperValue, wait, getLogger, replaceParameters, BatchResult },
} = require('@google-cloud/nodejs-common');

const { getApiHandler, getApiOnGcs, ApiHandlerFunction, ApiConfigItem } =
  require('./api_handlers/index.js');
const { ApiConfigDao } = require('./api_config/api_config_dao.js');
const { ApiLockDao } = require('./api_lock/api_lock_dao.js');
const { TentaclesFile, TentaclesFileDao, TentaclesFileStatus } =
  require('./tentacles_file/tentacles_file_dao.js');
const { TentaclesTask, TentaclesTaskDao, TentaclesTaskStatus } =
  require('./tentacles_task/tentacles_task_dao.js');

/**
 * The maximum length (megabyte) for original (before encoding) message.
 * Pub/Sub will encode the message string with base64. The ratio of output
 * bytes to input bytes is 4:3. (source: https://en.wikipedia.org/wiki/Base64)
 * So theoretically, the maximum length for original message would be:
 * 10 * 3 / 4 = 7.5 MB.
 * For Pub/Sub's message size, see:
 * https://cloud.google.com/pubsub/quotas#resource_limits
 * @type {number}
 */
const MESSAGE_MAXIMUM_SIZE = 7;

/** Cloud Storage file maximum size (MB). Different APIs may vary. */
const STORAGE_FILE_MAXIMUM_SIZE = 999;

/** Default value of how many tasks to be processed in `initiator`. */
const INIT_TASK_BATCH = 40;

/**
 * Results of a transport execution.
 * @enum {string}
 */
const TransportResult = {
  NO_SOURCE_TOPIC: 'no_source_topic',
  NO_LOCK: 'no_lock',
  DONE: 'done',
  DUPLICATED: 'duplicated',
  TIMEOUT: 'timeout',
};

/**
 * @typedef {{
 *   namespace:string,
 *   apiConfigDao:!ApiConfigDao,
 *   apiLockDao:!ApiLockDao,
 *   tentaclesFileDao:!TentaclesFileDao,
 *   tentaclesTaskDao:!TentaclesTaskDao,
 *   pubsub:!EnhancedPubSub,
 *   getStorage:{function(string,string):!StorageFile},
 *   validatedStorageTrigger:
 *       {function(MainFunctionOfStorage,string,string=):!CloudFunction},
 *   getApiHandler: {function(string):(!ApiHandlerFunction|undefined)},
 * }}
 */
let TentaclesOptions;

/**
 * Tentacles solution is composed with four Cloud Functions:
 *
 * 1. 'initiator', which is triggered by Cloud Storage event, validates
 * the coming file. After that, it slices and publishes the data as messages to
 * 'source queue' and sends a 'nudge' message to start the sending out process.
 *
 * 2. 'transporter', which is triggered by new 'nudge' messages, pulls
 * one message from the 'source queue' and publishes to the 'sending-out queue'.
 *
 * 3. 'apiRequester', which is triggered by messages from the 'sending-out
 * queue', sends the data to the corresponding API endpoint with proper
 * configuration based on the incoming file name.
 *
 * 4. 'fileJobManager', which is a HTTP based Cloud Functions which take in the
 * parameters of a Cloud Storage file and process it like 'initiator', e.g.
 * creating a TentaclesFile and related TentaclesTasks, sending data messages to
 * target Pub/Sub topic and starting the 'transporting' process, etc. It also
 * offers the status of how the file was processed when it is given the Id of
 * a TentaclesFile.
 */
class Tentacles {
  /**
   * Initializes the Tentacles instance.
   * @param {!TentaclesOptions} options
   */
  constructor(options) {
    /** @const {!TentaclesOptions} */ this.options = options;
    /** @const {string} */ this.namespace = options.namespace;
    /** @const {!ApiConfigDao} */ this.apiConfigDao = options.apiConfigDao;
    /** @const {!ApiLockDao} */ this.apiLockDao = options.apiLockDao;
    /** @const {!TentaclesFileDao} */ this.tentaclesFileDao = options.tentaclesFileDao;
    /** @const {!TentaclesTaskDao} */ this.tentaclesTaskDao = options.tentaclesTaskDao;
    /** @const {!EnhancedPubSub} */ this.pubsub = options.pubsub;
    /** @const {!Logger} */ this.logger = getLogger('T.MAIN');
    this.initTaskBatch = options.initTaskBatch
      || process.env['INIT_TASK_BATCH'] || INIT_TASK_BATCH;
  }

  /**
   * Gets the Cloud Functions 'initiator' which loads data from the incoming
   * Cloud Storage file and sends out as messages to Pub/Sub.
   * @param {string} outbound The folder that this function monitors.
   * @return {!CloudFunction} The Cloud Functions 'initiator'.
   */
  getInitiator(outbound) {
    return this.options.validatedStorageTrigger(
      this.getLoadFileAndNudgeFn(), outbound);
  }

  /**
   * Gets the function to load data from Google Cloud Storage (GCS) file and
   * slices into pieces then sends pieces as messages to Pub/sub. After that,
   * sends a 'nudge' message to start the process to send out data to target
   * API system.
   * @param {object} adhocConfig Specified config object to create a new
   *   TentaclesFile job. By default, Tentacles is driven by GCS events which
   *   has the file name. Tentacles will extra the `attributes`,
   *   including `api`, `config`, `size`, `dryRun`, etc. from the file name and
   *   load the ApiConfig based on `api` and `config`.
   *   A HTTP-based CF has been introduced to let other program can send more
   *   information (than GCS events) to start a TentaclesFile job. In this way,
   *   this parameter is used to offer explicit configuraiton information,
   *   including `attributes` and `config`.
   * @param {object} adhocConfig.attributes The property `api` is required if
   *   this object is present.
   * @param {!ApiConfigItem|undefined} adhocConfig.config The Api config object.
   *
   * @return {!MainFunctionOfStorage}
   */
  getLoadFileAndNudgeFn(adhocConfig = {}) {
    /**
     * Loads the GCS file and sent to Pub/Sub as messages, then sends a 'nudge'
     * message to start the sendibng process.
     * @param {!ValidatedStorageFile} file Cloud Storage file information.
     * @return {!Promise<string|undefined>} The Id of TentaclesFile.
     */
    return async (file) => {
      const attributes = adhocConfig.attributes || getAttributes(file.name);
      attributes.gcs = getApiOnGcs().includes(attributes.api).toString();
      attributes.topic = getTopicNameByApi(this.namespace, attributes.api);
      this.logger.debug(`Attributes from [${file.name}]: `, attributes);
      const fileEntity = Object.assign({ attributes }, file);
      const fileId = (await this.tentaclesFileDao.createFile(fileEntity)).toString();
      this.logger.debug(`Incoming file is logged as [${fileId}].`);
      try {
        const { api, config } = attributes;
        if (!this.options.getApiHandler(api)) {
          throw new Error(`Unknown API: ${api}.`);
        }
        const apiConfig = adhocConfig.config ||
          await this.apiConfigDao.getConfig(api, config);
        if (!apiConfig) {
          throw new Error(`API[${api}] has unknown config: ${config}.`);
        }
        if (file.size === 0) {
          this.logger.warn(`Empty file: ${file.name}.`);
        }
        /** @type {TentaclesTask} */
        const taskBaseInfo = Object.assign({ fileId }, attributes);
        let result;
        if (attributes.gcs === 'true') {
          result = await this.sendFileInfoToMessage_(file, taskBaseInfo, apiConfig);
        } else {
          result = await this.sendDataToMessage_(file, taskBaseInfo, apiConfig);
        }
        await this.tentaclesFileDao.updateFile(fileId, result);
        // The whole file be held if some of its tasks failed.
        if (result.status === TentaclesFileStatus.ERROR) {
          throw new Error(`Errors in send out data.`);
        }
        const fullFilePath = `gs://${file.bucket}/${file.name}`;
        await this.nudge(`After publish of ${fullFilePath}`,
          { topic: attributes.topic });
        await this.tentaclesFileDao.updateFile(fileId, {
          status: TentaclesFileStatus.STARTED,
          startSendingTime: new Date(),
        });
        return fileId;
      } catch (error) {
        this.logger.error(`Error in ${file.name}: `, error);
        await this.tentaclesFileDao.logError(fileId, error.message);
        throw new Error(`File ${fileId} failed: ${error.message}`);
      }
    };
  }

  /**
   * Saves Tentacles Task and sends out a message with the Task ID in the
   * message's attributes.
   * @param {!TentaclesTask} taskEntity Task information.
   * @param {string} data Pub/Sub message string.
   * @param {!ApiConfigItem} apiConfig Api config object.
   * @return {!Promise<boolean>} Whether the whole process (create a task, send
   *     out message and update the message id back to the task) succeeded.
   * @private
   */
  async saveTaskAndSendData_(taskEntity, data, apiConfig) {
    const taskId =
      (await this.tentaclesTaskDao.createTask(taskEntity)).toString();
    const messageAttributes = Object.assign(
      { taskId, apiConfigJson: JSON.stringify(apiConfig) }, taskEntity);
    const dataMessageId = await this.pubsub.publish(
      taskEntity.topic, data, messageAttributes);
    return this.tentaclesTaskDao.updateTask(taskId, { dataMessageId });
  }

  /**
   * Based on the size of bytes in one record/line and the qps of config, gets
   * the best split size (MB) of a task.
   * @param {StorageFile} storageFile
   * @param {!TentaclesTask} taskBaseInfo Task information.
   * @param {!ApiConfigItem} apiConfig Api config object.
   * @return {number}
   */
  async getSizeDynamically(storageFile, taskBaseInfo, apiConfig) {
    const { api } = taskBaseInfo;
    const apiHandler = this.options.getApiHandler(api);
    const { recordsPerRequest, qps } = apiHandler.getSpeedOptions(apiConfig);

    const maxDataForMessage =
      await storageFile.loadContent(0, MESSAGE_MAXIMUM_SIZE * 1000 * 1000);
    const lines = maxDataForMessage.split('\n');
    const averageSizePerLine = maxDataForMessage.length / lines.length;
    const maxLinesToSend = recordsPerRequest * qps * 500 // ~500s running time
    const batchSize = maxLinesToSend * averageSizePerLine;
    const safeSize = batchSize / 2; // Take half of a batch size as a safe value.
    this.logger.debug('averageSizePerLine', averageSizePerLine);
    this.logger.debug('maxLinesToSend', maxLinesToSend);
    this.logger.debug('batchSize', batchSize);
    this.logger.debug('safeSize', safeSize);
    this.logger.info(`Get dynamic size for ${storageFile.fileName}`, safeSize);
    return safeSize / 1000 / 1000;
  }

  /**
   * Splits the ingested file into messages and sends to Pub/Sub.
   * @param {!ValidatedStorageFile} file Information of the ingested file.
   * @param {!TentaclesTask} taskBaseInfo Task information.
   * @param {!ApiConfigItem} apiConfig Api config object.
   * @return {!Promise<TentaclesFile>} Updated data in TentaclesFile entity.
   * @private
   */
  async sendDataToMessage_(file, taskBaseInfo, apiConfig) {
    const { bucket, name: fileName, size: fileSize } = file;
    const { size, topic } = taskBaseInfo;
    const storageFile = this.options.getStorage(bucket, fileName);
    const sizeAsFloat = parseFloat(size);
    let dynamicSize;
    if (sizeAsFloat === 0 || typeof size === 'undefined') {
      dynamicSize =
        await this.getSizeDynamically(storageFile, taskBaseInfo, apiConfig);
    }
    const messageMaxSize = Math.floor(1000 * 1000 *
      getProperValue(dynamicSize || sizeAsFloat, MESSAGE_MAXIMUM_SIZE));
    this.logger.info(`Split data size: ${messageMaxSize}`);
    if (fileSize / 500 > messageMaxSize) {
      this.logger.warn(`High risk to get timeout for ${fileName}`,
        `file [${fileSize}], messageMaxSize[${messageMaxSize}]`);
    }
    const splitRanges =
      await storageFile.getSplitRanges(fileSize, messageMaxSize);
    this.logger.info('splitRanges', splitRanges);
    const sendDataResult = {
      numberOfTasks: splitRanges.length,
      failedTasks: [],
    };
    let currentResult = true;
    const batchSize = this.initTaskBatch;
    for (let i = 0; i < Math.ceil(splitRanges.length / batchSize); i++) {
      const startIndex = i * batchSize;
      const batchRanges = splitRanges.slice(startIndex, startIndex + batchSize);
      const results = await Promise.all(
        batchRanges.map(async ([start, end], index) => {
          const rawData = await storageFile.loadContent(start, end);
          // Handle the rare case when there is a BOM at the beginning of the file.
          // 'Use of a BOM is neither required nor recommended for UTF-8'.
          // Source: http://www.unicode.org/versions/Unicode5.0.0/ch02.pdf
          const data = (start === 0 && rawData.charCodeAt(0) === 0xFEFF)
            ? rawData.slice(1) : rawData;
          const taskEntity = Object.assign(
            { start: start.toString(), end: end.toString() }, taskBaseInfo);
          this.logger.debug(
            `[${startIndex + index}] Send ${data.length} bytes to Topic[${topic}].`);
          try {
            const result =
              await this.saveTaskAndSendData_(taskEntity, data, apiConfig);
            if (!result) {
              sendDataResult.failedTasks.push(
                { start, end, reason: 'Fail to update task.' });
            }
            return result;
          } catch (error) {
            sendDataResult.failedTasks.push({ start, end, reason: error.message })
          }
        }));
      currentResult = currentResult && results.every((result) => result);
    }
    if (currentResult !== true) {
      sendDataResult.status = TentaclesFileStatus.ERROR;
    }
    return sendDataResult;
  }

  /**
   * Splits the ingested file into Cloud Storage files based on the given size
   * limitation and sends the split files information as the messages to
   * Pub/Sub.
   * @param {!ValidatedStorageFile} file Information of the ingested file.
   * @param {!TentaclesTask} taskBaseInfo Task information.
   * @param {!ApiConfigItem} apiConfig Api config object.
   * @return {!Promise<TentaclesFile>} Updated data in TentaclesFile entity.
   * @private
   */
  async sendFileInfoToMessage_(file, taskBaseInfo, apiConfig) {
    const { bucket, name: fileName } = file;
    const storageFile = this.options.getStorage(bucket, fileName);
    const { size, topic } = taskBaseInfo;
    const gcsSplitSize = 1000 * 1000 *
      getProperValue(parseFloat(size), STORAGE_FILE_MAXIMUM_SIZE, false);
    this.logger.debug(`Split file into size: ${gcsSplitSize}`);
    const slicedFiles = await storageFile.split(gcsSplitSize);
    const sendDataResult = {
      numberOfTasks: slicedFiles.length,
      failedTasks: [],
    };
    const reducedFn = async (previous, slicedFile, index) => {
      const previousResult = await previous;
      const data = JSON.stringify({ file: slicedFile, bucket });
      const taskEntity = Object.assign({ slicedFile }, taskBaseInfo);
      this.logger.debug(`[${index}] Send ${data} to Topic[${topic}].`);
      try {
        const currentResult =
          await this.saveTaskAndSendData_(taskEntity, data, apiConfig);
        if (!currentResult) {
          sendDataResult.failedTasks.push(
            { slicedFile, reason: 'Fail to update task.' });
        }
        return currentResult && previousResult;
      } catch (error) {
        sendDataResult.failedTasks.push({ slicedFile, reason: error.message })
      }
    };
    const result = await slicedFiles.reduce(reducedFn, true);
    if (result !== true) {
      sendDataResult.status = TentaclesFileStatus.ERROR;
    }
    return sendDataResult;
  }

  /**
   * A HTTP based Cloud Functions which manages TentaclesFile job:
   * 1. checks (updates if necessary) and returns the status of a TentaclesFile
   *    job with the given `fileId`;
   * 2. or takes in the parameters of a Cloud Storage file and creates a new
   *    TentaclesFile job, just like the Cloud Function 'initiator'.
   * @param {object} request
   * @param {object} response
   */
  async fileJobManager(request, response) {
    const parameters = request.body;
    this.logger.info('Http get:', parameters);
    const { fileId, file } = parameters;
    if (fileId) {
      const tentaclesFile = await this.tentaclesFileDao.load(fileId);
      if (!tentaclesFile) {
        response.send({ error: `Can not find TentacleFile id: ${fileId}.` });
      }
      if (tentaclesFile.status === TentaclesFileStatus.STARTED) {
        await this.updateStartedFileStatus(fileId, tentaclesFile.numberOfTasks);
      } else if (tentaclesFile.status === TentaclesFileStatus.QUEUING) {
        await this.updateQueuingFileStatus(fileId,
          tentaclesFile.startQueuingTime);
      }
      const updatedFile = await this.tentaclesFileDao.load(fileId);
      response.send({
        fileId,
        status: updatedFile.status,
        error: updatedFile.error,
        numberOfFailed: updatedFile.numberOfFailed,
        numberOfLines: updatedFile.numberOfLines,
        numberOfTasks: updatedFile.updatedFile,
        hasError: updatedFile.hasError,
      });
    } else if (file) {
      const regex = /^gs:\/\/([^\/]*)\/(.*)$/g;
      const matches = regex.exec(file);
      const bucket = matches[1];
      const fileName = matches[2];
      this.logger.info(`Http get ${bucket}, ${fileName}`);
      try {
        const fileObj =
          await this.options.getStorage(bucket, fileName).getFile().get();
        const { attributes, config } = parameters;
        const fileId = await moveAndProcessFile(
          this.getLoadFileAndNudgeFn({ attributes, config }), fileObj[1], 'Http');
        response.send({ fileId });
      } catch (error) {
        this.logger.error('Http failed', error);
        response.send({ error: error.message });
      }
    } else {
      this.logger.error('Unknow data', parameters);
      response.send({
        error: `Missing data in request, ${JSON.stringify(parameters)}`,
      });
    }
  }

  /**
   * Updates the status of a TentaclesFile which is at 'queuing' state which
   * should not last longer than 9 minutes. It will make the TentaclesFile as
   * error if it is over 9 minutes.
   * @param {string} fileId
   * @param {object} startQueuingTime
   */
  async updateQueuingFileStatus(fileId, startQueuingTime) {
    const minutesPassed = (new Date() - startQueuingTime.toDate()) / 1000 / 3600;
    if (minutesPassed > 9) {
      await this.tentaclesFileDao.logError(fileId, 'Queuing timeout.');
    }
  }

  /**
   * Updates the status of a TentaclesFile which is at 'started' state.
   * @param {string} fileId
   * @param {number} numberOfTasks
   */
  async updateStartedFileStatus(fileId, numberOfTasks) {
    const filter = { property: 'fileId', value: fileId };
    const tasks = await this.tentaclesTaskDao.list([filter]);
    if (tasks.length !== numberOfTasks) {
      await this.tentaclesFileDao.logError(
        fileId, `Wrong number of tasks: ${tasks.length}`);
    } else {
      const isRunning = tasks.some(({ entity: { status } }) => {
        return status === TentaclesTaskStatus.QUEUING
          || status === TentaclesTaskStatus.TRANSPORTED
          || status === TentaclesTaskStatus.SENDING;
      });
      if (!isRunning) {
        let numberOfFailed = 0, numberOfLines = 0;
        let hasError = false;
        tasks.forEach(({ entity }) => {
          numberOfFailed += entity.numberOfFailed || 0;
          numberOfLines += entity.numberOfLines || 0;
          if (entity.status === TentaclesTaskStatus.ERROR) hasError = true;
        });
        await this.tentaclesFileDao.updateFile(fileId, {
          numberOfFailed,
          numberOfLines,
          hasError,
          //Only mark as error if all lines failed.
          status: numberOfLines === numberOfFailed
            ? TentaclesFileStatus.ERROR : TentaclesFileStatus.DONE,
        });
      }
    }
  }

  /**
   * Gets the Cloud Functions 'transporter' which is triggered by a 'nudge'
   * message. The Cloud Functions will take one message from the 'source queue'
   * topic and push the message to the 'sending-out' topic.
   * @param {number=} timeout Idle time (seconds) for this function to wait for
   *     a new message from the 'source queue'. The default value is 60.
   * @param {string=} targetTopic The name of topic that this function will
   *     push to. The default value is 'namespace' followed by '-push'.
   * @return {!CloudFunction} The Cloud Functions 'transporter'.
   */
  getTransporter(timeout = 60, targetTopic = `${this.namespace}-push`) {
    /** @type {!CloudFunctionNode8} */
    const transportMessage = async (message, context) => {
      const attributes = message.attributes || {};
      const messageId = context.eventId;
      if (!attributes.topic) {
        this.logger.warn(`There is no source topic: ${messageId}`);
        return TransportResult.NO_SOURCE_TOPIC;
      }
      const sourceTopic = attributes.topic;
      const lockToken = nanoid();
      const getLocked = await this.apiLockDao.getLock(sourceTopic, lockToken);
      if (!getLocked) {
        this.logger.warn(
          `There is no available lock for ${sourceTopic}. QUIT.`);
        return TransportResult.NO_LOCK;
      }
      const data = Buffer.from(message.data, 'base64').toString();
      this.logger.debug(
        `Get nudge message[${messageId}]:${data}. Transporting ${sourceTopic}`);
      const result =
        await this.passOneMessage_(sourceTopic, lockToken, timeout, targetTopic);
      this.logger.debug(
        `Nudge message[${messageId}] transport results: ${result}`);
      if (result === TransportResult.TIMEOUT) {
        await this.apiLockDao.unlock(sourceTopic, lockToken);
        this.logger.info(`There is no new message in ${sourceTopic}. QUIT`);
        return TransportResult.TIMEOUT;
      }
      if (result === TransportResult.DUPLICATED) {
        await this.apiLockDao.unlock(sourceTopic, lockToken);
        this.logger.warn(`Duplicated message[${messageId}] for ${sourceTopic}.`);
        await this.nudge(
          `Got a duplicated message[${messageId}], ahead next.`, attributes);
        return TransportResult.DUPLICATED;
      }
      if (result === TransportResult.DONE) {
        if (await this.apiLockDao.hasAvailableLock(sourceTopic)) {
          this.logger.info(
            `Continue as there is available lock ${sourceTopic}.`);
          await this.nudge(
            `Continue from message[${messageId}], more locks available.`,
            attributes);
        }
      }
      return result;
    };
    return transportMessage;
  }

  /**
   * Uses pull mode to fetch one message from the 'source' topic and sends to
   * the target topic. If there is no new message coming, this method will wait
   * for the seconds set by the arg 'timeout' before it exits.
   * @param {string} sourceTopic Name of 'source' topic.
   * @param {string} lockToken Token if current lock. This will be saved
   * @param {number} timeout Idle time in seconds.
   * @param {string} targetTopic Name of target topic.
   * @return {!Promise<!TransportResult>} Result of this execution.
   * @private
   */
  async passOneMessage_(sourceTopic, lockToken, timeout, targetTopic) {
    const subscriptionName = `${sourceTopic}-holder`;
    /**
     * Gets the message handler function for the pull subscription.
     * @param {function(*)} resolver Function to call when promise is fulfilled.
     * @return {function(!PubsubMessage):!Promise<!TransportResult>}
     */
    const getMessageHandler = (resolver) => {
      return async (message) => {
        const { id, length, attributes } = message;
        const messageTag = `[${id}]@[${sourceTopic}]`;  // For log.
        this.logger.debug(`Received ${messageTag} with data length: ${length}`);
        const taskId = attributes.taskId;
        attributes.lockToken = lockToken;
        const transported = await this.tentaclesTaskDao.transport(taskId);
        if (transported) {
          const messageId = await this.pubsub.publish(targetTopic,
            Buffer.from(message.data, 'base64').toString(), attributes);
          this.logger.debug(
            `Forward ${messageTag} as [${messageId}]@[${targetTopic}]`);
          await this.pubsub.acknowledge(subscriptionName, message.ackId);
          await this.tentaclesTaskDao.updateTask(taskId,
            { apiMessageId: messageId, lockToken });
          resolver(TransportResult.DONE);
        } else {
          this.logger.warn(
            `Wrong status for ${messageTag} (duplicated?) Task [${taskId}].`);
          await this.pubsub.acknowledge(subscriptionName, message.ackId);
          resolver(TransportResult.DUPLICATED);
        }
      };
    };
    const subscription = await this.pubsub.getOrCreateSubscription(
      sourceTopic, subscriptionName,
      { ackDeadlineSeconds: 300, flowControl: { maxMessages: 1 } });
    this.logger.debug(`Get subscription ${subscription.name}.`);
    const subscriber = new Promise((resolver) => {
      this.logger.debug(`Add messageHandler to Subscription:`, subscription);
      subscription.once(`message`, getMessageHandler(resolver));
    });
    const result = await Promise.race([
      subscriber,
      wait(timeout * 1000, TransportResult.TIMEOUT),
    ]);
    this.logger.debug(`Remove messageHandler after ${result}.`);
    subscription.removeAllListeners('message');
    return result;
  }

  /**
   * Gets the Cloud Functions 'apiRequester' which sends out the data that are
   * pushed to this function as Pub/Sub events data.
   * @return {!CloudFunction} The Cloud Functions 'apiRequester'.
   */
  getApiRequester() {
    /** @type {!CloudFunctionNode8} */
    const sendApiData = async (message, context) => {
      const messageId = context.eventId;
      const { attributes, data } = message;
      const records = Buffer.from(data, 'base64').toString();
      this.logger.debug(
        `Receive message[${messageId}] with ${records.length} bytes.`);
      const {
        api,
        config,
        apiConfigJson,
        dryRun,
        appended,
        taskId,
        topic,
        lockToken,
      } = attributes || {};
      /** @type {BatchResult} */ let batchResult;
      let needContinue = true;
      try {
        const canStartTask = await this.tentaclesTaskDao.start(taskId);
        if (!canStartTask) {// Wrong status task for duplicated message?
          this.logger.warn(`Quit before sending data. Duplicated message?`);
          return;
        }
        const apiHandler = this.options.getApiHandler(api);
        if (!apiHandler) throw new Error(`Unknown API: ${api}.`);
        const apiConfig = apiConfigJson ? JSON.parse(apiConfigJson) :
          await this.apiConfigDao.getConfig(api, config);
        if (!apiConfig) {
          throw new Error(`API[${api}] has unknown config: ${config}.`);
        }
        let finalConfig; // Dynamic config
        if (!appended) {
          finalConfig = apiConfig;
        } else {
          const parameters = JSON.parse(appended);
          const finalConfigString =
            replaceParameters(JSON.stringify(apiConfig), parameters, true);
          finalConfig = JSON.parse(finalConfigString);
        }
        if (dryRun === 'true') {
          this.logger.info(`[DryRun] API[${api}] and config[${config}]: `,
            finalConfig);
          batchResult = { result: true }; // A dry-run task always succeeds.
          if (!getApiOnGcs().includes(api)) {
            batchResult.numberOfLines = records.split('\n').length;
          }
        } else {
          batchResult =
            await apiHandler.sendData(records, messageId, finalConfig);
        }
        const { numberOfLines = 0, failedLines, groupedFailed } = batchResult;
        await this.tentaclesTaskDao.updateTask(taskId, {
          numberOfLines,
          numberOfFailed: failedLines ? failedLines.length : 0,
        });
        //If the record is empty, wait extra time to keep logs sequential.
        if (numberOfLines === 0) {
          await wait(1000);
        }
        if (groupedFailed) {
          const errorLogger = getLogger('TentaclesFailedRecord');
          Object.keys(groupedFailed).forEach((error) => {
            errorLogger.info(
              JSON.stringify(
                { taskId, error, records: groupedFailed[error] }));
          });
        }
        if (batchResult.result) {
          await this.tentaclesTaskDao.finish(taskId, batchResult.result);
        } else {
          await this.tentaclesTaskDao.logError(taskId, batchResult.errors);
        }
      } catch (error) {
        this.logger.error(`Error in API[${api}], config[${config}]: `, error);
        await this.tentaclesTaskDao.logError(taskId, error);
        needContinue = !error.message.startsWith('Unknown API');
      }
      if (!topic) {
        this.logger.info('There is no topic. In local file upload mode.');
        return;
      }
      if (!needContinue) {
        this.logger.info(`Skip unsupported API ${api}.`);
      }
      try {
        return this.releaseLockAndNotify(
          topic, lockToken, messageId, needContinue);
      } catch (error) {
        // Re-do this when unknown external exceptions happens.
        this.logger.error('Exception happened while try to release the lock: ',
          error);
        await wait(10000); // wait 10 sec
        this.logger.info('Wait 10 second and retry...');
        return this.releaseLockAndNotify(
          topic, lockToken, messageId, needContinue);
      }
    }
    return sendApiData;
  }

  /**
   * Releases the lock and sends notification message for next piece of data.
   * @param {string} topic The topic name as well as the lock name.
   * @param {string} lockToken The token for this lock.
   * @param {string} messageId ID of current message.
   * @param {boolean} needContinue Whether should send notification message.
   * @return {!Promise<string|undefined>} ID of the 'nudge' message.
   */
  async releaseLockAndNotify(topic, lockToken, messageId, needContinue) {
    await this.apiLockDao.unlock(topic, lockToken);
    if (needContinue) {
      return this.nudge(`Triggered by message[${messageId}]`, { topic });
    }
  }

  /**
   * Sends a 'nudge' message to the Topic which will trigger the Cloud Functions
   * 'transporter'.
   * @param {string} message The message string.
   * @param {{topic:(string|undefined)}=} attributes Message attributes.
   * @param {string=} topicName The name of the topic that this message will be
   *     sent to. The default value is 'namespace' followed by '-trigger'.
   * @return {!Promise<string>} ID of the 'nudge' message.
   */
  nudge(message, attributes = {}, topicName = `${this.namespace}-trigger`) {
    return this.pubsub.publish(topicName, message, attributes);
  }
}

/**
 * Returns the topic name for the data of a given API name.
 * @param {string} topicPrefix Prefix of Pub/Sub topic name.
 * @param {string} apiName API name.
 * @return {string} The topic name for the data of a given API name.
 */
const getTopicNameByApi = (topicPrefix, apiName) => {
  return `${topicPrefix}-${apiName}`;
};

/**
 * Pub/Sub message attributes from a given file name.
 * @typedef {{
 *   api:string,
 *   config:string,
 *   gcs:string,
 *   size:(string|undefined),
 *   dryRun:string,
 *   appended:(string|undefined),
 * }}
 */
let FileAttributes;

/**
 * Gets the task attributes from a given file name. Here, the 'attributes'
 * means the attributes in a Pub/Sub message. Its type is Object<string,string>.
 * Here the attributes of a Tentacles File includes: API name, configuration
 * name, whether it is through Cloud Storage, file maximum size and whether it
 * is 'dryRun' mode. (In 'dryRun' mode, there is not real request sent out to
 * external server.)
 * @param {string} fileName The incoming file name.
 * @return {!FileAttributes}
 */
const getAttributes = (fileName) => {
  const attributes = {};

  const api = /API[\[|{]([\w-]*)[\]|}]/i.exec(fileName);
  if (api) attributes.api = api[1];

  const config = /config[\[|{]([\w-]*)[\]|}]/i.exec(fileName);
  if (config) attributes.config = config[1];

  const size = /size[\[|{](\d*\.?\d*)(MB?)?[\]|}]/i.exec(fileName);
  if (size) attributes.size = size[1].toString();

  attributes.dryRun = /dryrun/i.test(fileName).toString();
  // attributes.gcs = getApiOnGcs().includes(attributes.api).toString();
  // Appended parameters for config in a JSON string format.
  const appended = /appended({[^}]*})/i.exec(fileName);
  if (appended) attributes.appended = appended[1];
  return attributes;
};

/**
 * Returns a Tentacles instance based on the parameters.
 * Tentacles works on several components which depend on the configuration. This
 * factory function will seal the details in product environment and let the
 * Tentacles class be more friendly to test.
 *
 * @param {string} namespace The `namespace` of this instance, e.g. prefix of
 *     the topics, Firestore root collection name, Datastore namespace, etc.
 * @param {!Database} database The database.
 * @return {!Tentacles} The Tentacles instance.
 */
const getTentacles = (namespace, database) => {
  /** @type {TentaclesOptions} */
  const options = {
    namespace,
    apiConfigDao: new ApiConfigDao(database, namespace),
    apiLockDao: new ApiLockDao(database, namespace),
    tentaclesFileDao: new TentaclesFileDao(database, namespace),
    tentaclesTaskDao: new TentaclesTaskDao(database, namespace),
    pubsub: new EnhancedPubSub(),
    getStorage: StorageFile.getInstance,
    validatedStorageTrigger,
    getApiHandler,
  };
  console.log(
    `Init Tentacles for ns[${namespace}], ds[${JSON.stringify(database)}]`);
  return new Tentacles(options);
};

/**
 * Probes the Google Cloud Project's Firestore mode (Native or Datastore), then
 * uses it to create an instance of Tentacles.
 * @param {(string|undefined)=} namespace
 * @param {(string|undefined)=} projectId
 * @param {(string|undefined)=} databaseId
 * @param {(string|undefined)=} databaseMode
 * @return {!Promise<!Tentacles>}
 */
const guessTentacles = async (namespace = process.env['PROJECT_NAMESPACE'],
  projectId = process.env['GCP_PROJECT'],
  databaseId = process.env['DATABASE_ID'] || DEFAULT_DATABASE,
  databaseMode = process.env['DATABASE_MODE']) => {
  if (!namespace) {
    console.warn(
      'Fail to find ENV variables PROJECT_NAMESPACE, will set as `tentacles`'
    );
    namespace = 'tentacles';
  }
  if (!databaseMode) {
    console.warn(
      'Database mode is not set. Please consider upgrade this solution.');
  }
  const database = databaseMode
    ? { source: DataSource[databaseMode], id: databaseId }
    : await getFirestoreDatabase(projectId, databaseId);
  return getTentacles(namespace, database);
};

module.exports = {
  STORAGE_FILE_MAXIMUM_SIZE,
  MESSAGE_MAXIMUM_SIZE,
  TransportResult,
  TentaclesOptions,
  Tentacles,
  getAttributes,
  getTopicNameByApi,
  getTentacles,
  guessTentacles,
};
