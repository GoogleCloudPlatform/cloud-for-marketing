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

const {
  firestore: {DataSource, FirestoreAccessBase,},
  pubsub: {EnhancedPubSub},
  storage: {StorageFile},
  cloudfunctions: {
    ValidatedStorageFile,
    PubsubMessage,
    CloudFunction,
    MainFunctionOfStorage,
    adaptNode6,
    validatedStorageTrigger,
  },
  utils: {getProperValue, wait, getLogger, replaceParameters, BatchResult},
} = require('@google-cloud/nodejs-common');

const {getApiHandler, getApiOnGcs, ApiHandlerFunction} = require(
    './api_handlers/index.js');
const {getApiConfig, ApiConfig, ApiConfigJson} = require(
    './api_config/index.js');
const {getApiLock, ApiLock} = require('./api_lock/index.js');
const {getTentaclesFile, TentaclesFile, TentaclesFileEntity} =
    require('./tentacles_file/index.js');
const {getTentaclesTask, TentaclesTask, TentaclesTaskEntity} =
    require('./tentacles_task/index.js');

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
 *   apiConfig:!ApiConfig,
 *   apiLock:!ApiLock,
 *   tentaclesFile:!TentaclesFile,
 *   tentaclesTask:!TentaclesTask,
 *   pubsub:!EnhancedPubSub,
 *   getStorage:{function(string,string):!StorageFile},
 *   validatedStorageTrigger:
 *       {function(MainFunctionOfStorage,string,string=):!CloudFunction},
 *   getApiHandler: {function(string):(!ApiHandlerFunction|undefined)},
 * }}
 */
let TentaclesOptions;

/**
 * Tentacles solution is composed with three Cloud Functions:
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
 */
class Tentacles {
  /**
   * Initializes the Tentacles instance.
   * @param {!TentaclesOptions} options
   */
  constructor(options) {
    /** @const {!TentaclesOptions} */ this.options = options;
    /** @const {string} */ this.namespace = options.namespace;
    /** @const {!ApiConfig} */ this.apiConfig = options.apiConfig;
    /** @const {!ApiLock} */ this.apiLock = options.apiLock;
    /** @const {!TentaclesFile} */ this.tentaclesFile = options.tentaclesFile;
    /** @const {!TentaclesTask} */ this.tentaclesTask = options.tentaclesTask;
    /** @const {!EnhancedPubSub} */ this.pubsub = options.pubsub;
    /** @const {!Logger} */ this.logger = getLogger('T.MAIN');
  }

  /**
   * Gets the Cloud Functions 'initiator' which loads data from the incoming
   * Cloud Storage file and sends out as messages to Pub/Sub.
   * @param {string} outbound The folder that this function monitors.
   * @return {!CloudFunction} The Cloud Functions 'initiator'.
   */
  getInitiator(outbound) {
    /**
     * Loads data from Google Cloud Storage (GCS) file and slices into pieces,
     * then sends pieces as messages to Pub/sub. After that, sends a 'nudge'
     * message to start the process to send out data to target API system.
     * @param {!ValidatedStorageFile} file Validated Storage file information
     *     from the function 'validatedStorageTrigger'..
     * @return {!Promise<string>} ID of the 'nudge' message.
     */
    const loadFileAndNudge = async (file) => {
      try {
        const topic = await this.loadGcsToPs(file);
        const fullFilePath = `gs://${file.bucket}/${file.name}`;
        if (topic) {
          return this.nudge(`After publish of ${fullFilePath}`, {topic});
        }
        this.logger.error(`Something wrong with the file ${fullFilePath}.`);
      } catch (error) {
        this.logger.error(error);
      }
    };
    return this.options.validatedStorageTrigger(loadFileAndNudge, outbound);
  }

  /**
   * Loads the GCS file and sent to Pub/Sub as messages.
   * Will split the messages based on the size.
   * @param {!ValidatedStorageFile} file Validated Storage file information from
   *     the function 'validatedStorageTrigger'.
   * @return {!Promise<string|undefined>} The name of the topic that receives
   *     the data.
   */
  async loadGcsToPs(file) {
    /** @const {!Object<string,string>} Attributes in the Pub/Sub messages. */
    const attributes = getAttributes(file.name);
    attributes.topic = getTopicNameByApi(this.namespace, attributes.api);
    this.logger.debug(`Attributes from [${file.name}]: `, attributes);
    /** @const {TentaclesFileEntity} */
    const fileEntity = Object.assign({attributes}, file);
    const fileId = (await this.tentaclesFile.save(fileEntity)).toString();
    this.logger.debug(`Incoming file is logged as [${fileId}].`);
    try {
      const {api, config} = attributes;
      if (!this.options.getApiHandler(api)) {
        throw new Error(`Unknown API: ${api}.`);
      }
      const apiConfig = await this.apiConfig.getConfig(api, config);
      if (!apiConfig) {
        throw new Error(`API[${api}] has unknown config: ${config}.`);
      }
      if (file.size === 0) {
        console.warn(`Empty file: ${file.name}.`);
      }
      /** @type {TentaclesTaskEntity} */
      const taskBaseInfo = Object.assign({fileId}, attributes);
      let sendOutAllData;
      if (attributes.gcs === 'true') {
        sendOutAllData = await this.sendFileInfoToMessage_(file, taskBaseInfo);
      } else {
        sendOutAllData = await this.sendDataToMessage_(file, taskBaseInfo);
      }
      if (sendOutAllData) return attributes.topic;
      throw new Error(`Errors in send out data.`);
    } catch (error) {
      this.logger.error(`Error in ${file.name}: `, error);
      await this.tentaclesFile.saveError(fileId, error.message);
      throw new Error(`File ${fileId} failed: ${error.message}`);
    }
  }

  /**
   * Saves Tentacles Task and sends out a message with the Task ID in the
   * message's attributes.
   * @param {!TentaclesTaskEntity} taskEntity Task information.
   * @param {string} data Pub/Sub message string.
   * @return {!Promise<boolean>} Whether the whole process (create a task, send
   *     out message and update the message id back to the task) succeeded.
   * @private
   */
  async saveTaskAndSendData_(taskEntity, data) {
    const taskId = (await this.tentaclesTask.createTask(taskEntity)).toString();
    const messageAttributes = Object.assign({taskId}, taskEntity);
    const messageId = await this.pubsub.publish(
        taskEntity.topic, data, messageAttributes);
    return this.tentaclesTask.updateTask(taskId, {dataMessageId: messageId});
  };

  /**
   * Splits the ingested file into messages and sends to Pub/Sub.
   * @param {!ValidatedStorageFile} file Information of the ingested file.
   * @param {!TentaclesTaskEntity} taskBaseInfo Task information.
   * @return {!Promise<boolean>} Whether all messages sent out successfully.
   * @private
   */
  async sendDataToMessage_(file, taskBaseInfo) {
    const {bucket, name: fileName, size: fileSize} = file;
    const {size, topic} = taskBaseInfo;
    const storageFile = this.options.getStorage(bucket, fileName);
    const messageMaxSize = 1000 * 1000 *
        getProperValue(parseFloat(size), MESSAGE_MAXIMUM_SIZE);
    this.logger.debug(`Split data size: ${messageMaxSize}`);
    const splitRanges =
        await storageFile.getSplitRanges(fileSize, messageMaxSize);
    const reducedFn = async (previous, [start, end], index) => {
      const previousResult = await previous;
      const data = await storageFile.loadContent(start, end);
      const taskEntity = Object.assign(
          {start: start.toString(), end: end.toString()}, taskBaseInfo);
      this.logger.debug(
          `[${index}] Send ${data.length} bytes to Topic[${topic}].`);
      const currentResult = await this.saveTaskAndSendData_(taskEntity, data);
      return currentResult && previousResult;
    };
    return splitRanges.reduce(reducedFn, true);
  }

  /**
   * Splits the ingested file into Cloud Storage files based on the given size
   * limitation and sends the split files information as the messages to
   * Pub/Sub.
   * @param {!ValidatedStorageFile} file Information of the ingested file.
   * @param {!TentaclesTaskEntity} taskBaseInfo Task information.
   * @return {!Promise<boolean>} Whether all messages sent out successfully.
   * @private
   */
  async sendFileInfoToMessage_(file, taskBaseInfo) {
    const {bucket, name: fileName} = file;
    const storageFile = this.options.getStorage(bucket, fileName);
    const {size, topic} = taskBaseInfo;
    const gcsSplitSize = 1000 * 1000 *
        getProperValue(parseFloat(size), STORAGE_FILE_MAXIMUM_SIZE, false);
    this.logger.debug(`Split file into size: ${gcsSplitSize}`);
    const slicedFiles = await storageFile.split(gcsSplitSize);
    const reducedFn = async (previous, slicedFile, index) => {
      const previousResult = await previous;
      const data = JSON.stringify({file: slicedFile, bucket});
      const taskEntity = Object.assign({slicedFile}, taskBaseInfo);
      this.logger.debug(`[${index}] Send ${data} to Topic[${topic}].`);
      const currentResult = await this.saveTaskAndSendData_(taskEntity, data);
      return currentResult && previousResult;
    };
    return slicedFiles.reduce(reducedFn, true);
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
      const getLocked = await this.apiLock.getLock(sourceTopic);
      if (!getLocked) {
        this.logger.warn(`There are running tasks for ${sourceTopic}. QUIT.`);
        return TransportResult.NO_LOCK;
      }
      const data = Buffer.from(message.data, 'base64').toString();
      this.logger.debug(`Get nudge message[${messageId}]: ${
          data}. Will transport for [${sourceTopic}]`);
      const result =
          await this.passOneMessage_(sourceTopic, timeout, targetTopic);
      this.logger.debug(
          `Nudge message[${messageId}] transport results: ${result}`);
      if (result === TransportResult.DONE) return result;
      await this.apiLock.unlock(sourceTopic);
      if (result === TransportResult.DUPLICATED) {
        await this.nudge(
            `Got a duplicated message[${messageId}], ahead next.`, attributes);
        return TransportResult.DUPLICATED;
      }
      this.logger.info(`There is no new message in ${sourceTopic}.`);
      return TransportResult.TIMEOUT;
    };
    return adaptNode6(transportMessage);
  }

  /**
   * Uses pull mode to fetch one message from the 'source' topic and sends to
   * the target topic. If there is no new message coming, this method will wait
   * for the seconds set by the arg 'timeout' before it exits.
   * @param {string} sourceTopic Name of 'source' topic.
   * @param {number} timeout Idle time in seconds.
   * @param {string} targetTopic Name of target topic.
   * @return {!Promise<!TransportResult>} Result of this execution.
   * @private
   */
  async passOneMessage_(sourceTopic, timeout, targetTopic) {
    /**
     * Gets the message handler function for the pull subscription.
     * @param {function(*)} resolver Function to call when promise is fulfilled.
     * @return {function(!PubsubMessage):!Promise<!TransportResult>}
     */
    const getMessageHandler = (resolver) => {
      return async (message) => {
        const {id, length, attributes} = message;
        const messageTag = `[${id}]@[${sourceTopic}]`;  // For log.
        this.logger.debug(`Received ${messageTag} with data length: ${length}`);
        const taskId = attributes.taskId;
        const startSuccessfully = await this.tentaclesTask.start(taskId);
        if (startSuccessfully) {
          const messageId = await this.pubsub.publish(targetTopic,
              Buffer.from(message.data, 'base64').toString(), attributes);
          this.logger.debug(`Forward ${messageTag} as [${messageId}]@[${
              targetTopic}]`);
          message.ack();
          await this.tentaclesTask.updateTask(taskId,
              {apiMessageId: messageId});
          resolver(TransportResult.DONE);
        } else {
          this.logger.warn(`Wrong status for ${
              messageTag} (maybe duplicated). Task ID: [${taskId}].`);
          message.ack();
          resolver(TransportResult.DUPLICATED);
        }
      };
    };
    const subscription = await this.pubsub.getOrCreateSubscription(
        sourceTopic, `${sourceTopic}-holder`,
        {ackDeadlineSeconds: 300, flowControl: {maxMessages: 1}});
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
      const {attributes, data} = message;
      const records = Buffer.from(data, 'base64').toString();
      this.logger.debug(
          `Receive message[${messageId}] with ${records.length} bytes.`);
      const {api, config, dryRun, appended, taskId, topic} = attributes || {};
      /** @type {BatchResult} */ let result;
      let needContinue;
      try {
        const apiConfig = await this.apiConfig.getConfig(api, config);
        const apiHandler = this.options.getApiHandler(api);
        if (!apiHandler) throw new Error(`Unknown API: ${api}.`);
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
          result = /** @type {!BatchResult} */ {result: true}; // A dry-run task always succeeds.
          if (!getApiOnGcs().includes(attributes.api)) {
            result.numberOfLines = records.split('\n').length;
          }
        } else {
          result = await apiHandler(records, messageId, finalConfig);
        }
        //TODO(lushu) For previous API handler, will be removed after all updated.
        if (typeof result.result === 'undefined') {
          await this.tentaclesTask.finish(taskId, result);
        } else {
          const {numberOfLines = 0, failedLines, groupedFailed} = result;
          await this.tentaclesTask.updateTask(taskId, {
            numberOfLines,
            numberOfFailed: failedLines ? failedLines.length : 0,
          });
          if (groupedFailed) {
            const errorLogger = getLogger('TentaclesFailedRecord');
            Object.keys(groupedFailed).forEach((error) => {
              errorLogger.info(
                  JSON.stringify(
                      {taskId, error, records: groupedFailed[error]}));
            });
          }
          if (result.result) {
            await this.tentaclesTask.finish(taskId, result.result);
          } else {
            await this.tentaclesTask.logError(taskId, result.errors);
          }
        }
        needContinue = true;
      } catch (error) {
        this.logger.error(`Error in API[${api}], config[${config}]: `, error);
        await this.tentaclesTask.logError(taskId, error);
        needContinue = !error.message.startsWith('Unsupported API');
      }
      if (!topic) {
        this.logger.info('There is no topic. In local file upload mode.');
        return;
      }
      if (!needContinue) {
        this.logger.info(`Skip unsupported API ${api}.`);
      }
      try {
        return this.releaseLockAndNotify(topic, messageId, needContinue);
      } catch (error) {
        // Re-do this when unknown external exceptions happens.
        this.logger.error('Exception happened while try to release the lock: ',
            error);
        await wait(10000); // wait 10 sec
        this.logger.info('Wait 10 second and retry...');
        return this.releaseLockAndNotify(topic, messageId, needContinue);
      }
    }
    return adaptNode6(sendApiData);
  }

  /**
   * Releases the lock and sends notification message for next piece of data.
   * @param {string} topic The topic name as well as the lock name.
   * @param {string} messageId ID of current message.
   * @param {boolean} needContinue Whether should send notification message.
   * @return {!Promise<string|undefined>} ID of the 'nudge' message.
   */
  async releaseLockAndNotify(topic, messageId, needContinue) {
    await this.apiLock.unlock(topic);
    if (needContinue) {
      return this.nudge(`Triggered by message[${messageId}]`, {topic});
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
  attributes.gcs = getApiOnGcs().includes(attributes.api).toString();
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
 * @param {!DataSource|undefined=} datasource The underlying datasource type.
 *     Tentacles use the database to manage 'API Configuration', 'ApiLock',
 *     'TentaclesTask' and 'TentaclesFile'. If this is omitted, Tentacles can
 *     still work without following features:
 *     1. anti-duplicated Pub/Sub messages.
 *     2. speed control for multiple simultaneously incoming files of a same
 *     API.
 *     3. logs for files and tasks for reports or further analysis.
 * @param {!ApiConfigJson|undefined=} apiConfig Source of the ApiConfig.
 *     In case there is no available Firestore/Datastore, Tentacles can still
 *     work on a JSON file based API configuration.
 * @return {!Tentacles} The Tentacles instance.
 */
const getTentacles = (namespace, datasource = undefined, apiConfig) => {
  /** @type {TentaclesOptions} */
  const options = {
    namespace,
    apiConfig: /** @type {ApiConfig} */ getApiConfig(apiConfig || datasource,
        namespace),
    apiLock: /** @type {ApiLock} */ getApiLock(datasource, namespace),
    tentaclesFile: /** @type {TentaclesFile} */ getTentaclesFile(datasource,
        namespace),
    tentaclesTask: /** @type {TentaclesTask} */ getTentaclesTask(datasource,
        namespace),
    pubsub: new EnhancedPubSub(),
    getStorage: StorageFile.getInstance,
    validatedStorageTrigger,
    getApiHandler,
  };
  console.log(
      `Init Tentacles for namespace[${namespace}], Datasource[${datasource}]`);
  return new Tentacles(options);
};

/**
 * Probes the Google Cloud Project's Firestore mode (Native or Datastore), then
 * uses it to create an instance of Tentacles.
 * @return {!Promise<!Tentacles>}
 */
const guessTentacles = async (namespace = process.env['PROJECT_NAMESPACE']) => {
  if (!namespace) {
    console.warn(
        'Fail to find ENV variables PROJECT_NAMESPACE, will set as `tentacles`'
    );
    namespace = 'tentacles';
  }
  const isNative = await FirestoreAccessBase.isNativeMode();
  const dataSource = isNative ? DataSource.FIRESTORE : DataSource.DATASTORE;
  return getTentacles(namespace, dataSource);
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
