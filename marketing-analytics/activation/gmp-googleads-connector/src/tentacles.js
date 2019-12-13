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
  FirestoreAccessBase: {DataSource},
  PubSubUtils: {
    publish,
    getOrCreateSubscription,
  },
  StorageUtils,
  CloudFunctionsUtils: {
    ValidatedStorageFile,
    PubsubMessage,
    CloudFunction,
    adaptNode6,
    validatedStorageTrigger,
  },
  utils: {
    getProperValue,
    wait,
    getLogger,
  },
} = require('nodejs-common');

const {getApiHandler, getApiOnGcs} = require('./api_handlers/index.js');
const {getApiConfig, ApiConfigHost, ApiConfigJson} = require('./api_config.js');
const {getApiLock, ApiLockBase} = require('./api_lock.js');
const {getTentaclesFile, TentaclesFileBase} = require('./tentacles_file.js');
const {getTentaclesTask, TentaclesTaskBase, TentaclesTaskEntity} =
    require('./tentacles_task.js');

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
exports.MESSAGE_MAXIMUM_SIZE = MESSAGE_MAXIMUM_SIZE;

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

exports.TransportResult = TransportResult;

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
   *
   * @param {string} topicPrefix Prefix of the topics that this instance will
   *     use.
   * @param {!DataSource|undefined=} datasource The underlying datasource type.
   *     Tentacles use the database to manage 'API Configuration', 'ApiLock',
   *     'TentaclesTask' and 'TentaclesFile'. If this is omitted, Tentacles can
   *     still work without following features:
   *     1. anti-duplicated Pub/Sub messages.
   *     2. speed control for multiple simultaneously incoming files of a same
   *     API.
   *     3. logs for files and tasks for reports or further analysis.
   * @param {!ApiConfigJson|undefined=} apiConfig Source of the ApiConfigHost.
   *     In case there is no available Firestore/Datastore, Tentacles can still
   *     work on a JSON file based API configuration.
   */
  constructor(topicPrefix, datasource = undefined, apiConfig = undefined) {
    /** @const {string} */ this.topicPrefix = topicPrefix;
    /** @const {!ApiConfigHost} */
    this.apiConfigHost = getApiConfig(apiConfig ? apiConfig : datasource);
    /** @const {!TentaclesFileBase} */
    this.tentaclesFile = getTentaclesFile(datasource);
    /** @const {!TentaclesTaskBase} */
    this.tentaclesTask = getTentaclesTask(datasource);
    /** @const {!ApiLockBase} */
    this.apiLock = getApiLock(datasource);
    /** @const {!Logger} */
    this.logger = getLogger('T.MAIN');
    console.log(`Init Tentacles for Topic[${topicPrefix}], Api Config[${
        this.apiConfigHost.constructor.name}] Lock[${
        this.apiLock.constructor.name}], File&Task[${datasource}]`);
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
    const loadFileAndNudge = (file) => {
      return this.loadGcsToPs(file)
          .then((topicName) => {
            const fullFilePath = `gs//:${file.bucket}/${file.name}`;
            if (topicName) {
              return this.nudge(
                  `After publish of ${fullFilePath}`, {topic: topicName});
            }
            console.warn(`Something wrong with the file ${fullFilePath}.`);
          })
          .catch(console.error);
    };
    return validatedStorageTrigger(loadFileAndNudge, outbound);
  }

  /**
   * Loads the GCS file and sent to Pub/Sub as messages.
   * Will split the messages based on the size.
   * @param {!ValidatedStorageFile} file Validated Storage file information from
   *     the function 'validatedStorageTrigger'.
   * @return {!Promise<string|undefined>} The name of the topic that receives
   *     the data.
   */
  loadGcsToPs(file) {
    /** @const {!Object<string,string>} Attributes in the Pub/Sub messages. */
    const attributes = getAttributes(file.name);
    attributes.topic = getTopicNameByApi(this.topicPrefix, attributes.api);
    this.logger.debug(`Attributes from [${file.name}]: `, attributes);
    return this.tentaclesFile.save(file).then((fileId) => {
      this.logger.debug(`Incoming file is logged as [${fileId}].`);
      return this.apiConfigHost.getConfig(attributes.api, attributes.config)
          .then((apiConfig) => {
            if (!getApiHandler(attributes.api)) {
              throw new Error(`Unknown API: ${attributes.api}.`);
            }
            if (!apiConfig) {
              throw new Error(`API[${attributes.api}] has unknown config: ${
                  attributes.config}.`);
            }
            if (file.size === 0) {
              console.warn(`Empty file: ${file.name}.`);
            }
            const taskBaseInfo = Object.assign({fileId: fileId}, attributes);
            if (attributes.gcs === 'true') {
              return this.sendFileInfoToMessage_(file, taskBaseInfo);
            } else {
              return this.sendDataToMessage_(file, taskBaseInfo);
            }
          })
          .then((sendOutAllData) => {
            if (sendOutAllData) return attributes.topic;
            throw new Error(`Errors in send out data.`);
          })
          .catch((error) => {
            console.error(`Error in ${file.name}: `, error);
            return this.tentaclesFile.saveError(fileId, error.message)
                .then(() => {
                  throw new Error(`File ${fileId} failed: ${error.message}`);
                });
          });
    });
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
  saveTaskAndSendData_(taskEntity, data) {
    return this.tentaclesTask.create(taskEntity).then((taskId) => {
      const messageAttributes = Object.assign({taskId: taskId}, taskEntity);
      return publish(taskEntity.topic, data, messageAttributes)
          .then((messageId) => {
            return this.tentaclesTask.update(
                taskId, {dataMessageId: messageId});
          });
    });
  };

  /**
   * Splits the ingested file into messages and sends to Pub/Sub.
   * @param {!ValidatedStorageFile} file Information of the ingested file.
   * @param {!TentaclesTaskEntity} taskBaseInfo Task information.
   * @return {!Promise<boolean>} Whether all messages sent out successfully.
   * @private
   */
  sendDataToMessage_(file, taskBaseInfo) {
    const storageUtils = new StorageUtils(file.bucket, file.name);
    const messageMaxSize = 1000 * 1000 *
        getProperValue(parseFloat(taskBaseInfo.size), MESSAGE_MAXIMUM_SIZE);
    this.logger.debug(`Split data size: ${messageMaxSize}`);
    return storageUtils.getFileSize()
        .then((fileSize) => {
          return storageUtils.getSplitRanges(fileSize, messageMaxSize);
        })
        .then((splitRanges) => {
          let promise = Promise.resolve(true);
          splitRanges.forEach(([start, end], index) => {
            promise = promise.then((latestResult) => {
              return storageUtils.loadContent(start, end).then((data) => {
                const taskEntity = Object.assign(
                    {start: start.toString(), end: end.toString()},
                    taskBaseInfo);
                this.logger.debug(`[${index}] Send ${
                    data.length} bytes to Topic[${taskEntity.topic}].`);
                return this.saveTaskAndSendData_(taskEntity, data)
                    .then((currentResult) => currentResult && latestResult);
              });
            });
          });
          return Promise.resolve(promise);
        });
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
  sendFileInfoToMessage_(file, taskBaseInfo) {
    const storageUtils = new StorageUtils(file.bucket, file.name);
    const gcsSplitSize =
        getProperValue(
            parseFloat(taskBaseInfo.size), STORAGE_FILE_MAXIMUM_SIZE, false) *
        1000 * 1000;
    return storageUtils.split(gcsSplitSize).then((slicedFiles) => {
      let promise = Promise.resolve(true);
      slicedFiles.forEach((slicedFile, index) => {
        const data = JSON.stringify({file: slicedFile, bucket: file.bucket});
        const taskEntity =
            Object.assign({slicedFile: slicedFile}, taskBaseInfo);
        this.logger.debug(
            `[${index}] Send ${data} to Topic[${taskEntity.topic}].`);
        promise = promise.then((latestResult) => {
          return this.saveTaskAndSendData_(taskEntity, data)
              .then((currentResult) => currentResult && latestResult);
        });
      });
      return Promise.resolve(promise);
    });
  }

  /**
   * Gets the Cloud Functions 'transporter' which is triggered by a 'nudge'
   * message. The Cloud Functions will take one message from the 'source queue'
   * topic and push the message to the 'sending-out' topic.
   * @param {number=} timeout Idle time (seconds) for this function to wait for
   *     a new message from the 'source queue'. The default value is 60.
   * @param {string=} targetTopic The name of topic that this function will
   *     push to. The default value is 'topicPrefix' followed by '-push'.
   * @return {!CloudFunction} The Cloud Functions 'transporter'.
   */
  getTransporter(timeout = 60, targetTopic = `${this.topicPrefix}-push`) {
    /** @type {!CloudFunctionNode8} */
    const transportMessage = (message, context) => {
      const attributes = message.attributes || {};
      const messageId = context.eventId;
      if (!attributes.topic) {
        console.warn(`There is no source topic: ${messageId}`);
        return Promise.resolve(TransportResult.NO_SOURCE_TOPIC);
      }
      const sourceTopic = attributes.topic;
      return this.apiLock.getLock(sourceTopic).then((getLocked) => {
        if (!getLocked) {
          console.warn(`There are running tasks for ${sourceTopic}. QUIT.`);
          return TransportResult.NO_LOCK;
        }
        const data = Buffer.from(message.data, 'base64').toString();
        this.logger.debug(`Get nudge message[${messageId}]: ${
            data}. Will transport for [${sourceTopic}]`);
        return this.passOneMessage_(sourceTopic, timeout, targetTopic)
            .then((result) => {
              this.logger.debug(
                  `Nudge message[${messageId}] transport results: ${result}`);
              if (result === TransportResult.DONE) return result;
              return this.apiLock.unlock(sourceTopic).then(() => {
                if (result === TransportResult.DUPLICATED) {
                  return this
                      .nudge(
                          `Got a duplicated message[${messageId}], ahead next.`,
                          attributes)
                      .then(() => TransportResult.DUPLICATED);
                }
                console.log(`There is no new message in ${sourceTopic}.`);
                return TransportResult.TIMEOUT;
              });
            });
      });
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
  passOneMessage_(sourceTopic, timeout, targetTopic) {
    /**
     * Gets the message handler function for the pull subscription.
     * @param {function(*)} resolver Function to call when promise is fulfilled.
     * @return {function(!PubsubMessage):!Promise<!TransportResult>}
     */
    const getMessageHandler = (resolver) => {
      return (message) => {
        const messageTag = `[${message.id}]@[${sourceTopic}]`;  // For log.
        this.logger.debug(
            `Received ${messageTag} with data length: ${message.length}`);
        const taskId = message.attributes.taskId;
        this.tentaclesTask.start(taskId).then((startSuccessfully) => {
          if (startSuccessfully) {
            publish(
                targetTopic, Buffer.from(message.data, 'base64').toString(),
                message.attributes)
                .then((messageId) => {
                  console.log(`Forward ${messageTag} as [${messageId}]@[${
                      targetTopic}]`);
                  message.ack();
                  return this.tentaclesTask
                      .update(taskId, {apiMessageId: messageId})
                      .then(() => {
                        resolver(TransportResult.DONE);
                      });
                });
          } else {
            console.warn(`Wrong status for ${
                messageTag} (maybe duplicated). Task ID: [${taskId}].`);
            message.ack();
            resolver(TransportResult.DUPLICATED);
          }
        });
      };
    };

    return getOrCreateSubscription(
        sourceTopic, `${sourceTopic}-holder`,
        {ackDeadlineSeconds: 300, flowControl: {maxMessages: 1}})
        .then((subscription) => {
          this.logger.debug(`Get subscription ${subscription.name}.`);
          const subscriber = new Promise((resolver) => {
            this.logger.debug(
                `Add messageHandler to Subscription:`, subscription);
            subscription.once(`message`, getMessageHandler(resolver));
          });
          return Promise
              .race([
                subscriber,
                wait(timeout * 1000, TransportResult.TIMEOUT),
              ])
              .then((result) => {
                this.logger.debug(`Remove messageHandler after ${result}.`);
                subscription.removeAllListeners('message');
                return result;
              });
        });
  }

  /**
   * Gets the Cloud Functions 'apiRequester' which sends out the data that are
   * pushed to this function as Pub/Sub events data.
   * @return {!CloudFunction} The Cloud Functions 'apiRequester'.
   */
  getApiRequester() {
    /** @type {!CloudFunctionNode8} */
    const sendApiData = (message, context) => {
      const messageId = context.eventId;
      const records = Buffer.from(message.data, 'base64').toString();
      console.log(
          `Receive message[${messageId}] with ${records.length} bytes.`);
      const attributes = message.attributes || {};
      return this.apiConfigHost.getConfig(attributes.api, attributes.config)
          .then((apiConfig) => {
            const apiHandler = getApiHandler(attributes.api);
            if (!apiHandler) {
              throw new Error(`Unknown API: ${attributes.api}.`);
            }
            if (!apiConfig) {
              throw new Error(`API[${attributes.api}] has unknown config: ${
                  attributes.config}.`);
            }
            if (attributes.dryRun === 'true') {
              console.log(
                  `[DryRun] API[${attributes.api}] and config[${
                      attributes.config}]: `,
                  apiConfig);
              return Promise.resolve(true);
            } else {
              return apiHandler(records, messageId, apiConfig);
            }
          })
          .then((succeeded) => {
            return this.tentaclesTask.finish(attributes.taskId, succeeded);
          })
          .catch((error) => {
            console.error(
                `Error in API[${attributes.api}], config[${
                    attributes.config}]: `,
                error);
            return this.tentaclesTask.logError(attributes.taskId, error);
          })
          .then(() => {
            if (attributes.topic) {
              return this.releaseLockAndNotify(attributes.topic,
                  messageId).catch((error) => {
                // Re-do this when unknown external exceptions happens.
                console.error('External exception happened: ', error);
                return wait(10000).then(() => {
                  console.log('Wait 10 second and retry...');
                  return this.releaseLockAndNotify(attributes.topic, messageId);
                });
              });
            }
            console.log(`There is no topic. In local file upload mode.`);
            return Promise.resolve();
          });
    };
    return adaptNode6(sendApiData);
  }

  /**
   * Releases the lock and sends notification message for next piece of data.
   * @param {string} topic The topic name as well as the lock name.
   * @param {string} messageId ID of current message.
   * @return {!Promise<string>} ID of the 'nudge' message.
   */
  releaseLockAndNotify(topic, messageId) {
    return this.apiLock.unlock(topic).then(() => {
      return this.nudge(
          `Triggered by message[${messageId}]`,
          {topic: topic});
    });
  }

  /**
   * Sends a 'nudge' message to the Topic which will trigger the Cloud Functions
   * 'transporter'.
   * @param {string} message The message string.
   * @param {{topic:(string|undefined)}=} attributes Message attributes.
   * @param {string=} topicName The name of the topic that this message will be
   *     sent to. The default value is `${this.topicPrefix}-trigger`
   * @return {!Promise<string>} ID of the 'nudge' message.
   */
  nudge(message, attributes = {}, topicName = `${this.topicPrefix}-trigger`) {
    return publish(topicName, message, attributes);
  }
}

exports.Tentacles = Tentacles;

/**
 * Returns the topic name for the data of a given API name.
 * @param {string} topicPrefix Prefix of Pub/Sub topic name.
 * @param {string} apiName API name.
 * @return {string} The topic name for the data of a given API name.
 */
const getTopicNameByApi = (topicPrefix, apiName) => {
  return `${topicPrefix}-${apiName}`;
};

exports.getTopicNameByApi = getTopicNameByApi;

/**
 * Pub/Sub message attributes from a given file name.
 * @typedef {{
 *   api:string,
 *   config:string,
 *   gcs:(string|undefined),
 *   size:(string|undefined),
 *   dryRun:string,
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

  const api = /API\[([\w-]*)]/i.exec(fileName);
  if (api) attributes.api = api[1];

  const config = /config\[([\w-]*)]/i.exec(fileName);
  if (config) attributes.config = config[1];

  const size = /size\[(\d*)(MB?)?]/i.exec(fileName);
  if (size) attributes.size = size[1].toString();

  attributes.dryRun = /dryrun/i.test(fileName).toString();
  attributes.gcs = getApiOnGcs().includes(attributes.api).toString();
  return attributes;
};

exports.getAttributes = getAttributes;
