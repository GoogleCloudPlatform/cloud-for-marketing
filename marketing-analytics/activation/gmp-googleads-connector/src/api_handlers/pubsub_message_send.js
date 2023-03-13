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
 * @fileoverview Tentacles API handler for Pub/Sub message sending.
 */

'use strict';

const {
  pubsub: {EnhancedPubSub},
  utils: {
    apiSpeedControl,
    getProperValue,
    replaceParameters,
    getLogger,
    wait,
    BatchResult,
  },
} = require('@google-cloud/nodejs-common');

/**
 * One message per request.
 */
const RECORDS_PER_REQUEST = 1;
/**
 * Queries per second.
 */
const QUERIES_PER_SECOND = 1;
const NUMBER_OF_THREADS = 1;

/** Retry times if error happens in sending Pub/Sub messages. */
const RETRY_TIMES = 3;

/** API name in the incoming file name. */
exports.name = 'PB';

/** Data for this API will be transferred through GCS by default. */
exports.defaultOnGcs = false;

/**
 * Configuration for a Pub/Sub message sending integration.
 *
 * @typedef {{
 *   topic:string,
 *   message:string|object|undefined,
 *   attributes:{string,string}|undefined,
 *   recordsPerRequest:(number|undefined),
 *   qps:(number|undefined),
 *   numberOfThreads:(number|undefined),
 * }}
 */
let PubSubMessageConfig;

exports.PubSubMessageConfig = PubSubMessageConfig;

/**
 * Sends out the data as messages to Pub/Sub (PB).
 * This function exposes a EnhancedPubSub parameter for test.
 * @param {!EnhancedPubSub} pubsub Injected EnhancedPubSub instance.
 * @param {string} records Attributes of the messages. Expected JSON string in
 *     each line.
 * @param {string} messageId Pub/sub message ID for log.
 * @param {!PubSubMessageConfig} config
 * @return {!BatchResult}
 */
const sendDataInternal = async (pubsub, records, messageId, config) => {
  const logger = getLogger('API.PB');
  logger.debug(`Init Pub/Sub message sender with Debug Mode.`);
  const recordsPerRequest =
      getProperValue(config.recordsPerRequest, RECORDS_PER_REQUEST);
  const numberOfThreads =
      getProperValue(config.numberOfThreads, NUMBER_OF_THREADS, false);
  const qps = getProperValue(config.qps, QUERIES_PER_SECOND, false);
  const managedSend = apiSpeedControl(recordsPerRequest, numberOfThreads, qps);
  const topic = await pubsub.getOrCreateTopic(config.topic);
  /** This function send out one message with the given data. */
  const getSendSingleMessageFn = async (lines, batchId) => {
    if (lines.length !== 1) throw Error('Wrong number of Pub/Sub messages.');
    /** @const {!BatchResult} */ const batchResult = {
      numberOfLines: 1,
    };
    const args = JSON.parse(lines[0]);
    // JSON string values need to be escaped
    Object.keys(args).forEach((key) => {
      try {
        if (typeof args[key] === 'string') {
          args[key] = args[key].replace(/\n/g, '\\n').replace(/\"/g, '\\"');
        }
      } catch (error) {
        logger.error(key, typeof args[key]);
        logger.error(error);
      }
    })
    const originalMessage = typeof (config.message) === 'object'
        ? JSON.stringify(config.message) : config.message;
    const message = replaceParameters(originalMessage || '', args, true);
    const attributes = JSON.parse(
        replaceParameters(JSON.stringify(config.attributes || {}), args, true)
    );
    let retryTimes = 0;
    let errors = [];
    do {
      // Wait sometime (1s, 2s, 3s, ...) before each retry.
      if (retryTimes > 0) await wait(retryTimes * 1000);
      try {
        const messageId = await pubsub.publish(topic, message, attributes);
        logger.debug(`Send ${lines[0]} to ${config.topic} as ${messageId}.`);
        batchResult.result = true;
        return batchResult;
      } catch (error) {
        logger.error(`Pub/Sub message[${batchId}] failed: ${lines[0]}`, error);
        errors.push(error.message);
        retryTimes++;
      }
    } while (retryTimes <= RETRY_TIMES)
    batchResult.result = false;
    batchResult.errors = errors;
    batchResult.failedLines = lines;
    return batchResult;
  };
  return managedSend(getSendSingleMessageFn, records, messageId);
};

exports.sendDataInternal = sendDataInternal;

/**
 * Sends out the data as messages to Pub/Sub (PB).
 * @param {string} records Attributes of the messages. Expected JSON string in
 *     each line.
 * @param {string} messageId Pub/sub message ID for log.
 * @param {!PubSubMessageConfig} config
 * @return {!BatchResult}
 */
exports.sendData = (records, messageId, config) => {
  const pubsub = new EnhancedPubSub();
  return sendDataInternal(pubsub, records, messageId, config);
};
