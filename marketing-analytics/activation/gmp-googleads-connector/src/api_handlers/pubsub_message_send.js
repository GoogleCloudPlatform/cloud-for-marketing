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
  utils: {apiSpeedControl, getProperValue, replaceParameters, wait},
} = require('nodejs-common');

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
 *   message:string|undefined,
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
 * @param {EnhancedPubSub} pubsub Injected EnhancedPubSub instance.
 * @param {string} records Attributes of the messages. Expected JSON string in
 *     each line.
 * @param {string} messageId Pub/sub message ID for log.
 * @param {!PubSubMessageConfig} config
 * @return {!Promise<boolean>} Whether 'records' have been sent out without any
 *     errors.
 */
const sendDataInternal = (pubsub, records, messageId, config) => {
  const recordsPerRequest =
      getProperValue(config.recordsPerRequest, RECORDS_PER_REQUEST);
  const numberOfThreads =
      getProperValue(config.numberOfThreads, NUMBER_OF_THREADS, false);
  const qps = getProperValue(config.qps, QUERIES_PER_SECOND, false);
  const managedSend = apiSpeedControl(recordsPerRequest, numberOfThreads, qps);
  /** This function send out one message with the given data. */
  const getSendSingleMessageFn = async (lines, batchId) => {
    if (lines.length !== 1) throw Error('Wrong number of Pub/Sub messages.');
    const args = JSON.parse(lines[0]);
    const message = replaceParameters(config.message || '', args, true);
    const attributes = JSON.parse(
        replaceParameters(JSON.stringify(config.attributes || {}), args, true));
    let retryTimes = 0;
    do {
      // Wait sometime (1s, 2s, 3s, ...) before each retry.
      if (retryTimes > 0) await wait(retryTimes * 1000);
      try {
        const messageId = await pubsub.publish(config.topic, message,
            attributes);
        console.log(`Send ${lines[0]} to ${config.topic} as ${messageId}.`);
        return true;
      } catch (error) {
        console.error(`Pub/Sub message[${batchId}] failed: ${lines[0]}`,
            error);
        retryTimes++;
      }
    } while (retryTimes <= RETRY_TIMES)
    return false;
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
 * @return {!Promise<boolean>} Whether 'records' have been sent out without any
 *     errors.
 */
exports.sendData = (records, messageId, config) => {
  const pubsub = new EnhancedPubSub();
  return sendDataInternal(pubsub, records, messageId, config);
};
