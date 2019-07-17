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
 * @fileoverview Tentacles API handler for Google Analytics Measurement
 * Protocol.
 */

'use strict';

const {
  api: {MeasurementProtocol: {MeasurementProtocol}},
  utils: {apiSpeedControl, getProperValue},
} = require('nodejs-common');

/**
 * Hits per request. Measurement Protocol(MP) has a value as 20.
 * see
 * https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#batch-limitations
 */
const RECORDS_PER_REQUEST = 20;  // Maximum value defined by this API.
const QUERIES_PER_SECOND = 20;
const NUMBER_OF_THREADS = 10;

/** API name in the incoming file name. */
exports.name = 'MP';

/** Data for this API will be transferred through GCS by default. */
exports.defaultOnGcs = false;

/**
 * Measurement Protocol configuration.
 *
 * @typedef {{
 *   recordsPerRequest:(number|undefined),
 *   qps:(number|undefined),
 *   numberOfThreads:(number|undefined),
 *   mpConfig:!Object<string,string>,
 * }}
 */
let MeasurementProtocolConfig;

exports.MeasurementProtocolConfig = MeasurementProtocolConfig;

/**
 * Sends out the data to Google Analytics Measurement Protocol (MP).
 * MP pings are composed by 'string' parameters and values. Put those common
 * parameters in the 'mpConfig' object, e.g. Protocol Version, Tracking ID / Web
 * Property ID, etc, and put other customized parameters, e.g. Client ID or
 * Custom Dimensions into 'records'.
 * For reference, see:
 *     https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters
 * @param {string} records Data to send out to Google Analytics. Expected JSON
 *     string in each line.
 * @param {string} messageId Pub/sub message ID for log.
 * @param {!MeasurementProtocolConfig} config Configuration for Measurement
 *     Protocol.
 * @return {!Promise<boolean>} Whether 'records' have been sent out without any
 *     errors.
 */
exports.sendData = (records, messageId, config) => {
  const measurementProtocol = new MeasurementProtocol();
  const recordsPerRequest =
      getProperValue(config.recordsPerRequest, RECORDS_PER_REQUEST);
  const numberOfThreads =
      getProperValue(config.numberOfThreads, NUMBER_OF_THREADS, false);
  const qps = getProperValue(config.qps, QUERIES_PER_SECOND, false);
  return apiSpeedControl(recordsPerRequest, numberOfThreads, qps)(
      measurementProtocol.getSinglePingFn(config.mpConfig), records, messageId);
};
