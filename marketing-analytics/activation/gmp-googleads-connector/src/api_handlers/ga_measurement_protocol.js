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
  api: { measurementprotocol: { MeasurementProtocol } },
  utils: { getProperValue, BatchResult },
} = require('@google-cloud/nodejs-common');
const { ApiHandler } = require('./api_handler.js');

/**
 * Hits per request. Measurement Protocol(MP) has a value as 20.
 * see
 * https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#batch-limitations
 */
const RECORDS_PER_REQUEST = 20;  // Maximum value defined by this API.
const QUERIES_PER_SECOND = 20;
const NUMBER_OF_THREADS = 10;

/**
 * Measurement Protocol configuration.
 *
 * @typedef {{
 *   recordsPerRequest:(number|undefined),
 *   qps:(number|undefined),
 *   numberOfThreads:(number|undefined),
 *   debug:(boolean|undefined)
 *   mpConfig:!Object<string,string>,
 * }}
 */
let MeasurementProtocolConfig;

/**
 * Measurement Protocol for Google Analytics.
 */
class GoogleAnalyticsMeasurementProtocol extends ApiHandler {

  /** @override */
  getSpeedOptions(config) {
    const recordsPerRequest =
      getProperValue(config.recordsPerRequest, RECORDS_PER_REQUEST);
    const numberOfThreads =
      getProperValue(config.numberOfThreads, NUMBER_OF_THREADS, false);
    const qps = getProperValue(config.qps, QUERIES_PER_SECOND, false);
    return { recordsPerRequest, numberOfThreads, qps };
  }

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
   * @return {!Promise<!BatchResult>}
   * @override
   */
  sendData(records, messageId, config) {
    const debug = this.getDebug(config.debug);
    const measurementProtocol = new MeasurementProtocol(debug);
    const managedSend = this.getManagedSendFn(config);
    const configedUpload = measurementProtocol.getSinglePingFn(config.mpConfig);
    return managedSend(configedUpload, records, messageId);
  }
}

/** API name in the incoming file name. */
GoogleAnalyticsMeasurementProtocol.code = 'MP';

module.exports = {
  MeasurementProtocolConfig,
  GoogleAnalyticsMeasurementProtocol,
};
