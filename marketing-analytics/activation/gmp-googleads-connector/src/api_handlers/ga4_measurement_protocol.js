// Copyright 2021 Google Inc.
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
 * Protocol GA4.
 */

'use strict';

const {
  api: {
    measurementprotocolga4: {
      MeasurementProtocolGA4,
      MeasurementProtocolGA4Config,
    }
  },
  utils: {apiSpeedControl, getProperValue},
} = require('@google-cloud/nodejs-common');

/**
 * Hits per request. Measurement Protocol(GA4) has a value as 1.
 * see
 * https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#batch-limitations
 */
const RECORDS_PER_REQUEST = 1;  // Maximum value defined by this API.
const QUERIES_PER_SECOND = 20;
const NUMBER_OF_THREADS = 20;

/** API name in the incoming file name. */
exports.name = 'MP_GA4';

/** Data for this API will be transferred through GCS by default. */
exports.defaultOnGcs = false;

/**
 * Measurement Protocol configuration.
 *
 * @typedef {{
 *   qps:(number|undefined),
 *   numberOfThreads:(number|undefined),
 *   mpGa4Config:!MeasurementProtocolGA4Config,
 * }}
 */
let MpGa4IntegrationConfig;

exports.MpGa4IntegrationConfig = MpGa4IntegrationConfig;

/**
 * Sends out the data to Measurement Protocol (GA4).
 * MP pings are posted as strings of JSON objects. To set up an integration:
 * 1. Put those common parameters in the 'mpGa4Config' object, e.g. App Id,
 * events definition, etc.
 * 2. Put other customized parameters, e.g. App install ID or timestamp into
 * 'records'.
 * For reference, see:
 *     https://developers.google.com/analytics/devguides/collection/protocol/ga4/reference#payload_post_body
 * @param {string} records Data to send out to Google Analytics. Expected JSON
 *     string in each line.
 * @param {string} messageId Pub/sub message ID for log.
 * @param {!MpGa4IntegrationConfig} config Configuration for Measurement
 *     Protocol.
 * @return {!Promise<boolean>} Whether 'records' have been sent out without any
 *     errors.
 */
const sendData = (records, messageId, config) => {
  const mpGa4 = new MeasurementProtocolGA4();
  return sendDataInternal(mpGa4, records, messageId, config);
};

exports.sendData = sendData;

/**
 * Sends out the data to Measurement Protocol (GA4). This function exposes a
 * MeasurementProtocolGA4 parameter for test.
 * @param {!MeasurementProtocolGA4} mpGa4 Injected MeasurementProtocolGA4
 *     instance.
 * @param {string} records Data to send out to Google Analytics. Expected JSON
 *     string in each line.
 * @param {string} messageId Pub/sub message ID for log.
 * @param {!MpGa4IntegrationConfig} config Configuration for Measurement
 *     Protocol.
 * @return {!Promise<boolean>} Whether 'records' have been sent out without any
 *     errors.
 */
const sendDataInternal = (mpGa4, records, messageId, config) => {
  const recordsPerRequest = RECORDS_PER_REQUEST;
  const numberOfThreads =
      getProperValue(config.numberOfThreads, NUMBER_OF_THREADS, false);
  const qps = getProperValue(config.qps, QUERIES_PER_SECOND, false);
  const managedSend = apiSpeedControl(recordsPerRequest, numberOfThreads, qps);
  const configedUpload = mpGa4.getSinglePingFn(config.mpGa4Config);
  return managedSend(configedUpload, records, messageId);
};

exports.sendDataInternal = sendDataInternal;
