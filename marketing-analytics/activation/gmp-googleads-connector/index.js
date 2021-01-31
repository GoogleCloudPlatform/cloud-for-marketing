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
 * @fileoverview Library export file of Tentacles. GMP and Google Ads Connector
 * (code name 'Tentacles') is an out-of-box solution based on Google Cloud
 * Platform. It can send a massive amount data to GMP (e.g. Google Analytics,
 * Campaign Manager) or Google Ads in a automatic and reliable way.
 */

'use strict';

const {cloudfunctions: {convertEnvPathToAbsolute}} = require('nodejs-common');
const {TransportResult, guessTentacles} = require('./src/tentacles.js');

/** Exports functions in tentacles_helper.js which are used in installation. */
Object.assign(exports, require('./src/tentacles_helper.js'));

/** Exports Tentacles class/functions for extension. */
Object.assign(exports, require('./src/tentacles.js'));

/**
 * Loads data from the incoming Cloud Storage file and sends out as messages
 * to Pub/Sub.
 * @param {...!Object} arg The parameters from Cloud Functions. The parameters
 *     vary in different runtime, for more details, see the definition in
 *     'CloudFunctionsUtils'.
 * @return {!Promise<string>} Id of the message start Cloud Functions
 *     'transport'.
 */
exports.initiate = (...arg) => {
  if (!process.env['TENTACLES_OUTBOUND']) {
    console.warn(
        'Fail to find ENV variables TENTACLES_OUTBOUND, will set as `outbound/`');
  }
  const monitorFolder = process.env['TENTACLES_OUTBOUND'] || 'outbound/';
  const tentaclesRequest = guessTentacles();
  return tentaclesRequest.then((tentacles) => {
    const initiator = tentacles.getInitiator(monitorFolder);
    return initiator(...arg);
  });
};

/**
 * Triggered by a 'nudge' message, it pulls one message from the 'source
 * queue' topic and push the message to the 'sending-out' topic.
 * @param {...!Object} arg The parameters from Cloud Functions. The parameters
 *     vary in different runtime, for more details, see the definition in
 *     'CloudFunctionsUtils'.
 * @return {!Promise<!TransportResult>} Result of this transporting job.
 */
exports.transport = (...arg) => {
  const tentaclesRequest = guessTentacles();
  return tentaclesRequest.then((tentacles) => {
    const transporter = tentacles.getTransporter();
    return transporter(...arg);
  });
};

/**
 * Sends out the data that are pushed to this function as Pub/Sub events data.
 * @param {...!Object} arg The parameters from Cloud Functions. The parameters
 *     vary in different runtime, for more details, see the definition in
 *     'CloudFunctionsUtils'.
 * @return {!Promise<string|void>} Id of the 'nudge' message after sending the
 *     data. In the case there is no topic in the attributes of trigger message,
 * it returns void.
 */
exports.requestApi = (...arg) => {
  /** Converts the key files value from relative paths to absolute ones. */
  convertEnvPathToAbsolute('OAUTH2_TOKEN_JSON', __dirname);
  convertEnvPathToAbsolute('API_SERVICE_ACCOUNT', __dirname);
  const tentaclesRequest = guessTentacles();
  return tentaclesRequest.then((tentacles) => {
    const apiRequester = tentacles.getApiRequester();
    return apiRequester(...arg);
  });
};
