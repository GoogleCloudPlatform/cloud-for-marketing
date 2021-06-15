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
 * @fileoverview Library export file of Sentinel. Cloud Task Coordinator
 * (code name 'Sentinel').
 * It is a Cloud Functions based solution to coordinate BigQuery related
 * jobs(tasks), e.g. loading file from Cloud Storage, querying or exporting data
 * to Cloud Storage. Besides these jobs, it also support AutoML Tables batch
 * prediction job.
 */

'use strict';

const {cloudfunctions: {convertEnvPathToAbsolute}} = require(
    '@google-cloud/nodejs-common');
const {guessSentinel} = require('./src/sentinel.js');
const {
  uploadTaskConfig,
  startTaskFromLocal,
  startTaskThroughPubSub,
} = require('./src/sentinel_helper.js');

/**
 * Monitors events on Storage.
 * Triggered by the new coming files in the Storage Bucket.
 * @param {...!Object} args The parameters from Cloud Functions. The parameters
 *     vary in different runtime, for more details, see the definition in
 *     'CloudFunctionsUtils'.
 * @return {!Promise<(!Array<string>|undefined)>} IDs of the 'start load task'
 *     messages.
 */
const monitorStorage = (...args) => {
  if (!process.env['SENTINEL_INBOUND']) {
    console.warn(
        'Fail to find ENV variables SENTINEL_INBOUND, will set as `inbound/`');
  }
  const monitorFolder = process.env['SENTINEL_INBOUND'] || 'inbound/';
  return guessSentinel().then((sentinel) => {
    const monitorStorage = sentinel.getStorageMonitor(monitorFolder);
    return monitorStorage(...args);
  });
};

/**
 * Coordinates tasks based on the messages of a Pub/sub topic named 'monitor'.
 * @param {...!Object} args The parameters from Cloud Functions. The parameters
 *     vary in different runtime, for more details, see the definition in
 *     'CloudFunctionsUtils'.
 * @return {!Promise<(!Array<string>|number|undefined)>} The message Id array
 *     of the next tasks and an empty Array if there is no followed task.
 *     Returns taskLogId (number) when an error occurs.
 *     Returns undefined if there is no related task.
 */
const coordinateTask = (...args) => {
  /** Converts the key files value from relative paths to absolute ones. */
  convertEnvPathToAbsolute('OAUTH2_TOKEN_JSON', __dirname);
  convertEnvPathToAbsolute('API_SERVICE_ACCOUNT', __dirname);
  return guessSentinel().then((sentinel) => {
    const coordinateTask = sentinel.getTaskCoordinator();
    return coordinateTask(...args);
  });
};

module.exports = {
  monitorStorage,
  coordinateTask,
  uploadTaskConfig,
  startTaskFromLocal,
  startTaskThroughPubSub,
};
