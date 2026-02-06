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
 * to Cloud Storage. Besides these jobs, it also support Vertex AI batch
 * prediction job.
 */

'use strict';

const {
  cloudfunctions: {
    StorageEventData,
    PubsubMessage,
    EventContext,
    convertEnvPathToAbsolute,
  } } = require('@google-cloud/nodejs-common');
const {guessSentinel} = require('./src/sentinel.js');
const {
  uploadTaskConfig,
  checkGoogleAdsReports,
  startTaskFromLocal,
  startTaskThroughPubSub,
} = require('./src/sentinel_helper.js');

/**
 * Monitors events on Storage.
 * Triggered by the new coming files in the Storage Bucket.
 * @param {!StorageEventData} eventData An object representing the event data
 *   payload. Its format depends on the event type.
 * @param {!EventContext} context An object containing metadata about the event.
 * @return {!Promise<(!Array<string>|undefined)>} IDs of the 'start load task'
 *     messages.
 */
const monitorStorage = async (eventData, context) => {
  if (!process.env['SENTINEL_INBOUND']) {
    console.warn(
        'Fail to find ENV variables SENTINEL_INBOUND, will set as `inbound/`');
  }
  const monitorFolder = process.env['SENTINEL_INBOUND'] || 'inbound/';
  const sentinel = await guessSentinel();
  const monitorStorage = sentinel.getStorageMonitor(monitorFolder);
  return monitorStorage(eventData, context);
};

/**
 * Coordinates tasks based on the messages of a Pub/sub topic named 'monitor'.
 * @param {!PubsubMessage} eventData An object representing the event data
 *   payload. Its format depends on the event type.
 * @param {!EventContext} context An object containing metadata about the event.
 * @return {!Promise<(!Array<string>|number|undefined)>} The message Id array
 *     of the next tasks and an empty Array if there is no followed task.
 *     Returns taskLogId (number) when an error occurs.
 *     Returns undefined if there is no related task.
 */
const coordinateTask = async (eventData, context) => {
  /** Converts the key files value from relative paths to absolute ones. */
  convertEnvPathToAbsolute('OAUTH2_TOKEN_JSON', __dirname);
  convertEnvPathToAbsolute('API_SERVICE_ACCOUNT', __dirname);
  const sentinel = await guessSentinel();
  const coordinateTask = sentinel.getTaskCoordinator();
  return coordinateTask(eventData, context);
};

/**
 * A HTTP based Cloud Functions which returns Sentinel workflow information.
 * @param {Object} request Cloud Function request context.
 * @param {Object} response Cloud Function response context.
 */
const reportWorkflow = async (request, response) => {
  const sentinel = await guessSentinel();
  sentinel.workflowReporter(request, response);
}

module.exports = {
  monitorStorage,
  coordinateTask,
  reportWorkflow,
  uploadTaskConfig,
  checkGoogleAdsReports,
  startTaskFromLocal,
  startTaskThroughPubSub,
};
