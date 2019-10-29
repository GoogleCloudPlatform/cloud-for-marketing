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
 * @fileoverview Sentinel library export file.
 */

'use strict';

const {
  utils: {checkPermissions},
  FirestoreAccessBase: {DataSource, isNativeMode},
} = require('nodejs-common');
const {Sentinel} = require('./src/sentinel.js');

/** Exports functions in sentinel_helper.js which are used in installation. */
Object.assign(exports, require('./src/sentinel_helper.js'));

/** Exports Sentinel class/functions for extension. */
Object.assign(exports, {Sentinel: Sentinel});

exports.checkPermissions = checkPermissions;
/**
 * Offers a Sentinel instance for this installation. Ideally, it can figure
 * out the Firestore/Datastore at backend.
 * If want to use JSON file to offer Module configurations:
 * const datasource = require('./config_module.json');
 * @return {!Promise<!Sentinel>}
 */
const getSentinel = () => {
  if (!process.env['SENTINEL_TOPIC_PREFIX']) {
    return Promise.reject('Fail to find ENV variables SENTINEL_TOPIC_PREFIX');
  }
  return isNativeMode().then((nativeMode) => {
    return (nativeMode) ? DataSource.FIRESTORE : DataSource.DATASTORE;
  }).then((datasource) => {
    process.env['FIRESTORE_TYPE'] = datasource;
    return new Sentinel(process.env['SENTINEL_TOPIC_PREFIX']);
  });
};

/**
 * Monitors events on Storage.
 * Triggered by the new coming files in the Storage Bucket.
 * @param {...!Object} arg The parameters from Cloud Functions. The parameters
 *     vary in different runtime, for more details, see the definition in
 *     'CloudFunctionsUtils'.
 * @return {!Promise<string>} Id of task log.
 */
exports.monitorStorage = (...arg) => {
  if (!process.env['SENTINEL_INBOUND']) {
    console.warn(
        'Fail to find ENV variables SENTINEL_INBOUND, will set as `inbound/`');
  }
  const monitorFolder = process.env['SENTINEL_INBOUND'] || 'inbound/';
  const sentinelRequest = getSentinel();
  return sentinelRequest.then((sentinel) => {
    const monitorStorage = sentinel.getStorageMonitor(monitorFolder);
    return monitorStorage(...arg);
  });
};

/**
 * Monitors events on BigQuery.
 * Triggered by the log export events through Pub/Sub.
 * @param {...!Object} arg The parameters from Cloud Functions. The parameters
 *     vary in different runtime, for more details, see the definition in
 *     'CloudFunctionsUtils'.
 * @return {!Promise<string>} Id of task log.
 */
exports.monitorBigQuery = (...arg) => {
  const sentinelRequest = getSentinel();
  return sentinelRequest.then((sentinel) => {
    const monitorBigQuery = sentinel.getBigQueryMonitor();
    return monitorBigQuery(...arg);
  });
};

/**
 * Starts a task based on the message information. Expects: `taskId` in
 * attributes and parameters JSON string in message data.
 * @param {...!Object} arg The parameters from Cloud Functions. The parameters
 *     vary in different runtime, for more details, see the definition in
 *     'CloudFunctionsUtils'.
 * @return {!Promise<string>} Id of task log.
 */
exports.startTask = (...arg) => {
  const sentinelRequest = getSentinel();
  return sentinelRequest.then((sentinel) => {
    const startTask = sentinel.getTaskRunner();
    return startTask(...arg);
  });
};
