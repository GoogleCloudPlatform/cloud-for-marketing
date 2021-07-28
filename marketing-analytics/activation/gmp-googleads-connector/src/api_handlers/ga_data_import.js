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
 * @fileoverview Tentacles API handler for Google Analytics Data Import
 * (Analytics API).
 */

'use strict';

const {File} = require('@google-cloud/storage');
const {
  api: {analytics: {Analytics, DataImportConfig, DataImportClearConfig,}},
  storage: {StorageFile},
} = require('@google-cloud/nodejs-common');

/** API name in the incoming file name. */
exports.name = 'GA';

/** Data for this API will be transferred through GCS by default. */
exports.defaultOnGcs = true;

/**
 * 'dataImportHeader' is the header of uploading CSV file. It is fixed in
 * Google Analytics when a Data Import item is set up and it must exist as the
 * first line in the file uploaded.
 * Sometimes, the data files do not have such a header line, e.g. BigQuery
 * cannot export data with colon in column names. In that case, an explicit
 * config item dataImportHeader may apply. The function 'sendData' here will
 * automatically attach this line at the beginning of data file before
 * uploading.
 * 'clearOption' is the configuration to manage how to clear previous uploaded
 * data files.
 *
 * @typedef {{
 *   dataImportHeader:(string|undefined),
 *   gaConfig:!DataImportConfig,
 *   clearOption:(!DataImportClearConfig|undefined),
 * }}
 */
let GoogleAnalyticsConfig;

exports.GoogleAnalyticsConfig = GoogleAnalyticsConfig;

/**
 * Gets Cloud Storage file ready for uploading.
 * In case of there is a header line need to be inserted the file, this helper
 * function will create a new file with the header line and other content from
 * the original file for uploading.
 * @param {string} bucket Cloud Storage Bucket name.
 * @param {string} fileName Cloud Storage file name.
 * @param {string=} dataImportHeader Header line needs to be inserted into the
 *     file.
 * @return {!Promise<!File>} Cloud Storage File to be uploaded.
 */
const prepareFile = async (bucket, fileName, dataImportHeader = undefined) => {
  const storageFile = new StorageFile(bucket, fileName);
  if (!dataImportHeader) {
    console.log(`No head line need to take care for: `, fileName);
    return storageFile.getFile();
  }
  console.log(`Appends ${dataImportHeader} to the head of: `, fileName);
  const newFileName = await storageFile.addHeader(dataImportHeader);
  return new StorageFile(bucket, newFileName).getFile();
};

/**
 * Sends the data or file from a Pub/sub message to Google Analytics Data
 * Import. It will delete previous uploaded data files if there is a setting
 * in the 'GoogleAnalyticsConfig'.
 * @see https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/uploads/
 *
 * @param {string} message Message data from Pubsub. It could be the
 *     information of the file to be sent out, or a piece of data that need to
 *     be send out.
 * @param {string} messageId Pub/sub message ID for log.
 * @param {!GoogleAnalyticsConfig} config
 * @return {!Promise<boolean>} Whether 'records' have been sent out without any
 *     errors.
 */
const sendData = (message, messageId, config) => {
  const analytics = new Analytics();
  return sendDataInternal(analytics, message, messageId, config);
}
exports.sendData = sendData;

/**
 * Internal sendData function for test.
 * @param {!Analytics} analytics Injected Analytics instance.
 * @param {string} message Message data from Pubsub. It could be the
 *     information of the file to be sent out, or a piece of data that need to
 *     be send out.
 * @param {string} messageId Pub/sub message ID for log.
 * @param {!GoogleAnalyticsConfig} config
 * @return {!Promise<boolean>} Whether 'records' have been sent out without any
 *     errors.
 */
const sendDataInternal = async (analytics, message, messageId, config) => {
  let uploadData = '';
  const {dataImportHeader} = config;
  try {
    const {bucket, file} = JSON.parse(message);
    if (bucket) {  // Data is a GCS file.
      const file = await prepareFile(bucket, file, dataImportHeader);
      uploadData = file.createReadStream();
    } else {
      console.error('Could find bucket in message', message);
      return false;
    }
  } catch (error) {
    console.log(`This is not a JSON string. GA Data Import's data not on GCS.`);
    if (dataImportHeader) {
      uploadData = dataImportHeader + '\n';
    }
    uploadData += message;
  }
  if (config.clearOption) {
    await analytics.deleteUploadedData(config.gaConfig, config.clearOption);
  }
  return analytics.uploadData(uploadData, config.gaConfig, messageId);
};

exports.sendDataInternal = sendDataInternal;
