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
 * @fileoverview Tentacles API handler for SFTP upload.
 */

'use strict';

const SftpClient = require('ssh2-sftp-client');
const path = require('path');
const {StorageUtils, utils} = require('nodejs-common');

/** Placeholder for a timestamp in the name of a uploaded file. */
const TIMESTAMP_PLACEHOLDER = 'TIMESTAMP';

/** API name in the incoming file name. */
exports.name = 'SFTP';

/** Data for this API will be transferred through GCS by default. */
exports.defaultOnGcs = true;

/**
 * A Sftp server connection information.
 * @typedef {{
 *   host:string,
 *   port:string,
 *   username:string,
 *   password:string,
 * }}
 */
let SftpServer;

/**
 * The 'fileName' is an optional setting for the name of a uploaded file.
 *
 * If the fileName has the placeholder 'TIMESTAMP_PLACEHOLDER', a
 * processing time timestamp will be generated and replace that with a string
 * like this: '2019-05-31T01.42.25.590Z'.
 *
 * If the fileName is not defined, the default value is:
 * For the file from Cloud Storage, source file's basename will be used by
 * removing some redundant characters/pieces, including:
 * 'API[','config[', 'GCS[', ']', ':', etc.
 * For the uploaded data from Pub/Sub messages directly, default value is
 * 'upload_by_tentacles_TIMESTAMP'. Please note: for Search Ads 360 business
 * data feed upload, the system will expect a reasonable file extension name,
 * e.g. 'csv', 'xlsx'.
 *
 * @typedef {{
 *   fileName:(string|undefined),
 *   sftp:!SftpServer,
 * }}
 */
let SftpConfig;

exports.SftpConfig = SftpConfig;
/**
 * Uploads a file to a SFTP server. One use case is to upload business data in
 * Search Ads 360.
 * @param {string} message Message data from Pubsub. It is the information of
 *     the file to be sent out, or a piece of data that need to be send out.
 * @param {string} messageId Pub/sub message ID as log tag.
 * @param {!SftpConfig} config
 * @return {!Promise<boolean>} Whether 'records' have been sent out without any
 *     errors.
 */
exports.sendData = (message, messageId, config) => {
  const logger = utils.getLogger('API.SFTP');
  logger.debug(`Init SFTP uploader with Debug Mode.`);
  let data;
  try {
    data = JSON.parse(message);
  } catch (error) {
    console.log(`This is not a JSON string, SFTP uploading file not on GCS.`);
    data = message;
  }
  let sftpClient = new SftpClient();
  let output;
  let targetFile;
  if (data.bucket) {
    const storageUtils = new StorageUtils(data.bucket, data.file);
    output = storageUtils.getFile().createReadStream();
    const defaultFileName = path.basename(data.file)
                                .replace(/API\[(\w*)]/i, '$1')
                                .replace(/config\[(\w*)]/i, '$1')
                                .replace(/GCS\[(\w*)]/i, '$1')
                                .replace(/:/g, '-');
    targetFile = (config.fileName) ? config.fileName : defaultFileName;
  } else {
    output = Buffer.from(data);
    targetFile = config.fileName || 'upload_by_tentacles_TIMESTAMP';
  }
  if (targetFile.indexOf(TIMESTAMP_PLACEHOLDER) > -1) {
    const timestamp = new Date().toISOString().replace(/:/g, '.');
    targetFile = targetFile.replace(TIMESTAMP_PLACEHOLDER, timestamp);
  }
  logger.debug(`Get message: ${message}`);
  logger.debug(`SFTP Configuration: `, config);
  logger.debug(`Output filename: ${targetFile}`);
  return sftpClient.connect(config.sftp)
      .then(() => {
        console.log(`Open SFTP server: ${config.sftp.host}`);
        return sftpClient.put(output, targetFile).then((result) => {
          console.log('SFTP operation: ', result);
          return true;
        });
      })
      .catch((err) => {
        console.error('catch error', err);
        return false;
      })
      .then((output) => {
        console.log(`SFTP upload [${messageId}]: ${output}`);
        return sftpClient.end().then(() => output);
      });
};
