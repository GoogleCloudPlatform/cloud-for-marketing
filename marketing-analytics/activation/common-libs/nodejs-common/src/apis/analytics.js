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
 * @fileoverview Google Analytics Data Import based on Google API Client
 * Library.
 */

'use strict';

const stream = require('stream');
const {google} = require('googleapis');
const {Schema$Upload} = google.analytics;
const AuthClient = require('./auth_client.js');
const {wait, getLogger} = require('../components/utils.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/analytics',
]);
const API_VERSION = 'v3';

/**
 * Configuration for a Google Analytics(GA) Data Import item, should include the
 * GA account Id, web property Id and the Data Set Id.
 * @typedef {{
 *   accountId:string,
 *   webPropertyId:string,
 *   customDataSourceId:string,
 * }}
 */
let DataImportConfig;

/**
 * Google Analytics API v3 stub.
 */
class Analytics {
  constructor() {
    const authClient = new AuthClient(API_SCOPES);
    const auth = authClient.getDefaultAuth();
    /** @type {!google.analytics} */
    this.instance = google.analytics({
      version: API_VERSION,
      auth,
    });
    this.logger = getLogger('API.GA');
    this.logger.debug(`Init ${this.constructor.name} with Debug Mode.`);
  }

  /**
   * Uploads data for Analytics Custom Data Source. For more details, see
   * https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/uploads/uploadData
   * Note: there is no uploaded filename currently. (see
   * https://github.com/googleapis/google-api-dotnet-client/issues/1087)
   * @param {string|!stream.Readable} data A string or a stream to be uploaded.
   * @param {!DataImportConfig} config GA data import configuration.
   * @param {string=} batchId A tag for log.
   * @return {!Promise<boolean>} Promise returning whether data import
   *     succeeded.
   */
  uploadData(data, config, batchId = 'unnamed') {
    const uploadConfig = Object.assign(
        {
          media: {
            mimeType: 'application/octet-stream',
            body: data,
          }
        },
        config);
    return this.instance.management.uploads.uploadData(uploadConfig)
        .then((response) => {
          this.logger.debug('Configuration: ', config);
          this.logger.debug('Upload Data: ', data);
          this.logger.debug('Response: ', response);
          const job = /** @type {Schema$Upload} */ response.data;
          const jobId = job.id;
          console.log(`Task [${batchId}] creates GA Data import job: ${jobId}`);
          return job;
        })
        .then((job) => {
          const jobConfig = Object.assign(
              {uploadId: (/** @type {Schema$Upload} */job).id}, config);
          return Promise
              .race([
                this.checkJobStatus(jobConfig),
                wait(8 * 60 * 1000, job),  // wait up to 8 minutes here
              ])
              .then((job) => {
                switch ((/** @type {Schema$Upload} */ job).status) {
                  case 'FAILED':
                    console.error('GA Data Import failed', job);
                    return false;
                  case 'COMPLETED':
                    console.log(`GA Data Import job[${(
                        /** @type {Schema$Upload} */job).id}] completed.`);
                    this.logger.debug('Response: ', job);
                    return true;
                  case 'PENDING':
                    console.log('GA Data Import pending.', job);
                    console.log('Still will return true here.');
                    return true;
                  default:
                    console.error('Unknown results of GA Data Import: ', job);
                    return false;
                }
              });
        });
  }

  /**
   * Checks the status of a Data Import job.
   *
   * @param {!Object<string, string>} jobConfig Google Analytics Data Import
   *     Job.
   * @return {!Promise<!Schema$Upload>} Updated data import Job status.
   */
  checkJobStatus(jobConfig) {
    return this.instance.management.uploads.get(jobConfig)
        .then(({data: job}) => {
          if (job.status !== 'PENDING') return job;
          this.logger.debug(`GA Data Import Job[${
              jobConfig.uploadId}] is not finished. Wait 10 sec...`);
          // GA Data Import is an asynchronous job. Waits some time (10 seconds)
          // here to get the updated status.
          return wait(10 * 1000).then(() => this.checkJobStatus(jobConfig));
        });
  }

  /**
   * Lists all accounts.
   * @return {!Promise<!Array<string>>}
   */
  listAccounts() {
    return this.instance.management.accounts.list().then(
        (response) => response.data.items.map(
            (account) => `Account id: ${account.name}[${account.id}]`
        ));
  }
}

module.exports = {
  Analytics,
  DataImportConfig,
  API_VERSION,
  API_SCOPES,
};
