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
const AuthClient = require('./auth_client.js');
const {wait, getLogger} = require('../components/utils.js');

const ANALYTICS_API_SCOPES = ['https://www.googleapis.com/auth/analytics'];

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
    const authClient = new AuthClient(ANALYTICS_API_SCOPES);
    this.instance = authClient.getDefaultAuth().then((auth) => {
      return google.analytics({
        version: 'v3',
        auth: auth,
        maxContentLength: 1000 * 1024 * 1024,
      });
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
    return this.instance.then((ga) => {
      return ga.management.uploads.uploadData(uploadConfig)
          .then((response) => {
            this.logger.debug('Configuration: ', config);
            this.logger.debug('Upload Data: ', data);
            this.logger.debug('Response: ', response);
            const job = response.data;
            console.log(
                `Task [${batchId}] creates GA Data import job: ${job.id}`);
            return job;
          })
          .then((job) => {
            const jobConfig = Object.assign({uploadId: job.id}, config);
            return Promise
                .race([
                  this.checkJobStatus(ga, jobConfig),
                  wait(8 * 60 * 1000, job),  // wait up to 8 minutes here
                ])
                .then((job) => {
                  switch (job.status) {
                    case 'FAILED':
                      console.error('GA Data Import failed', job);
                      return false;
                    case 'COMPLETED':
                      console.log(`GA Data Import job[${job.id}] completed.`);
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
    });
  }

  /**
   * Checks the status of a Data Import job.
   *
   * @param {!google.analytics} ga Google Analytics client stub object.
   * @param {!Object<string, string>} jobConfig Google Analytics Data Import
   *     Job.
   * @return {!Promise<!Object<string,string>>} Updated data import Job status.
   */
  checkJobStatus(ga, jobConfig) {
    return ga.management.uploads.get(jobConfig).then((response) => {
      const job = response.data;
      if (job.status === 'PENDING') {
        this.logger.debug(`GA Data Import Job[${
            jobConfig.uploadId}] is not finished. Wait 10 sec...`);
        // GA Data Import is an asynchronous job. Waits some time (10 seconds)
        // here to get the updated status.
        return wait(10 * 1000).then(() => {
          return this.checkJobStatus(ga, jobConfig);
        });
      } else {
        return job;
      }
    });
  }

  /**
   * Lists all accounts.
   * @return {!Promise<!Array<string>>}
   */
  listAccounts() {
    return this.instance.then((ga) => {
      return ga.management.accounts.list().then((accounts) => {
        return accounts.data.items.map((account) => {
          return `Account id: ${account.name} [${account.id}]`;
        });
      });
    });
  }
}

exports.Analytics = Analytics;
exports.DataImportConfig = DataImportConfig;
