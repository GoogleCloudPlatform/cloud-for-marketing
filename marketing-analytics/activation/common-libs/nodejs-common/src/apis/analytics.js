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
const {wait, getLogger, BatchResult} = require('../components/utils.js');

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
 * The configuration to delete uploaded data files.
 *   'max' stands for how many data files will be kept;
 *   'ttl' stands for how many days an uploaded file could be kept.
 * @typedef {{
 *   max:number|undefined,
 *   ttl:number|undefined,
 * }}
 */
let DataImportClearConfig;

/**
 * Google Analytics API v3 stub.
 */
class Analytics {
  /**
   * @constructor
   * @param {!Object<string,string>=} env The environment object to hold env
   *     variables.
   */
  constructor(env = process.env) {
    this.authClient = new AuthClient(API_SCOPES, env);
    this.logger = getLogger('API.GA');
  }

  /**
   * Prepares the Google Analytics instance.
   * @return {!google.analytics}
   * @private
   */
  async getApiClient_() {
    if (this.analytics) return this.analytics;
    await this.authClient.prepareCredentials();
    this.logger.debug(`Initialized ${this.constructor.name} instance.`);
    this.analytics = google.analytics({
      version: API_VERSION,
      auth: this.authClient.getDefaultAuth(),
    });
    return this.analytics;
  }

  /**
   * Uploads data for Analytics Custom Data Source. For more details, see
   * https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/uploads/uploadData
   * Note: there is no uploaded filename currently. (see
   * https://github.com/googleapis/google-api-dotnet-client/issues/1087)
   * @param {string|!stream.Readable} data A string or a stream to be uploaded.
   * @param {!DataImportConfig} config GA data import configuration.
   * @param {string=} batchId A tag for log.
   * @return {!BatchResult}
   */
  async uploadData(data, config, batchId = 'unnamed') {
    const uploadConfig = Object.assign(
        {
          media: {
            mimeType: 'application/octet-stream',
            body: data,
          }
        },
        config);

    const analytics = await this.getApiClient_();
    const response = await analytics.management.uploads.uploadData(
        uploadConfig);
    this.logger.debug('Configuration: ', config);
    this.logger.debug('Upload Data: ', data);
    this.logger.debug('Response: ', response);
    const job = /** @type {Schema$Upload} */ response.data;
    const uploadId = (/** @type {Schema$Upload} */job).id;
    this.logger.info(
        `Task [${batchId}] creates GA Data import job: ${uploadId}`);
    const jobConfig = Object.assign({uploadId}, config);
    const result = await Promise.race([
      this.checkJobStatus(jobConfig),
      wait(8 * 60 * 1000, job),  // wait up to 8 minutes here
    ]);
    /** @type {BatchResult} */ const batchResult = {};
    switch ((/** @type {Schema$Upload} */ result).status) {
      case 'FAILED':
        this.logger.error('GA Data Import failed', result);
        batchResult.result = false;
        batchResult.errors = result.errors
            || [`Unknown reason. ID: ${uploadId}`];
        break;
      case 'COMPLETED':
        this.logger.info(`GA Data Import job[${uploadId}] completed.`);
        this.logger.debug('Response: ', result);
        batchResult.result = true;
        break;
      case 'PENDING':
        this.logger.info('GA Data Import pending.', result);
        this.logger.info('Still will return true here.');
        batchResult.result = true;
        break;
      default:
        this.logger.error('Unknown results of GA Data Import: ', result);
        batchResult.result = false;
        batchResult.errors = [`Unknown status. ID: ${uploadId}`];
    }
    return batchResult;
  }

  /**
   * Checks the status of a Data Import job.
   *
   * @param {!Object<string, string>} jobConfig Google Analytics Data Import
   *     Job.
   * @return {!Promise<!Schema$Upload>} Updated data import Job status.
   */
  async checkJobStatus(jobConfig) {
    const analytics = await this.getApiClient_();
    const { data: job } = await analytics.management.uploads.get(jobConfig);
    if (job.status !== 'PENDING') return job;
    this.logger.debug(
        `GA Data Import Job[${jobConfig.uploadId}] is not finished.`);
    this.logger.debug('  Wait 10 sec...');
    // GA Data Import is an asynchronous job. Waits some time (10 seconds)
    // here to get the updated status.
    await wait(10 * 1000);
    return this.checkJobStatus(jobConfig);

  }

  /**
   * Lists all accounts.
   * @return {!Promise<!Array<string>>}
   */
  async listAccounts() {
    const analytics = await this.getApiClient_();
    const response = await analytics.management.accounts.list();
    return response.data.items.map(
        (account) => `Account id: ${account.name}[${account.id}]`
    );
  }

  /**
   * Lists all uploads.
   * @param {!DataImportConfig} config GA data import configuration.
   * @return {!Promise<!Array<Object>>}
   */
  async listUploads(config) {
    const analytics = await this.getApiClient_();
    const response = await analytics.management.uploads.list(config);
    return response.data.items;
  }

  /**
   * Delete the uploaded data files based on the options.
   * @param {!DataImportConfig} config GA data import configuration.
   * @param {!DataImportClearConfig} options
   * @return {Promise<void>}
   */
  async deleteUploadedData(config, options) {
    const toDelete = await this.getUploadsToDelete_(config, options);
    if (toDelete.length === 0) {
      this.logger.debug('There are no uploads needs to be deleted.');
      return;
    }
    const customDataImportUids = toDelete.map((upload) => upload.id);
    const request = Object.assign({}, config, {
      resource: {customDataImportUids},
    });
    const analytics = await this.getApiClient_();
    await analytics.management.uploads.deleteUploadData(request);
    this.logger.debug('Delete uploads: ', customDataImportUids);
  }

  /**
   * Returns the uploaded data files to delete based on the options.
   * @param {!DataImportConfig} config GA data import configuration.
   * @param {!DataImportClearConfig} options
   * @return {!Promise<!Array<{
   *   id: string,
   *   kind: 'analytics#upload',
   *   accountId: string,
   *   customDataSourceId: string,
   *   status: string,
   *   uploadTime: string,
   *   errors: !Array<string>,
   * }>>}
   * @private
   */
  async getUploadsToDelete_(config, options) {
    /** @const {Array} */ const uploads = await this.listUploads(config);
    let toDelete = [];
    if (uploads) {
      // Sorts the uploads
      uploads.sort(({uploadTime: first}, {uploadTime: second}) => {
        return Date.parse(first) - Date.parse(second);
      });
      // Gets expired uploads.
      if (options.ttl) {
        const cutOff = Date.now() - options.ttl * 24 * 3600 * 1000;
        toDelete = uploads.filter(
            (upload) => Date.parse(upload.uploadTime) < cutOff);
      }
      // If there is a number limit, remove more uploads if exceeds.
      if (options.max && uploads.length - toDelete.length > options.max) {
        const leftUpdates = uploads.slice(toDelete.length);
        toDelete = toDelete.concat(
            leftUpdates.slice(0, leftUpdates.length - options.max));
      }
    }
    return toDelete;
  }

}

module.exports = {
  Analytics,
  DataImportConfig,
  DataImportClearConfig,
  API_VERSION,
  API_SCOPES,
};
