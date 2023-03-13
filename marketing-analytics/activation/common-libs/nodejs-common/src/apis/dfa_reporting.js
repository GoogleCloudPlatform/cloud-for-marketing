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
 * @fileoverview Google Campaign Manager Conversions uploading (DfaReport API)
 * on Google API Client Library.
 */

'use strict';

const {google} = require('googleapis');
const {request} = require('gaxios');
const AuthClient = require('./auth_client.js');
const {
  getLogger,
  getFilterFunction,
  SendSingleBatch,
  BatchResult,
} = require('../components/utils.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/ddmconversions',
  'https://www.googleapis.com/auth/dfareporting',
  'https://www.googleapis.com/auth/dfatrafficking',
]);
const API_VERSION = 'v4';

/**
 * Configuration for preparing conversions for Campaign Manager, includes:
 * profileId, idType, conversion, customVariables, encryptionInfo.
 * The 'idType' can be one of the values: 'encryptedUserId', 'gclid' or
 * 'mobileDeviceId'.
 * For other properties, see
 * https://developers.google.com/doubleclick-advertisers/guides/conversions_update
 *
 * @typedef {{
 *   profileId:string,
 *   idType:string,
 *   conversion:{
 *     floodlightConfigurationId:string,
 *     floodlightActivityId:string,
 *     quantity:(number|undefined),
 *   },
 *   customVariables:(!Array<string>|undefined),
 *   encryptionInfo:({
 *     encryptionEntityId:string,
 *     encryptionEntityType:string,
 *     encryptionSource:string,
 *   }|undefined),
 * }}
 */
let InsertConversionsConfig;

/**
 * List of properties that will be take from the data file as elements of a
 * conversion.
 * See https://developers.google.com/doubleclick-advertisers/rest/v4/Conversion
 * @type {Array<string>}
 */
const PICKED_PROPERTIES = [
  'ordinal',
  'timestampMicros',
  'value',
  'quantity',
];

/**
 * Google DfaReport API v3.0 stub.
 * see https://developers.google.com/doubleclick-advertisers/service_accounts
 */
class DfaReporting {

  /**
   * @constructor
   * @param {!Object<string,string>=} env The environment object to hold env
   *     variables.
   */
  constructor(env = process.env) {
    this.authClient = new AuthClient(API_SCOPES, env);
    this.logger = getLogger('API.CM');
  }

  /**
    * Prepares the Google DfaReport API instance.
    * @return {!google.dfareporting}
    * @private
    */
  async getApiClient_() {
    if (this.dfareporting) return this.dfareporting;
    this.logger.debug(`Initialized ${this.constructor.name} instance.`);
    this.dfareporting = google.dfareporting({
      version: API_VERSION,
      auth: await this.getAuth_(),
    });
    return this.dfareporting;
  }

  /**
   * Gets the auth object.
   * @return {!Promise<{!OAuth2Client|!JWT|!Compute}>}
   */
  async getAuth_() {
    if (this.auth) return this.auth;
    await this.authClient.prepareCredentials();
    this.auth = this.authClient.getDefaultAuth();
    return this.auth;
  }

  /**
   * Gets the UserProfile ID for the current (authenticated) user and the given
   * CM account. The profile must exist, otherwise will generate a Promise
   * reject.
   * @param {string} accountId Campaign Manager UserProfile ID.
   * @return {!Promise<string>}
   */
  async getProfileId(accountId) {
    const dfareporting = await this.getApiClient_();
    const { data: { items } } = await dfareporting.userProfiles.list();
    const profiles = items.filter(
        (profile) => profile.accountId === accountId
    );
    if (profiles.length === 0) {
      throw new Error(
        `Failed to find profile of current user for CM account ${accountId}`);
    } else {
      const {profileId, userName, accountId, accountName,} = profiles[0];
      this.logger.debug(`Find UserProfile: ${profileId}[${userName}] for`
          + ` account: ${accountId}[${accountName}]`);
      return profileId;
    }
  }

  /**
   * Returns the function to sends out a request to CM with a batch of
   * conversions.
   * @param {!InsertConversionsConfig} config Campaign Manager configuration.
   * @return {!SendSingleBatch} Function which can send a batch of hits to
   *     Campaign Manager.
   */
  getUploadConversionFn(config) {
    /**
     * Sends a batch of hits to Campaign Manager.
     * @param {!Array<string>} lines Data for single request. It should be
     *     guaranteed that it doesn't exceed quota limitation.
     * @param {string} batchId The tag for log.
     * @return {!Promise<BatchResult>}
     */
    return async (lines, batchId) => {
      /** @type {function} Gets the conversion elements from the data object. */
      const filterObject = getFilterFunction(PICKED_PROPERTIES);
      const time = new Date().getTime();
      const conversions = lines.map((line) => {
        const record = JSON.parse(line);
        const conversion = Object.assign(
            {
              // Default value, can be overwritten by the exported data.
              ordinal: time,
              timestampMicros: time * 1000,
            },
            config.conversion, filterObject(record));
        conversion[config.idType] = record[config.idType];
        // Custom Variables
        if (typeof config.customVariables !== 'undefined') {
          conversion.customVariables = config.customVariables.map(
              (variable) => ({'type': variable, 'value': record[variable],}));
        }
        return conversion;
      });
      const requestBody = {conversions};
      if (config.idType === 'encryptedUserId') {
        requestBody.encryptionInfo = config.encryptionInfo;
      }
      /** @const {BatchResult} */
      const batchResult = {
        result: true,
        numberOfLines: lines.length,
      };
      try {
        const dfareporting = await this.getApiClient_();
        const response = await dfareporting.conversions.batchinsert({
          profileId: config.profileId,
          requestBody: requestBody,
        });
        const failed = response.data.hasFailures;
        if (failed) {
          this.logger.warn(`CM [${batchId}] has failures.`);
          this.extraFailedLines_(batchResult, response.data.status, lines);
        }
        this.logger.debug('Configuration: ', config);
        this.logger.debug('Response: ', response);
        return batchResult;
      } catch (error) {
        this.logger.error(`CM[${batchId}] failed.`, error);
        batchResult.result = false;
        batchResult.errors = [error.message || error.toString()];
        return batchResult;
      }
    };
  };

  /**
   * Campaign Manager API returns an array of ConversionStatus for the status of
   * uploaded conversions. If there are errors related to the conversion, then
   * an array of 'ConversionError' named 'errors' will be available in the
   * ConversionStatus object. This function extras failed lines and error
   * messages based on the 'errors'.
   * For 'ConversionStatus', see:
   *   https://developers.google.com/doubleclick-advertisers/rest/v4/ConversionStatus
   * For 'ConversionError', see:
   *   https://developers.google.com/doubleclick-advertisers/rest/v4/ConversionStatus#ConversionError
   * @param {!BatchResult} batchResult
   * @param {!Array<!Schema$ConversionStatus>} statuses
   * @param {!Array<string>} lines The original input data.
   * @private
   */
  extraFailedLines_(batchResult, statuses, lines) {
    batchResult.result = false;
    batchResult.failedLines = [];
    batchResult.groupedFailed = {};
    const errors = new Set();
    statuses.forEach((conversionStatus, index) => {
      if (conversionStatus.errors) {
        const failedLine = lines[index];
        batchResult.failedLines.push(failedLine);
        conversionStatus.errors.forEach(({message}) => {
          // error messages have detailed IDs. Need to generalize them.
          const generalMessage = message.replace(/.*error: /, '');
          errors.add(generalMessage);
          const groupedFailed = batchResult.groupedFailed[generalMessage]
              || [];
          groupedFailed.push(failedLine);
          if (groupedFailed.length === 1) {
            batchResult.groupedFailed[generalMessage] = groupedFailed;
          }
        });
      }
      batchResult.errors = Array.from(errors);
    });
  }

  /**
   * Lists all UserProfiles.
   * @return {!Promise<!Array<string>>}
   */
  async listUserProfiles() {
    const dfareporting = await this.getApiClient_();
    const { data: { items } } = await dfareporting.userProfiles.list();
    return items.map(({profileId, userName, accountId, accountName}) => {
      return `Profile: ${profileId}[${userName}] `
          + `Account: ${accountId}[${accountName}]`;
    });
  }

  /**
   * Returns profile ID based on given config.
   * If there is profileId in the config, just return a Promise resolve it;
   * if there is accountId, uses the accountId to get profileId and returns it;
   * Otherwise, throws an error.
   * @param {{
   *   accountId:(string|undefined),
   *   profileId:(string|undefined),
   * }} config
   * @return {!Promise<string>} Profile Id.
   * @private
   */
  async getProfileForOperation_(config) {
    if (config.profileId) return config.profileId;
    if (config.accountId) return this.getProfileId(config.accountId);
    throw new Error('There is no profileId or accountId in the configuration.');
  }

  /**
   * Runs a report and return the file Id. As an asynchronized process, the
   * returned file Id will be a placeholder until the status changes to
   * 'REPORT_AVAILABLE' in the response of `getFile`.
   * Campaign Manager reports use a fixed timezone(America/Los Angeles) when it
   * has a relative date range, e.g. YESTERDAY. To solve this, if there are
   * `startDate` and `endDate` available in the given `config`, they will be
   * used to update the report before the report is started to run.
   * @see https://developers.google.com/doubleclick-advertisers/rest/v4/reports/run
   *
   * @param {{
   *   accountId:(string|undefined),
   *   profileId:(string|undefined),
   *   reportId:string,
   * }} config
   * @return {!Promise<string>} FileId of report run.
   */
  async runReport(config) {
    const profileId = await this.getProfileForOperation_(config);
    const dfareporting = await this.getApiClient_();
    const { startDate, endDate } = config;
    if (startDate && endDate) {
      const { data: report } = await dfareporting.reports.get({
        profileId,
        reportId: config.reportId,
      });
      report.criteria.dateRange = { startDate, endDate };
      const updated = await dfareporting.reports.update({
        profileId,
        reportId: config.reportId,
        requestBody: report
      });
      if (updated.status >= 400) {
        this.logger.error(
          'Failed to update data range of CM360 report', config.reportId);
        this.logger.error('Report to be updated', report);
      }
    }
    const response = await dfareporting.reports.run({
      profileId,
      reportId: config.reportId,
      synchronous: false,
    });
    return response.data.id;
  }

  /**
   * Returns file url from a report. If the report status is 'REPORT_AVAILABLE',
   * then return the apiUrl from the response; if the status is 'PROCESSING' or
   * 'QUEUED', returns undefined as it is unfinished; otherwise throws an error.
   * @see https://developers.google.com/doubleclick-advertisers/rest/v4/reports/get
   *
   * @param {{
   *   accountId:(string|undefined),
   *   profileId:(string|undefined),
   *   reportId:string,
   *   fileId:string,
   * }} config
   * @return {!Promise<(string|undefined)>} FileId of report run.
   */
  async getReportFileUrl(config) {
    const profileId = await this.getProfileForOperation_(config);
    const dfareporting = await this.getApiClient_();
    const response = await dfareporting.reports.files.get({
      profileId,
      reportId: config.reportId,
      fileId: config.fileId,
    });
    const { data } = response;
    if (data.status === 'PROCESSING' || data.status === 'QUEUED') return;
    if (data.status === 'REPORT_AVAILABLE') return data.urls.apiUrl;
    throw new Error(`Unsupported report status: ${data.status}`);
  }

  //TODO(lushu) check the response for very big file.
  /**
   * Downloads the report file.
   * @param {string} url
   * @return {!Promise<string>}
   */
  async downloadReportFile(url) {
    const auth = await this.getAuth_();
    const headers = await auth.getRequestHeaders();
    const response = await request({
      method: 'GET',
      headers,
      url,
    });
    return response.data;
  }
}

module.exports = {
  DfaReporting,
  InsertConversionsConfig,
  API_VERSION,
  API_SCOPES,
};
