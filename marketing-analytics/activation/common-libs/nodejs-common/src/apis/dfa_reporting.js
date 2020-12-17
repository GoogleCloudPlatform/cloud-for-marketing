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
} = require('../components/utils.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/ddmconversions',
  'https://www.googleapis.com/auth/dfareporting',
  'https://www.googleapis.com/auth/dfatrafficking',
]);
const API_VERSION = 'v3.3';

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
 * See https://developers.google.com/doubleclick-advertisers/v3.3/conversions
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
  constructor() {
    const authClient = new AuthClient(API_SCOPES);
    this.auth = authClient.getDefaultAuth();
    /** @const {!google.dfareporting} */
    this.instance = google.dfareporting({
      version: API_VERSION,
      auth: this.auth,
    });
    this.logger = getLogger('API.CM');
  }

  /**
   * Gets the UserProfile ID for the current (authenticated) user and the given
   * CM account. The profile must exist, otherwise will generate a Promise
   * reject.
   * @param {string} accountId Campaign Manager UserProfile ID.
   * @return {!Promise<string>}
   */
  getProfileId(accountId) {
    return this.instance.userProfiles.list()
        .then(({data: {items}}) =>
            items.filter((profile) => profile.accountId === accountId))
        .then((profiles) => {
          if (profiles.length === 0) {
            return Promise.reject(
                `Fail to find profile of current user for CM account ${
                    accountId}`);
          } else {
            const profile = profiles[0];
            this.logger.debug(`Find UserProfile: ${profile.profileId}[${
                profile.userName}] for account: ${profile.accountId}[${
                profile.accountName}]`);
            return profile.profileId;
          }
        });
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
     * @return {!Promise<boolean>}
     */
    return (lines, batchId) => {
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
      return this.instance.conversions.batchinsert({
            profileId: config.profileId,
            requestBody: requestBody,
          })
          .then((response) => {
            const failed = response.data.hasFailures;
            if (failed) {
              console.error(`Dfareporting[${batchId}] has failures.`);
              const errorMessages = response.data.status
                  .filter((record) => typeof record.errors !== 'undefined')
                  .map((record) => {
                    this.logger.debug(record);
                    return record.errors.map(
                        (error) => error.message).join(',');
                  });
              console.error(errorMessages.join('\n'));
            }
            this.logger.debug('Configuration: ', config);
            this.logger.debug('Response: ', response);
            return !failed;
          })
          .catch((error) => {
            console.error(`Dfareporting[${batchId}] failed.`, error);
            return false;
          });
    };
  };

  /**
   * Lists all UserProfiles.
   * @return {!Promise<!Array<string>>}
   */
  listUserProfiles() {
    return this.instance.userProfiles.list().then(({data: {items}}) => {
      return items.map(({profileId, userName, accountId, accountName}) => {
        return `Profile: ${profileId}[${userName}] Account: ${
            accountId}[${accountName}]`;
      });
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
  getProfileForOperation_(config) {
    if (config.profileId) return Promise.resolve(config.profileId);
    if (config.accountId) return this.getProfileId(config.accountId);
    throw new Error('There is no profileId or accountId in the configuration.');
  }

  /**
   * Runs a report and return the file Id. As an asynchronized process, the
   * returned file Id will be a placeholder until the status changes to
   * 'REPORT_AVAILABLE' in the response of `getFile`.
   * @see https://developers.google.com/doubleclick-advertisers/v3.3/reports/run
   *
   * @param {{
   *   accountId:(string|undefined),
   *   profileId:(string|undefined),
   *   reportId:string,
   * }} config
   * @return {!Promise<string>} FileId of report run.
   */
  runReport(config) {
    return this.getProfileForOperation_(config)
        .then((profileId) => {
              return this.instance.reports.run({
                profileId,
                reportId: config.reportId,
                synchronous: false,
              });
            }
        ).then((response) => response.data.id);
  }

  /**
   * Returns file url from a report. If the report status is 'REPORT_AVAILABLE',
   * then return the apiUrl from the response; if the status is 'PROCESSING',
   * returns undefined; otherwise throws an error.
   * @see https://developers.google.com/doubleclick-advertisers/v3.3/reports/files/get
   *
   * @param {{
   *   accountId:(string|undefined),
   *   profileId:(string|undefined),
   *   reportId:string,
   *   fileId:string,
   * }} config
   * @return {!Promise<(string|undefined)>} FileId of report run.
   */
  getReportFileUrl(config) {
    return this.getProfileForOperation_(config)
        .then((profileId) => {
              return this.instance.reports.files.get({
                profileId,
                reportId: config.reportId,
                fileId: config.fileId,
              });
            }
        ).then((response) => {
          const data = response.data;
          if (data.status === 'PROCESSING') return;
          if (data.status === 'REPORT_AVAILABLE') return data.urls.apiUrl;
          throw new Error(`Unsupport report status: ${data.status}`);
        });
  }

  //TODO(lushu) check the response for very big file.
  /**
   * Downloads the report file.
   * @param {string} url
   * @return {!Promise<string>}
   */
  downloadReportFile(url) {
    return this.auth.getRequestHeaders()
        .then((headers) => {
          return request({
            method: 'GET',
            headers,
            url,
          });
        })
        .then((response) => response.data);
  }
}

module.exports = {
  DfaReporting,
  InsertConversionsConfig,
  API_VERSION,
  API_SCOPES,
};
