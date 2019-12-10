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
const AuthClient = require('./auth_client.js');
const {getLogger, getFilterFunction} = require('../components/utils.js');

const CAMPAIGN_MANAGER_API_SCOPES = [
  'https://www.googleapis.com/auth/ddmconversions',
  'https://www.googleapis.com/auth/dfareporting',
  'https://www.googleapis.com/auth/dfatrafficking',
];

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
    const authClient = new AuthClient(CAMPAIGN_MANAGER_API_SCOPES);
    this.instance = authClient.getDefaultAuth().then((auth) => {
      return google.dfareporting({version: 'v3.3', auth: auth});
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
    return this.instance
        .then((dfa) => {
          return dfa.userProfiles.list();
        })
        .then((response) => {
          const profiles = response.data.items.filter((profile) => {
            return profile.accountId === accountId;
          });
          if (profiles.length === 0) {
            return Promise.reject(
                `Fail to find profile of current user for CM account ${
                    accountId}`);
          } else {
            const profile = profiles[0];
            this.logger.debug(`Find UserProfile: ${profile.profileId}[${
                profile.userName}] for account: ${profile.accountId}[${
                profile.accountName}]`);
            return Promise.resolve(profile.profileId);
          }
        });
  }

  /**
   * Returns the function to sends out a request to CM with a batch of
   * conversions.
   * @param {!InsertConversionsConfig} config Campaign Manager configuration.
   * @return {function(!Array<string>, string): !Promise<boolean>} Function
   *     which can send a batch of hits to Campaign Manager.
   */
  getUploadConversionFn(config) {
    /**
     * Sends a batch of hits to Campaign Manager.
     * @param {!Array<string>} lines Data for single request. It should be
     *     guaranteed that it doesn't exceed quota limitation.
     * @param {string} batchId The tag for log.
     * @return {!Promise<boolean>}
     */
    const sendRequest = (lines, batchId) => {
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
          conversion.customVariables =
              config.customVariables.map((variable) => {
                return {
                  'type': variable,
                  'value': record[variable],
                };
              });
        }
        return conversion;
      });
      return this.instance.then((dfa) => {
        const requestBody = {conversions: conversions};
        if (config.idType === 'encryptedUserId') {
          requestBody.encryptionInfo = config.encryptionInfo;
        }
        return dfa.conversions
            .batchinsert({
              profileId: config.profileId,
              requestBody: requestBody,
            })
            .then((response) => {
              const failed = response.data.hasFailures;
              if (failed) {
                console.error(`Dfareporting[${batchId}] has failures.`);
                const errorMessages =
                    response.data.status
                        .filter((record) => {
                          return typeof record.errors !== 'undefined';
                        })
                        .map((record) => {
                          this.logger.debug(record);
                          return record.errors
                              .map((error) => {
                                return error.message;
                              })
                              .join(',');
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
      });
    };
    return sendRequest;
  };

  /**
   * Lists all UserProfiles.
   * @return {!Promise<!Array<string>>}
   */
  listUserProfiles() {
    return this.instance.then((dfa) => {
      return dfa.userProfiles.list().then((response) => {
        return response.data.items.map((profile) => {
          return `Profile: ${profile.profileId}[${profile.userName}] Account: ${
              profile.accountId}[${profile.accountName}]`;
        });
      });
    });
  }
}

exports.DfaReporting = DfaReporting;
exports.InsertConversionsConfig = InsertConversionsConfig;
