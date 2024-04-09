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
 * @fileoverview Tentacles API handler for Google Ads Customer Match
 * uploading (Google Ads API (unofficial) Wrapper).
 */

'use strict';

const {
  api: { googleadsapi: { GoogleAdsApi: GoogleAds, CustomerMatchConfig } },
  utils: { getProperValue, changeObjectNamingFromSnakeToLowerCamel, BatchResult },
} = require('@google-cloud/nodejs-common');
const { GoogleAdsClickConversionUpload } =
  require('./google_ads_click_conversions_upload.js');

/**
 * @see https://developers.google.com/google-ads/api/docs/best-practices/quotas
 * However UserDataService has limit of 10 operations and 100 userIds per request
 * @see https://developers.google.com/google-ads/api/docs/migration/user-data-service#rate_limits
 * Based on this: https://ads-developers.googleblog.com/2021/10/userdata-enforcement-in-google-ads-api.html
 * Each operation only contains the data for one user, so the limited number of
 * records for a single request is 10 (operations).
 */
const RECORDS_PER_REQUEST = 10;

/**
 * Queries per second. Google Ads has no limits on queries per second, however
 * it has limits on the gRPC size (4MB), so large requests may fail.
 * This can be overwritten by configuration.
 */
const QUERIES_PER_SECOND = 10;

/**
 * Configuration for a Google Ads customer match upload.
 * @typedef {{
*   developerToken:string,
*   customerMatchConfig: !CustomerMatchConfig,
*   recordsPerRequest:(number|undefined),
*   qps:(number|undefined),
*   secretName:(string|undefined),
* }}
*/
let GoogleAdsCustomerMatchConfig;

/**
 * Customer match user data upload for Google Ads.
 */
class GoogleAdsCustomerMatch extends GoogleAdsClickConversionUpload {

  /** @override */
  getSpeedOptions(config) {
    const recordsPerRequest =
      getProperValue(config.recordsPerRequest, RECORDS_PER_REQUEST);
    const qps = getProperValue(config.qps, QUERIES_PER_SECOND, false);
    const numberOfThreads = qps;
    return { recordsPerRequest, numberOfThreads, qps };
  }

  /**
   * Gets the user list Id based on the specified name and type.
   * Creates the list if it doesn't exist and returns the Id.
   * @param {GoogleAds} googleAds Injected Google Ads instance.
   * @param {{
   *   listId:(string|undefined),
   *   listName:(string|undefined),
   *   uploadKeyType:('CONTACT_INFO'|'CRM_ID'|'MOBILE_ADVERTISING_ID'|undefined),
   * }} config
   * @return {string} User list Id.
   */
  async getOrCreateUserList(googleAds, config) {
    if (config.listId) return config.listId;
    if (!config.listName || !config.uploadKeyType) {
      throw new Error(`Missing user list info in ${JSON.stringify(config)}`);
    }
    const listId = await googleAds.getCustomerMatchUserListId(config);
    if (listId) {
      this.logger.info(`Get UserList id ${listId}.`);
      return listId;
    } else {
      const createdListId = await googleAds.createCustomerMatchUserList(config);
      this.logger.info(`Create UserList id ${createdListId}.`);
      return createdListId;
    }
  }

  /**
   * Sends out the data as user ids to Google Ads API.
   * This function exposes a googleAds parameter for test
   * @param {GoogleAds} googleAds Injected Google Ads instance.
   * @param {string} records Data to send out as user ids. Expected JSON
   *     string in each line.
   * @param {string} messageId Pub/sub message ID for log.
   * @param {!GoogleAdsCustomerMatchConfig} config
   * @return {!Promise<BatchResult>}
   */
  async sendDataInternal(googleAds, records, messageId, config) {
    const { customerMatchConfig } =
      changeObjectNamingFromSnakeToLowerCamel(config);
    try {
      customerMatchConfig.listId =
        await this.getOrCreateUserList(googleAds, customerMatchConfig);
    } catch (error) {
      this.logger.error('Error in UserdataService: ', error);
      return this.getResultFromError(googleAds, error);
    }
    const managedSend = this.getManagedSendFn(config);
    const configedUpload =
      googleAds.getUploadCustomerMatchFn(customerMatchConfig);
    return managedSend(configedUpload, records, messageId);
  }
}

/** API name in the incoming file name. */
GoogleAdsCustomerMatch.code = 'ACM';

module.exports = {
  GoogleAdsCustomerMatchConfig,
  GoogleAdsCustomerMatch,
};
