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
 * @fileoverview Google Ads API (unofficial) Wrapper.
 */
'use strict';

const {protos: {google: {ads: {googleads}}}} = require('google-ads-node');
const {
  v7: {
    common: {
      UserData,
      UserIdentifier,
      CustomerMatchUserListMetadata,
    },
    resources: {
      GoogleAdsField,
    },
    services: {
      UploadClickConversionsRequest,
      UploadUserDataRequest,
      UserDataOperation,
      SearchGoogleAdsFieldsRequest,
    },
  }
} = googleads;
const {GoogleAdsApi} = require('google-ads-api');
const AuthClient = require('./auth_client.js');
const {getLogger} = require('../components/utils.js');

/** @type {!ReadonlyArray<string>} */
const API_SCOPES = Object.freeze(['https://www.googleapis.com/auth/adwords',]);

/**
 * Configuration for uploading click conversions for Google Ads, includes:
 * gclid, conversion_action, conversion_date_time, conversion_value,
 * currency_code, order_id, external_attribution_data
 * @see https://developers.google.com/google-ads/api/reference/rpc/v7/ClickConversion
 * @typedef {{
 *   gclid: string,
 *   conversion_action: string,
 *   conversion_date_time: string,
 *   conversion_value: number,
 *   currency_code:(string|undefined),
 *   order_id: (string|undefined),
 *   external_attribution_data: (GoogleAdsApi.ExternalAttributionData|undefined),
 * }}
 */
let ClickConversionConfig;

/**
 * Configuration for uploading customer match to Google Ads, includes:
 * customer_id, login_customer_id, list_id, list_type and operation
 * list_type must be one of the following: hashed_email,
 * hashed_phone_number, mobile_id, third_party_user_id or address_info;
 * operation must be one of the two: 'create' or 'remove';
 * @see https://developers.google.com/google-ads/api/reference/rpc/v7/UserDataOperation
 * @typedef {{
 *   customer_id: string,
 *   login_customer_id: string,
 *   list_id: string,
 *   list_type: 'hashed_email'|'hashed_phone_number'|'mobile_id'|
 *       'third_party_user_id'|'address_info',
 *   operation: 'create'|'remove',
 * }}
 */
let CustomerMatchConfig;

/**
 * Configuration for uploading customer match data for Google Ads, includes one of:
 * hashed_email, hashed_phone_number, mobile_id, third_party_user_id or address_info
 * @see https://developers.google.com/google-ads/api/reference/rpc/v7/UserIdentifier
 * @typedef {{
 *   hashed_email: string,
 * }|{
 *   hashed_phone_number: string,
 * }|{
 *   mobile_id: string,
 * }|{
 *   third_party_user_id: string,
 * }|{
 *   address_info: GoogleAdsApi.OfflineUserAddressInfo,
 * }}
 */
let CustomerMatchRecord;

/**
 * Configuration for querying report from Google Ads, includes:
 * entity, attributes, metrics, and constraints etc.
 * For other properties, see
 * https://opteo.com/dev/google-ads-api/#report
 * https://developers.google.com/google-ads/api/docs/query/grammar
 * @typedef {{
 *   entity:string,
 *   attributes:(!Array<string>|undefined),
 *   metrics:(!Array<string>|undefined),
 *   segments:(!Array<string>|undefined),
 *   constraints:(!Array<{
 *     key:string,
 *     op:string,
 *     val:string,
 *   }|{string,string}>|undefined),
 *   date_constant:(string|undefined),
 *   from_date:(string|undefined),
 *   to_date:(string|undefined),
 *   limit:(number|undefined),
 *   order_by:(string|undefined),
 *   sort_order:('DESC'|'ASC'|undefined),
 *   page_size:(number|undefined),
 * }}
 */
let ReportQueryConfig;

/**
 * Google Ads API v6.1 stub.
 * see https://opteo.com/dev/google-ads-api/#features
 */
class GoogleAds {
  /**
   * Note: Rate limits is set by the access level of Developer token.
   * @param {string} developerToken Developer token to access the API.
   */
  constructor(developerToken) {
    this.debug = process.env['DEBUG'] === 'true';
    const oauthClient = new AuthClient(API_SCOPES).getOAuth2Token();
    /** @const {GoogleAdsApi} */ this.apiClient = new GoogleAdsApi({
      client_id: oauthClient.clientId,
      client_secret: oauthClient.clientSecret,
      developer_token: developerToken,
    });
    /** @const {string} */ this.refreshToken = oauthClient.refreshToken;
    this.logger = getLogger('API.ADS');
    this.logger.debug(`Init ${this.constructor.name} with Debug Mode.`);
  }

  /**
   * Gets report as generator of a given Customer account.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {!ReportQueryConfig} reportQueryConfig
   * @return {!ReadableStream}
   */
  async generatorReport(customerId, loginCustomerId, reportQueryConfig) {
    const customer = this.getGoogleAdsApiCustomer_(loginCustomerId, customerId);
    return customer.reportStream(reportQueryConfig);
  }

  /**
   * Gets stream report of a given Customer account.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {!ReportQueryConfig} reportQueryConfig
   * @return {!ReadableStream}
   */
  async streamReport(customerId, loginCustomerId, reportQueryConfig) {
    const customer = this.getGoogleAdsApiCustomer_(loginCustomerId, customerId);
    return customer.reportStreamRaw(reportQueryConfig);
  }

  /**
   * Returns resources information from Google Ads API. see:
   * https://developers.google.com/google-ads/api/docs/concepts/field-service
   * Note, it looks like this function doesn't check the CID, just using
   * developer token and OAuth.
   * @param {string|number} loginCustomerId Login customer account ID.
   * @param {Array<string>} adFields Array of Ad fields.
   * @param {Array<string>} metadata Select fields, default values are:
   *     name, data_type, is_repeated, type_url.
   * @return {!Promise<!Array<GoogleAdsField>>}
   */
  async searchMetaData(loginCustomerId, adFields, metadata = [
    'name', 'data_type', 'is_repeated', 'type_url',]) {
    const customer = this.getGoogleAdsApiCustomer_(loginCustomerId);
    const selectClause = metadata.join(',');
    const fields = adFields.join('","');
    const query = `SELECT ${selectClause} WHERE name IN ("${fields}")`;
    const request = new SearchGoogleAdsFieldsRequest({query});
    const [results] = await customer.googleAdsFields
        .searchGoogleAdsFields(request);
    return results;
  }

  /**
   * Returns the function to send out a request to Google Ads API with a batch
   * of click conversions.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {!ClickConversionConfig} adsConfig Default click conversion params
   * @return {!SendSingleBatch} Function which can send a batch of hits to
   *     Google Ads API.
   */
  getUploadConversionFn(customerId, loginCustomerId, adsConfig) {
    /**
     * Sends a batch of hits to Google Ads API.
     * @param {!Array<string>} lines Data for single request. It should be
     *     guaranteed that it doesn't exceed quota limitation.
     * @param {string} batchId The tag for log.
     * @return {!Promise<boolean>}
     */
    return async (lines, batchId) => {
      /** @type {!Array<ClickConversionConfig>} */
      const conversions = lines.map((line) => {
        const record = JSON.parse(line);
        return Object.assign({}, adsConfig, record);
      });
      try {
        return await this.uploadClickConversions(conversions, customerId,
            loginCustomerId);
      } catch (error) {
        this.logger.info(
            `Error in getUploadConFn in batchId: ${batchId}`, error);
        return false;
      }
    }
  }

  /**
   * Uploads click conversions to google ads account.
   * It requires an array of click conversions and customer id.
   * In DEBUG mode, this function will only validate the conversions.
   * @param {Array<ClickConversionConfig>} clickConversions ClickConversions
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @return {!Promise<boolean>}
   */
  async uploadClickConversions(clickConversions, customerId, loginCustomerId) {
    this.logger.debug('Upload click conversions for customerId:', customerId);
    const customer = this.getGoogleAdsApiCustomer_(loginCustomerId, customerId);
    const request = new UploadClickConversionsRequest({
      conversions: clickConversions,
      customer_id: customerId,
      validate_only: this.debug, // when true makes no changes
      partial_failure: true, // Will still create the non-failed entities
    });
    const result = await customer.conversionUploads.uploadClickConversions(
        request);
    const {results, partial_failure_error: failed} = result;
    const response = results.map((conversion) => conversion.gclid);
    this.logger.debug('Uploaded gclids:', response);
    // const failed = result.partial_failure_error;
    // Note: the response is different from previous version. current 'message'
    // only contains partial failed conversions. The other field 'details'
    // contains more information with the type of Array<Buffer>.
    if (failed) {
      this.logger.info('Errors:', failed.message);
    }
    return !failed;
  }

  /**
   * Returns the function to send out a request to Google Ads API with
   * user ids for Customer Match upload
   * @param {!CustomerMatchConfig} customerMatchConfig
   * @return {!SendSingleBatch} Function which can send a batch of hits to
   *     Google Ads API.
   */
  getUploadCustomerMatchFn(customerMatchConfig) {
    /**
     * Sends a batch of hits to Google Ads API.
     * @param {!Array<string>} lines Data for single request. It should be
     *     guaranteed that it doesn't exceed quota limitation.
     * @param {string} batchId The tag for log.
     * @return {!Promise<boolean>}
     */
    return async (lines, batchId) => {
      /** @type {Array<CustomerMatchRecord>} */
      const userIds = lines.map((line) => JSON.parse(line));
      try {
        return await this.uploadUserDataToUserList(userIds,
            customerMatchConfig);
      } catch (error) {
        this.logger.error(
            `Error in getUploadCustomerMatchFn in batchId: ${batchId}`, error);
        return false;
      }
    }
  }

  /**
   * Uploads a user data to a user list (aka customer match).
   * @see https://developers.google.com/google-ads/api/reference/rpc/v7/UserDataService
   * @see https://developers.google.com/google-ads/api/reference/rpc/v7/UserDataOperation
   * @see https://developers.google.com/google-ads/api/reference/rpc/v7/UserData
   * @see https://developers.google.com/google-ads/api/reference/rpc/v7/UserIdentifier
   * @see https://developers.google.com/google-ads/api/reference/rpc/v7/CustomerMatchUserListMetadata
   * Please note: The UserDataService has a limit of 10 UserDataOperations
   * and 100 user IDs per request
   * @see https://developers.google.com/google-ads/api/docs/migration/user-data-service#rate_limits
   * @param {!Array<CustomerMatchRecord>} customerMatchRecords user Ids
   * @param {CustomerMatchConfig} customerMatchConfig Customer Match config containing
   * customer_id, login_customer_id, list_id, list_type which can be one of the following
   * hashed_email, hashed_phone_number, mobile_id, third_party_user_id or address_info and
   * operation which can be either 'create' or 'remove'
   * @return {!Promise<boolean>}
   */
  async uploadUserDataToUserList(customerMatchRecords, customerMatchConfig) {
    const customerId = customerMatchConfig.customer_id.replace(/-/g, '');
    const loginCustomerId = customerMatchConfig.login_customer_id.replace(/-/g,
        '');
    const userListType = customerMatchConfig.list_type;
    const userListId = customerMatchConfig.list_id;
    const operation = customerMatchConfig.operation;

    const customer = this.getGoogleAdsApiCustomer_(loginCustomerId, customerId);
    const operationsList = this.buildOperationsList_(operation,
        customerMatchRecords, userListType);
    const metadata = this.buildCustomerMatchUserListMetadata_(customerId,
        userListId);
    const request = UploadUserDataRequest.create({
      customer_id: customerId,
      operations: [operationsList],
      customer_match_user_list_metadata: metadata,
    });
    const response = await customer.userData.uploadUserData(request);
    this.logger.debug('Uploaded CM users:', response);
    return true;
  }

  /**
   * Builds a list of UserDataOperations.
   * Since v6 you can set a user_attribute
   * @see https://developers.google.com/google-ads/api/reference/rpc/v7/UserData
   * @see https://developers.google.com/google-ads/api/reference/rpc/v7/UserIdentifier
   * @see https://developers.google.com/google-ads/api/reference/rpc/v7/UserDataOperation
   * @param {string} operationType either 'create' or 'remove'
   * @param {Array<CustomerMatchRecord>} customerMatchRecords userIds
   * @param {string} userListType One of the following hashed_email, hashed_phone_number,
   *     mobile_id, third_party_user_id or address_info
   * @return {Array<UserDataOperation>}
   * @private
   */
  buildOperationsList_(operationType, customerMatchRecords, userListType) {
    const userIdentifiers = customerMatchRecords.map((customerMatchRecord) => {
      return UserIdentifier.create(
          {[userListType]: customerMatchRecord[userListType]});
    });
    const userData = UserData.create({user_identifiers: userIdentifiers});
    return UserDataOperation.create({[operationType]: userData});
  }

  /**
   * Creates CustomerMatchUserListMetadata.
   * @see https://developers.google.com/google-ads/api/reference/rpc/v7/CustomerMatchUserListMetadata
   * @param {string} customerId part of the ResourceName to be mutated
   * @param {string} userListId part of the ResourceName to be mutated
   * @return {!CustomerMatchUserListMetadata}
   * @private
   */
  buildCustomerMatchUserListMetadata_(customerId, userListId) {
    const resourceName = `customers/${customerId}/userLists/${userListId}`;
    return CustomerMatchUserListMetadata.create({
      user_list: resourceName,
    });
  }

  /**
   * Returns an instance of GoogleAdsApi.Customer on google-ads-api.
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {string=} customerId Customer account ID, default is the same as
   *     the login customer account ID.
   * @return {GoogleAdsApi.Customer}
   * @private
   */
  getGoogleAdsApiCustomer_(loginCustomerId, customerId = loginCustomerId) {
    const googleAdsApiClient = this.apiClient;
    return googleAdsApiClient.Customer({
      customer_id: customerId,
      login_customer_id: loginCustomerId,
      refresh_token: this.refreshToken,
    });
  }

}

module.exports = {
  ClickConversionConfig,
  CustomerMatchRecord,
  CustomerMatchConfig,
  GoogleAds,
  ReportQueryConfig,
  GoogleAdsField,
};
