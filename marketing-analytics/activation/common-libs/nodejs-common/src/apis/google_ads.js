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
const googleAdsLib = googleads[Object.keys(googleads)[0]];
const {
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
    UploadClickConversionsResponse,
    UploadUserDataRequest,
    UploadUserDataResponse,
    UserDataOperation,
    SearchGoogleAdsFieldsRequest,
  },
  errors: {
    GoogleAdsFailure,
  },
} = googleAdsLib;
const {GoogleAdsApi} = require('google-ads-api');
const lodash = require('lodash');

const AuthClient = require('./auth_client.js');
const {getLogger, BatchResult,} = require('../components/utils.js');

/** @type {!ReadonlyArray<string>} */
const API_SCOPES = Object.freeze(['https://www.googleapis.com/auth/adwords',]);

/**
 * List of properties that will be taken from the data file as elements of a
 * conversion.
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/ClickConversion
 * @type {Array<string>}
 */
const PICKED_PROPERTIES = [
  'external_attribution_data',
  'cart_data',
  'user_identifiers',
  'gclid',
  'conversion_action',
  'conversion_date_time',
  'conversion_value',
  'currency_code',
  'order_id',
];

/**
 * Kinds of UserIdentifier.
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserIdentifier
 * @type {Array<string>}
 */
const IDENTIFIERS = [
  'hashed_email',
  'hashed_phone_number',
  'mobile_id',
  'third_party_user_id',
  'address_info',
];

/**
 * Maximum number of user identifiers in single UserData.
 * @see https://ads-developers.googleblog.com/2021/10/userdata-enforcement-in-google-ads-api.html
 * @type {number}
 */
const MAX_IDENTIFIERS_PER_USER = 20;

/**
 * Configuration for uploading click conversions for Google Ads, includes:
 * gclid, conversion_action, conversion_date_time, conversion_value,
 * currency_code, order_id, external_attribution_data, etc.
 * @see PICKED_PROPERTIES
 *
 * Other properties that will be used to build the conversions but not picked by
 * the value directly including:
 * 1. 'user_identifier_source', source of the user identifier. If there is user
 * identifiers information in the conversion, this property should be set as
 * 'FIRST_PARTY'.
 * @see IDENTIFIERS
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserIdentifier?hl=en
 * 2. 'custom_variable_tags', the tags of conversion custom variables. To upload
 * custom variables, 'conversion_custom_variable_id' is required rather than the
 * 'tag'. So the invoker is expected to use the function
 * 'getConversionCustomVariableId' to get the ids and pass in as a
 * map(customVariables) of <tag, id> pairs before uploading conversions.
 *
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/ClickConversion
 * @typedef {{
 *   external_attribution_data: (GoogleAdsApi.ExternalAttributionData|undefined),
 *   cart_data: (object|undefined),
 *   gclid: string,
 *   conversion_action: string,
 *   conversion_date_time: string,
 *   conversion_value: number,
 *   currency_code:(string|undefined),
 *   order_id: (string|undefined),
 *   user_identifier_source:(UserIdentifierSource|undefined),
 *   custom_variable_tags:(!Array<string>|undefined),
 *   customVariables:(!Object<string,string>|undefined),
 * }}
 */
let ClickConversionConfig;

/**
 * Configuration for uploading customer match to Google Ads, includes:
 * customer_id, login_customer_id, list_id and operation.
 * operation must be one of the two: 'create' or 'remove';
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserDataOperation
 * @typedef {{
 *   customer_id: string,
 *   login_customer_id: string,
 *   list_id: string,
 *   operation: 'create'|'remove',
 * }}
 */
let CustomerMatchConfig;

/**
 * Configuration for uploading customer match data for Google Ads.
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserIdentifier
 * @typedef {{
 *   hashed_email: (string|Array<string>|undefined),
 *   hashed_phone_number: (string|Array<string>|undefined),
 *   mobile_id: (string|Array<string>|undefined),
 *   third_party_user_id: (string|Array<string>|undefined),
 *   address_info: (GoogleAdsApi.OfflineUserAddressInfo|undefined),
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
 * Google Ads API class based on Opteo's Nodejs library.
 * see https://opteo.com/dev/google-ads-api/#features
 */
class GoogleAds {
  /**
   * Note: Rate limits is set by the access level of Developer token.
   * @param {string} developerToken Developer token to access the API.
   * @param {boolean=} debugMode This is used to set ONLY validate conversions
   *     but not real uploading.
   * @param {!Object<string,string>=} env The environment object to hold env
   *     variables.
   */
  constructor(developerToken, debugMode = false, env = process.env) {
    this.debugMode = debugMode;
    const oauthClient = new AuthClient(API_SCOPES, env).getOAuth2Token();
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
     * @return {!BatchResult}
     */
    return async (lines, batchId) => {
      /** @type {!Array<ClickConversionConfig>} */
      const conversions = lines.map(
          (line) => buildClickConversionFromLine(line, adsConfig, customerId));
      /** @const {BatchResult} */
      const batchResult = {
        result: true,
        numberOfLines: lines.length,
      };
      try {
        const response = await this.uploadClickConversions(conversions,
            customerId, loginCustomerId);
        const {results, partial_failure_error: failed} = response;
        if (this.logger.isDebugEnabled()) {
          const gclids = results.map((conversion) => conversion.gclid);
          this.logger.debug('Uploaded gclids:', gclids);
        }
        if (failed) {
          this.logger.info('partial_failure_error:', failed.message);
          const failures = failed.details.map(
              ({value}) => GoogleAdsFailure.decode(value));
          this.extraFailedLines_(batchResult, failures, lines, 0);
        }
        return batchResult;
      } catch (error) {
        this.logger.error(
            `Error in upload conversions batch: ${batchId}`, error);
        this.updateBatchResultWithError_(batchResult, error, lines, 0);
        return batchResult;
      }
    }
  }

  /**
   * Updates the BatchResult based on errors.
   *
   * There are 2 types of errors here:
   * 1. Normal JavaScript Error object. It happens when the whole process fails
   * (not partial failure), so there is no detailed failed lines.
   * 2. GoogleAdsFailure. It is a Google Ads' own error object which has an
   * array of GoogleAdsError (property name 'errors'). GoogleAdsError contains
   * the detailed failed data if it is a line-error. For example, a wrong
   * encoded user identifier is a line-error, while a wrong user list id is not.
   * GoogleAdsFailure: https://developers.google.com/google-ads/api/reference/rpc/latest/GoogleAdsFailure
   * GoogleAdsError: https://developers.google.com/google-ads/api/reference/rpc/latest/GoogleAdsError
   *
   * For Customer Match data uploading, there is not partial failure, so the
   * result can be either succeeded or a thrown error. The thrown error will be
   * used to build the returned result here.
   * For Conversions uploading (partial failure enabled), if there is an error
   * fails the whole process, the error will also be thrown and handled here.
   * Otherwise, the errors will be wrapped in the response as the property named
   * 'partial_failure_error' which contains an array of GoogleAdsFailure. This
   * kind of failure doesn't fail the process, while line-errors can be
   * extracted from it.
   * For more information, see the function `extraFailedLines_`.
   *
   * An example of 'GoogleAdsFailure' is:
   * GoogleAdsFailure {
   *   errors: [
   *     GoogleAdsError {
   *       error_code: ErrorCode { offline_user_data_job_error: 25 },
   *       message: 'The SHA256 encoded value is malformed.',
   *       location: ErrorLocation {
   *         field_path_elements: [
   *           FieldPathElement { field_name: 'operations', index: 0 },
   *           FieldPathElement { field_name: 'create' },
   *           FieldPathElement { field_name: 'user_identifiers', index: 0 },
   *           FieldPathElement { field_name: 'hashed_email' }
   *         ]
   *       }
   *     }
   *   ],
   *   request_id: 'xxxxxxxxxxxxxxx'
   * }
   *
   * @param {!BatchResult} batchResult
   * @param {(!GoogleAdsFailure|!Error)} error
   * @param {!Array<string>} lines The original input data.
   * @param {number} fieldPathIndex The index of 'FieldPathElement' in the array
   *     'field_path_elements'. This is used to get the original line related to
   *     this GoogleAdsError.
   * @private
   */
  updateBatchResultWithError_(batchResult, error, lines, fieldPathIndex) {
    batchResult.result = false;
    if (error.errors) { //GoogleAdsFailure
      this.extraFailedLines_(batchResult, [error], lines, fieldPathIndex);
    } else {
      batchResult.errors = [error.message || error.toString()];
    }
  }

  /**
   * Extras failed lines based on the GoogleAdsFailures.
   *
   * Different errors have different 'fieldPathIndex' which is the index of
   * failed lines in original input data (an array of a string).
   *
   * For conversions, the ErrorLocation is like:
   * ErrorLocation {
   *   field_path_elements: [
   *     FieldPathElement { field_name: 'operations', index: 0 },
   *     FieldPathElement { field_name: 'create' }
   *   ]
   * }
   * So the index is 0, index of 'operations'.
   *
   * For customer match upload, the ErrorLocation is like:
   * ErrorLocation {
   *   field_path_elements: [
   *     FieldPathElement { field_name: 'operations', index: 0 },
   *     FieldPathElement { field_name: 'create' },
   *     FieldPathElement { field_name: 'user_identifiers', index: 0 },
   *     FieldPathElement { field_name: 'hashed_email' }
   *   ]
   * }
   * The index should be 2, index of 'user_identifiers'.
   *
   * With this we can get errors and failed lines. The function will set
   * following for the given BatchResult object:
   *   result - false
   *   errors - de-duplicated error reasons
   *   failedLines - failed lines, an array of string. Without the reason of
   *     failure.
   *   groupedFailed - a hashmap of failed the lines. The key is the reason, the
   *     value is the array of failed lines due to this reason.
   * @param {!BatchResult} batchResult
   * @param {!Array<!GoogleAdsFailure>} failures
   * @param {!Array<string>} lines The original input data.
   * @param {number} fieldPathIndex The index of 'FieldPathElement' in the array
   *     'field_path_elements'. This is used to get the original line related to
   *     this GoogleAdsError.
   * @private
   */
  extraFailedLines_(batchResult, failures, lines, fieldPathIndex) {
    batchResult.result = false;
    batchResult.failedLines = [];
    batchResult.groupedFailed = {};
    const errors = new Set();
    failures.forEach((failure) => {
      failure.errors.forEach(({message, location}) => {
        errors.add(message);
        if (location && location.field_path_elements[fieldPathIndex]) {
          const {index} = location.field_path_elements[fieldPathIndex];
          if (typeof index === 'undefined') {
            this.logger.warn(`Unknown field path index: ${fieldPathIndex}`,
                location.field_path_elements);
          } else {
            const groupedFailed = batchResult.groupedFailed[message] || [];
            const failedLine = lines[index];
            batchResult.failedLines.push(failedLine);
            groupedFailed.push(failedLine);
            if (groupedFailed.length === 1) {
              batchResult.groupedFailed[message] = groupedFailed;
            }
          }
        }
      });
    });
    batchResult.errors = Array.from(errors);
  }

  /**
   * Uploads click conversions to google ads account.
   * It requires an array of click conversions and customer id.
   * In DEBUG mode, this function will only validate the conversions.
   * @param {Array<ClickConversionConfig>} clickConversions ClickConversions
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @return {!Promise<!UploadClickConversionsResponse>}
   */
  uploadClickConversions(clickConversions, customerId, loginCustomerId) {
    this.logger.debug('Upload click conversions for customerId:', customerId);
    const customer = this.getGoogleAdsApiCustomer_(loginCustomerId, customerId);
    const request = new UploadClickConversionsRequest({
      conversions: clickConversions,
      customer_id: customerId,
      validate_only: this.debugMode, // when true makes no changes
      partial_failure: true, // Will still create the non-failed entities
    });
    return customer.conversionUploads.uploadClickConversions(request);
  }

  /**
   * Returns the id of Conversion Custom Variable with the given tag.
   * @param {string} tag Custom Variable tag.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @return {Promise<number|undefined>} Returns undefined if can't find tag.
   */
  async getConversionCustomVariableId(tag, customerId, loginCustomerId) {
    const customer = this.getGoogleAdsApiCustomer_(loginCustomerId, customerId);
    const customVariables = await customer.query(`
        SELECT conversion_custom_variable.id,
               conversion_custom_variable.tag
        FROM conversion_custom_variable
        WHERE conversion_custom_variable.tag = "${tag}" LIMIT 1
    `);
    if (customVariables.length > 0) {
      return customVariables[0].conversion_custom_variable.id;
    }
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
     * @return {!Promise<BatchResult>}
     */
    return async (lines, batchId) => {
      /** @type {Array<CustomerMatchRecord>} */
      const userIds = lines.map((line) => JSON.parse(line));
      /** @const {BatchResult} */ const batchResult = {
        result: true,
        numberOfLines: lines.length,
      };
      try {
        const response = await this.uploadUserDataToUserList(userIds,
            customerMatchConfig);
        this.logger.debug(`Customer Match upload batch[${batchId}]`, response);
        return batchResult;
      } catch (error) {
        this.logger.error(
            `Error in Customer Match upload batch[${batchId}]`, error);
        this.updateBatchResultWithError_(batchResult, error, lines, 2);
        return batchResult;
      }
    }
  }

  /**
   * Uploads a user data to a user list (aka customer match).
   * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserDataService
   * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserDataOperation
   * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserData
   * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserIdentifier
   * @see https://developers.google.com/google-ads/api/reference/rpc/latest/CustomerMatchUserListMetadata
   * Please note: The UserDataService has a limit of 10 UserDataOperations
   * and 100 user IDs per request
   * @see https://developers.google.com/google-ads/api/docs/migration/user-data-service#rate_limits
   * @param {!Array<CustomerMatchRecord>} customerMatchRecords user Ids
   * @param {CustomerMatchConfig} customerMatchConfig Customer Match config containing
   * customer_id, login_customer_id, list_id, list_type which can be one of the following
   * hashed_email, hashed_phone_number, mobile_id, third_party_user_id or address_info and
   * operation which can be either 'create' or 'remove'
   * @return {!Promise<UploadUserDataResponse>}
   */
  async uploadUserDataToUserList(customerMatchRecords, customerMatchConfig) {
    const customerId = customerMatchConfig.customer_id.replace(/-/g, '');
    const loginCustomerId = customerMatchConfig.login_customer_id.replace(/-/g,
        '');
    const userListId = customerMatchConfig.list_id;
    const operation = customerMatchConfig.operation;

    const customer = this.getGoogleAdsApiCustomer_(loginCustomerId, customerId);
    const operationsList = this.buildOperationsList_(operation,
        customerMatchRecords);
    const metadata = this.buildCustomerMatchUserListMetadata_(customerId,
        userListId);
    const request = UploadUserDataRequest.create({
      customer_id: customerId,
      operations: operationsList,
      customer_match_user_list_metadata: metadata,
    });
    const response = await customer.userData.uploadUserData(request);
    return response;
  }

  /**
   * Builds a list of UserDataOperations.
   * Since v6 you can set a user_attribute
   * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserData
   * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserIdentifier
   * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserDataOperation
   * @param {string} operationType either 'create' or 'remove'
   * @param {Array<CustomerMatchRecord>} customerMatchRecords userIds
   * @return {Array<UserDataOperation>}
   * @private
   */
  buildOperationsList_(operationType, customerMatchRecords) {
    return customerMatchRecords.map((customerMatchRecord) => {
      const userIdentifiers = [];
      IDENTIFIERS.forEach((idType) => {
        const idValue = customerMatchRecord[idType];
        if (idValue) {
          if (Array.isArray(idValue)) {
            idValue.forEach((user) => {
              userIdentifiers.push(UserIdentifier.create({[idType]: user}));
            });
          } else {
            userIdentifiers.push(UserIdentifier.create({[idType]: idValue}));
          }
        }
      });
      let userData;
      if (userIdentifiers.length <= MAX_IDENTIFIERS_PER_USER) {
        userData = UserData.create({user_identifiers: userIdentifiers});
      } else {
        this.logger.warn(
            `Too many user identifiers, will only send ${MAX_IDENTIFIERS_PER_USER}:`,
            JSON.stringify(customerMatchRecord));
        userData = UserData.create({user_identifiers: userIdentifiers}.slice(0,
            MAX_IDENTIFIERS_PER_USER));
      }
      return UserDataOperation.create({[operationType]: userData});
    });
  }

  /**
   * Creates CustomerMatchUserListMetadata.
   * @see https://developers.google.com/google-ads/api/reference/rpc/latest/CustomerMatchUserListMetadata
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

/**
 * Returns a conversion object based the given config and line data.
 * @param {string} line A JSON string of a conversion data.
 * @param {ClickConversionConfig} config Default click conversion params
 * @param {string} customerId
 * @return {object} A conversion
 */
const buildClickConversionFromLine = (line, config, customerId) => {
  const {customVariables, user_identifier_source} = config;
  const record = JSON.parse(line);
  const conversion = lodash.merge(lodash.pick(config, PICKED_PROPERTIES),
      lodash.pick(record, PICKED_PROPERTIES));
  if (customVariables) {
    const tags = Object.keys(customVariables);
    conversion.custom_variables = tags.map((tag) => {
      return {
        conversion_custom_variable:
            `customers/${customerId}/conversionCustomVariables/${customVariables[tag]}`,
        value: record[tag],
      };
    });
  }
  const user_identifiers = [];
  IDENTIFIERS.forEach((identifier) => {
    if (record[identifier]) {
      user_identifiers.push({
        user_identifier_source,
        [identifier]: record[identifier],
      });
    }
  });
  if (user_identifiers.length > 0) {
    conversion.user_identifiers = user_identifiers;
  }
  return conversion;
}

module.exports = {
  ClickConversionConfig,
  CustomerMatchRecord,
  CustomerMatchConfig,
  GoogleAds,
  ReportQueryConfig,
  GoogleAdsField,
  buildClickConversionFromLine,
};
