// Copyright 2024 Google Inc.
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
 * @fileoverview Google Ads API Wrapper based on the NodeJS library complied
 * from the Bazel generated files.
 * @see https://github.com/googleapis/googleapis
 * @see https://github.com/lushu/googleads-nodejs
 */
'use strict';

const { Transform } = require('stream');
const lodash = require('lodash');
const { request: gaxiosRequest } = require('gaxios');
const {
  ConversionAdjustmentUploadServiceClient,
  ConversionUploadServiceClient,
  GoogleAdsServiceClient,
  GoogleAdsFieldServiceClient,
  OfflineUserDataJobServiceClient,
  UserDataServiceClient,
  UserListServiceClient,
  protos: { google: { ads: { googleads } } },
} = require('google-ads-nodejs-client');

const API_VERSION = Object.keys(googleads)[0];
const {
  common: {
    Consent,
    CustomerMatchUserListMetadata,
    StoreSalesMetadata,
    TransactionAttribute,
    UserAttribute,
    UserData,
    UserIdentifier,
  },
  resources: {
    GoogleAdsField,
    OfflineUserDataJob,
    UserList,
  },
  services: {
    AddOfflineUserDataJobOperationsRequest,
    AddOfflineUserDataJobOperationsResponse,
    CallConversion,
    ClickConversion,
    ConversionAdjustment,
    CreateOfflineUserDataJobRequest,
    CustomVariable,
    GoogleAdsRow,
    MutateUserListsRequest,
    MutateUserListsResponse,
    OfflineUserDataJobOperation,
    RunOfflineUserDataJobRequest,
    SearchGoogleAdsFieldsRequest,
    SearchGoogleAdsRequest,
    SearchGoogleAdsResponse,
    UploadCallConversionsRequest,
    UploadCallConversionsResponse,
    UploadClickConversionsRequest,
    UploadClickConversionsResponse,
    UploadConversionAdjustmentsRequest,
    UploadConversionAdjustmentsResponse,
    UploadUserDataRequest,
    UploadUserDataResponse,
    UserDataOperation,
  },
  errors: {
    GoogleAdsError,
    GoogleAdsFailure,
  },
  enums: {
    ConsentStatusEnum: { ConsentStatus },
    OfflineUserDataJobFailureReasonEnum: { OfflineUserDataJobFailureReason },
    OfflineUserDataJobTypeEnum: { OfflineUserDataJobType },
    OfflineUserDataJobStatusEnum: { OfflineUserDataJobStatus },
    UserIdentifierSourceEnum: { UserIdentifierSource },
    UserListMembershipStatusEnum: { UserListMembershipStatus },
    UserListTypeEnum: { UserListType },
    CustomerMatchUploadKeyTypeEnum: { CustomerMatchUploadKeyType },
  },
} = googleads[API_VERSION];

const AuthClient = require('./auth_client.js');
const {
  getLogger,
  BatchResult,
  extractObject,
  changeNamingFromSnakeToLowerCamel,
  changeObjectNamingFromSnakeToLowerCamel,
  changeObjectNamingFromLowerCamelToSnake,
} = require('../components/utils.js');
const { getCleanCid, RestSearchStreamTransform }
  = require('./base/ads_api_common.js');

/** @type {!ReadonlyArray<string>} */
const API_SCOPES = Object.freeze(['https://www.googleapis.com/auth/adwords']);
const API_ENDPOINT = 'https://googleads.googleapis.com';
/**
 * List of properties that will be taken from the data file as elements of a
 * conversion or a conversion adjustment.
 * @const {string: Array<string>}
 */
const CONVERSION_FIELDS = {};
/**
 * @see CallConversion
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/CallConversion
 */
CONVERSION_FIELDS.CALL = [
  'callerId',
  'consent',
  'callStartDateTime',
  'conversionAction',
  'conversionDateTime',
  'conversionValue',
  'currencyCode',
];
/**
 * @see ClickConversion
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/ClickConversion
 */
CONVERSION_FIELDS.CLICK = [
  'gbraid',
  'wbraid',
  'gclid',
  'externalAttributionData',
  'conversionEnvironment',
  'consent',
  'gclid',
  'conversionAction',
  'conversionDateTime',
  'conversionValue',
  'currencyCode',
  'orderId',
];
/**
 * @see ConversionAdjustment
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/ConversionAdjustment
 */
CONVERSION_FIELDS.ADJUSTMENT = [
  'gclidDateTimePair',
  'adjustmentType',
  'restatementValue',
  'orderId',
  'conversionAction',
  'adjustmentDateTime',
  'userAgent',
];

/**
 * Additional attributes in user data for store sales data or customer match.
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserData
 * @type {Array<string>}
 */
const USERDATA_ADDITIONAL_ATTRIBUTES = [
  'transactionAttribute', // Attribute of the store sales transaction.
  'userAttribute', // Only be used with CUSTOMER_MATCH_WITH_ATTRIBUTES job type.
  'consent',
];

/**
 * Kinds of UserIdentifier for different services.
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserIdentifier
 * @see UserIdentifier
 * @type {Array<string>}
 */
const IDENTIFIERS = {};
IDENTIFIERS.CUSTOMER_MATCH = [
  'hashedEmail',
  'hashedPhoneNumber',
  'mobileId',
  'thirdPartyUserId',
  'addressInfo',
];
IDENTIFIERS.STORE_SALES = [
  'hashedEmail',
  'hashedPhoneNumber',
  'thirdPartyUserId',
  'addressInfo',
];
IDENTIFIERS.CLICK_CONVERSION = [
  'hashedEmail',
  'hashedPhoneNumber',
];
IDENTIFIERS.CONVERSION_ADJUSTMENT = [
  'hashedEmail',
  'hashedPhoneNumber',
  'addressInfo',
];

/**
 * Maximum number of user identifiers in single UserData.
 * @see https://ads-developers.googleblog.com/2021/10/userdata-enforcement-in-google-ads-api.html
 * @see https://developers.google.com/google-ads/api/reference/rpc/v15/ClickConversion
 * @type {number}
 */
const MAX_IDENTIFIERS = {
  USER_DATA: 20,
  CONVERSION: 5,
};

/**
 * Configuration for uploading click conversions, call converions or conversion
 * adjustments for Google Ads, includes:
 * gclid, conversionAction, conversionDateTime, conversionValue,
 * currencyCode, orderId, externalAttributionData,
 * callerId, callStartDateTime,
 * adjustmentType, adjustmentDateTime, userAgent, gclidDateTimePair, etc.
 * @see CONVERSION_FIELDS
 *
 * Other properties that will be used to build the conversions but not picked by
 * the value directly including:
 * 1. 'userIdentifierSource', source of the user identifier. If there is user
 * identifiers information in the conversion, this property should be set as
 * 'FIRST_PARTY'.
 * @see IDENTIFIERS
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserIdentifier?hl=en
 * 2. 'customVariableTags', the tags of conversion custom variables. To upload
 * custom variables, 'conversionCustomVariableId' is required rather than the
 * 'tag'. So the invoker is expected to use the function
 * 'getConversionCustomVariableId' to get the ids and pass in as a
 * map(customVariables) of <tag, id> pairs before uploading conversions.
 *
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/ClickConversion
 * @typedef {{
 *   externalAttributionData: (GoogleAdsApi.ExternalAttributionData|undefined),
 *   cartData: (object|undefined),
 *   gclid: (string|undefined),
 *   callerId: (string|undefined),
 *   callStartDateTime: (string|undefined),
 *   conversionAction: string,
 *   conversionDateTime: string,
 *   conversionValue: number,
 *   currencyCode:(string|undefined),
 *   orderId: (string|undefined),
 *   adjustmentType: (string|undefined),
 *   adjustmentDateTime: (!ConversionAdjustmentType|undefined),
 *   userAgent: (string|undefined),
 *   userIdentifierSource:(!UserIdentifierSource|undefined),
 *   customVariableTags:(!Array<string>|undefined),
 *   customVariables:(!Object<string,string>|undefined),
 *   consent: (!Consent),
 * }}
 */
let ConversionConfig;

/**
 * Configuration for uploading customer match to Google Ads, includes:
 * customerId, loginCustomerId, listId and operation.
 * If audience listId is not present, 'listName' and 'uploadKeyType' need to
 * be there so they can be used to create a customer match user list.
 * operation must be one of 'create' or 'remove'.
 * `customerMatchUserListMetadata` offers a request level metadata, including
 * 'consent'. While the top level 'consent', if presents, it will serve as the
 * fallback value for each UserIdentifier.
 * Should not include `userIdentifierSource` based on:
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserIdentifier
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserDataOperation
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/CustomerMatchUploadKeyTypeEnum.CustomerMatchUploadKeyType
 * @typedef {{
 *   customerId: (string|number),
 *   loginCustomerId: (string|number),
 *   listId: (string|undefined),
 *   listName: (string|undefined),
 *   uploadKeyType: ('CONTACT_INFO'|'CRM_ID'|'MOBILE_ADVERTISING_ID'|undefined),
 *   customerMatchUserListMetadata: (undefined|CustomerMatchUserListMetadata),
 *   operation: ('create'|'remove'),
 *   consent: (!Consent),
 * }}
 */
let CustomerMatchConfig;

/**
 * Configuration for offline user data job, includes:
 * customerId, loginCustomerId, listId, operation and type.
 * 'operation' should be one of the two: 'create' or 'remove',
 * 'type' is OfflineUserDataJobType, it can be 'CUSTOMER_MATCH_USER_LIST',
 * 'CUSTOMER_MATCH_WITH_ATTRIBUTES' or 'STORE_SALES_UPLOAD_FIRST_PARTY'.
 * For job type 'CUSTOMER_MATCH_USER_LIST', if `listId` is not present,
 * 'listName' and 'uploadKeyType' need to be there so they can be used to
 *  create a customer match user list.
 * For job type 'CUSTOMER_MATCH_WITH_ATTRIBUTES', 'user_attribute' can be used
 * to store shared additional user attributes.
 * For job type 'CUSTOMER_MATCH_*', `customerMatchUserListMetadata` offers a job
 * level metadata, including 'consent'. While the top level 'consent',
 * if presents, it will serve as the fallback value for each UserIdentifier.
 * For job type 'STORE_SALES_UPLOAD_FIRST_PARTY', `storeSalesMetadata` is
 * required to offer StoreSalesMetadata. Besides that, for the store sales data,
 * common data (e.g. `currencyCode`, `conversionAction`) in
 * `transactionAttribute` can be put here as well.
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/OfflineUserDataJob
 * @typedef {{
 *   customerId: (string|number),
 *   loginCustomerId: (string|number),
 *   listId: (string|undefined),
 *   listName: (string|undefined),
 *   uploadKeyType: ('CONTACT_INFO'|'CRM_ID'|'MOBILE_ADVERTISING_ID'|undefined),
 *   operation: ('create'|'remove'),
 *   type: !OfflineUserDataJobType,
 *   storeSalesMetadata: (undefined|StoreSalesMetadata),
 *   customerMatchUserListMetadata: (undefined|CustomerMatchUserListMetadata),
 *   transactionAttribute: (undefined|TransactionAttribute),
 *   userAttribute: (undefined|UserAttribute),
 *   userIdentifierSource: (!UserIdentifierSource|undefined),
 *   consent: (!Consent),
 * }}
 */
let OfflineUserDataJobConfig;

/**
 * Configuration for uploading customer match data for Google Ads.
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserIdentifier
 * @typedef {{
 *   hashedEmail: (string|Array<string>|undefined),
 *   hashedPhoneNumber: (string|Array<string>|undefined),
 *   mobileId: (string|Array<string>|undefined),
 *   thirdPartyUserId: (string|Array<string>|undefined),
 *   addressInfo: (GoogleAdsApi.OfflineUserAddressInfo|undefined),
 *   userIdentifierSource: (!UserIdentifierSource|undefined),
 * }}
 */
let CustomerMatchRecord;

/**
 * Google Ads API class based on Google Ads API library.
 */
class GoogleAdsApi {
  /**
   * Note: Rate limits is set by the access level of Developer token.
   * @param {string} developerToken Developer token to access the API.
   * @param {boolean=} debugMode This is used to set ONLY validate conversions
   *     but not real uploading.
   * @param {!Object<string,string>=} env The environment object to hold env
   *     variables.
   */
  constructor(developerToken, debugMode = false, env = process.env) {
    this.developerToken = developerToken;
    this.debugMode = debugMode;
    this.authClient = new AuthClient(API_SCOPES, env);
    this.logger = getLogger('API.ADS.N');
    this.logger.info(
      `Init ${this.constructor.name} with Debug Mode?`, this.debugMode);
  }

  /**
   * Gets a Google Ads report based on a Google Ads Query Language(GAQL) query
   * with pagination automatically handled. It returns an array of
   * `GoogleAdsRow` without other fields in the object
   * `SearchGoogleAdsResponse`, e.g.`fieldMask`.
   * The `GoogleAdsRow` objects have `null` value for those unselected fields.
   * The enum fields in the query are present as index number.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {string} query A Google Ads Query Language query.
   * @return {!Array<!GoogleAdsRow>}
   */
  async getReport(customerId, loginCustomerId, query) {
    const request = new SearchGoogleAdsRequest({
      query,
      customerId: getCleanCid(customerId),
    });
    return this.getReport_(request, loginCustomerId, true);
  }

  /**
   * Gets a page of Google Ads report based on a GAQL query.
   * It returns a `SearchGoogleAdsResponse` object with the property `results`
   * which contains an array of `GoogleAdsRow`. The returned object also
   * contains `nextPageToken`.
   * Note, the `GoogleAdsRow` objects have `null` value for those unselected
   * fields. The `fieldMask` in the response CAN be used to clear the result
   * objects.
   * The enum fields in the query are present as index number.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {string} query A Google Ads Query Language query.
   * @param {object=} options Options for `SearchGoogleAdsRequest`. It can
   *     contain `pageSize` whose default value is 10000 and `pageToken` if it
   *     is going to get the next page.
   * @return {!SearchGoogleAdsResponse}
   */
  async getPaginatedReport(customerId, loginCustomerId, query, options = {}) {
    const request = new SearchGoogleAdsRequest(
      Object.assign({
        query,
        customerId: getCleanCid(customerId),
        pageSize: 10000,
      }, options)
    );
    return this.getReport_(request, loginCustomerId);
  }

  /**
   * Gets Google Ads report based on the parameters. The Google Ads API returns
   * an array with three element:
   * 1. Array<GoogleAdsRow>
   * 2. SearchGoogleAdsRequest | null,
   * 3. SearchGoogleAdsResponse
   * When the `autoPaginate` is set to be true, only the first element is
   * available, so the returned value is an array of GoogleAdsRow, otherwise
   * the returned value is a `SearchGoogleAdsResponse` object.
   * @param {!SearchGoogleAdsRequest} request
   * @param {string} loginCustomerId
   * @param {boolean=} autoPaginate
   * @return {!Array<(!GoogleAdsRow>|!SearchGoogleAdsResponse)}
   * @private
   */
  async getReport_(request, loginCustomerId, autoPaginate = false) {
    const client = await this.getGoogleAdsServiceClient_();
    const callOptions = this.getCallOptions_(loginCustomerId);
    callOptions.autoPaginate = autoPaginate;
    const response = await client.search(request, callOptions);
    const result = response[autoPaginate ? 0 : 2];
    return result;
  }

  /**
   * Gets stream report of a given Customer account. The stream will send
   * `SearchGoogleAdsResponse` objects with the event 'data'.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {string} query
   * @return {!StreamProxy}
   */
  async streamReport(customerId, loginCustomerId, query) {
    const client = await this.getGoogleAdsServiceClient_();
    const request = new SearchGoogleAdsRequest({
      query,
      customerId: getCleanCid(customerId),
    });
    const callOptions = this.getCallOptions_(loginCustomerId);
    const response = await client.searchStream(request, callOptions);
    return response;
  }

  /**
   * Gets the report with only selected fields.
   * Based on the `fieldMask` in the `SearchGoogleAdsResponse` to filter out
   * selected fields of the report and returns the `GoogleAdsRows` in the
   * `results` in JSON format strings with the delimit of a line breaker.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {string} query A Google Ads Query string.
   * @param {boolean} snakeCase Output JSON objects in snake_case.
   * @return {!Promise<stream>}
   */
  async cleanedStreamReport(customerId, loginCustomerId, query,
    snakeCase = false) {
    const cleanReportStream = new Transform({
      writableObjectMode: true,
      transform(chunk, encoding, callback) {
        const { fieldMask: { paths } } = chunk;
        const camelPaths = paths.map((path) => {
          return path.split('.').map(changeNamingFromSnakeToLowerCamel).join('.');
        });
        const extractor = extractObject(camelPaths);
        const results = snakeCase
          ? chunk.results.map(extractor).map(changeObjectNamingFromLowerCamelToSnake)
          : chunk.results.map(extractor);
        // Add a line break after each chunk to keep files in proper format.
        const data = results.map(JSON.stringify).join('\n') + '\n';
        callback(null, data);
      }
    });
    const stream = await this.streamReport(customerId, loginCustomerId, query);
    return stream.on('error', (error) => cleanReportStream.emit('error', error))
      .pipe(cleanReportStream);
  }

  /**
   * Gets the report stream through REST interface.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {string} query A Google Ads Query string.
   * @return {!Promise<stream>}
   */
  async restStreamReport(customerId, loginCustomerId, query) {
    await this.authClient.prepareCredentials();
    const headers = Object.assign(
      await this.authClient.getDefaultAuth().getRequestHeaders(),
      this.getGoogleAdsHeaders_(loginCustomerId)
    );
    const options = {
      baseURL: `${API_ENDPOINT}/${API_VERSION}/`,
      url: `customers/${getCleanCid(customerId)}/googleAds:searchStream`,
      headers,
      data: { query },
      method: 'POST',
      responseType: 'stream',
    };
    const response = await gaxiosRequest(options);
    return response.data;
  }

  /**
   * Gets the report stream through REST interface.
   * Based on the `fieldMask` in the response to filter out
   * selected fields of the report and returns an array of JSON format strings
   * with the delimit of a line breaker.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {string} query A Google Ads Query string.
   * @param {boolean} snakeCase Output JSON objects in snake_case.
   * @return {!Promise<stream>}
   */
  async cleanedRestStreamReport(customerId, loginCustomerId, query,
    snakeCase = false) {
    const transform = new RestSearchStreamTransform(snakeCase);
    const stream =
      await this.restStreamReport(customerId, loginCustomerId, query);
    return stream.on('error', (error) => transform.emit('error', error))
      .pipe(transform);
  }

  /**
   * Returns resources information from Google Ads API. see:
   * https://developers.google.com/google-ads/api/docs/concepts/field-service
   * Note, it looks like this function doesn't check the CID, just using
   * developer token and OAuth.
   * @param {Array<string>} adFields Array of Ad fields.
   * @param {Array<string>} metadata Select fields, default values are:
   *     name, data_type, is_repeated, type_url.
   * @return {!Promise<!Array<GoogleAdsField>>}
   */
  async searchReportField(adFields,
    metadata = ['name', 'data_type', 'is_repeated', 'type_url',]) {
    const client = await this.getGoogleAdsFieldServiceClient_();
    const selectClause = metadata.join(',');
    const fields = adFields.join('","');
    const query = `SELECT ${selectClause} WHERE name IN ("${fields}")`;
    const request = new SearchGoogleAdsFieldsRequest({ query });
    const callOptions = this.getCallOptions_();
    const [results] = await client.searchGoogleAdsFields(request, callOptions);
    return results;
  }

  /**
   * Returns the function to send out a request to Google Ads API with a batch
   * of call conversions.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {!ConversionConfig} adsConfig Default call conversion params
   * @return {!SendSingleBatch} Function which can send a batch of hits to
   *     Google Ads API.
   */
  getUploadCallConversionFn(customerId, loginCustomerId, adsConfig) {
    return this.getUploadConversionFnBase_(customerId, loginCustomerId,
      adsConfig, 'uploadCallConversions', 'callerId');
  }

  /**
   * Returns the function to send out a request to Google Ads API with a batch
   * of click conversions.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {!ConversionConfig} adsConfig Default click conversion params
   * @return {!SendSingleBatch} Function which can send a batch of hits to
   *     Google Ads API.
   */
  getUploadClickConversionFn(customerId, loginCustomerId, adsConfig) {
    return this.getUploadConversionFnBase_(customerId, loginCustomerId,
      adsConfig, 'uploadClickConversions', 'gclid');
  }

  /**
   * Returns the function to send out a request to Google Ads API with a batch
   * of conversion adjustments.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {!ConversionConfig} adsConfig Default conversion adjustments
   *     params.
   * @return {!SendSingleBatch} Function which can send a batch of hits to
   *     Google Ads API.
   */
  getUploadConversionAdjustmentFn(customerId, loginCustomerId, adsConfig) {
    return this.getUploadConversionFnBase_(customerId, loginCustomerId,
      adsConfig, 'uploadConversionAdjustments', 'orderId');
  }

  /**
   * Returns the function to send call conversions, click conversions or
   * conversion adjustment (enhanced conversions).
   *
   * Google Ads API conversion related services (ConversionUploadService,
   * ConversionAdjustmentUploadService) support `partial_failure` which will
   * return `partialFailureError`(Status) when 'some kinds of errors' happen,
   * e.g. wrong conversion Id, wrong gclid, etc.
   * `Status` has a property `message` that has the general information of the
   * error, however, the detailed information (e.g. the failed conversions and
   * their reasons) lies in the property named `datails` (an array of
   * `GoogleAdsFailure`). Each `GoogleAdsFailure` is related to a failed
   * conversion. The function `extraFailedLines_` is used to extract the
   * details.
   *
   * There is also another kind of error that will be throw directly. Sometimes,
   * this kind of error is a wrapped `GoogleAdsFailure`, e.g. wrong CID.
   * The function `updateBatchResultWithError` for this kind of error.
   * @see extraFailedLines_
   * @see updateBatchResultWithError
   * @see https://developers.google.com/google-ads/api/reference/rpc/google.rpc#google.rpc.Status
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {!ConversionConfig} conversionConfig Default conversion parameters.
   * @param {string} functionName The name of sending converions function, could
   *   be `uploadClickConversions`, `uploadCallConversions` or
   *   `uploadConversionAdjustments`.
   * @param {string} propertyForDebug The name of property for debug info.
   * @return {!SendSingleBatch} Function which can send a batch of hits to
   *     Google Ads API.
   * @private
   */
  getUploadConversionFnBase_(customerId, loginCustomerId, conversionConfig,
    functionName, propertyForDebug) {
    /** @type {!ConversionConfig} */
    const adsConfig = this.getCamelConfig_(conversionConfig);
    adsConfig.customerId = getCleanCid(customerId);
    adsConfig.loginCustomerId = getCleanCid(loginCustomerId);
    /**
     * Sends a batch of hits to Google Ads API.
     * @param {!Array<string>} lines Data for single request. It should be
     *     guaranteed that it doesn't exceed quota limitation.
     * @param {string} batchId The tag for log.
     * @return {!BatchResult}
     */
    return async (lines, batchId) => {
      /** @const {BatchResult} */
      const batchResult = {
        result: true,
        numberOfLines: lines.length,
      };
      try {
        const response = await this[functionName](lines, adsConfig);
        const { results, partialFailureError: failed, jobId } = response;
        if (this.logger.isDebugEnabled()) {
          const id = results.map((conversion) => conversion[propertyForDebug]);
          this.logger.debug(`Uploaded ${propertyForDebug}:`, id);
        }
        if (failed) {
          this.logger.info(`Job[${jobId}] partialFailureError:`, failed.message);
          const failures = failed.details.map(
            ({ value }) => GoogleAdsFailure.decode(value));
          this.extraFailedLines_(batchResult, failures, lines, 0);
        }
        return batchResult;
      } catch (error) {
        this.logger.error(
          `Error in ${functionName} batch: ${batchId}`, error);
        this.updateBatchResultWithError(batchResult, error, lines, 0);
        return batchResult;
      }
    }
  }

  /**
   * Updates the BatchResult based on errors.
   *
   * There are 2 types of errors here:
   * 1. Normal JavaScript Error object. It happens outside of the communication
   * with Google Ads API and fails the whole process, so there is no detailed
   * failed lines.
   * 2. GoogleAdsFailure (which is wrapped in error.metadata in this library).
   * GoogleAdsFailure is a Google Ads' own error object which has an array of
   * GoogleAdsError (property name 'errors'). GoogleAdsError contains
   * the detailed failed data if it is a "partial failure". For example, a wrong
   * encoded user identifier is a "partial failure", while a wrong user list id
   * is not.
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
   *         fieldPathElements: [
   *           FieldPathElement { fieldName: 'operations', index: 0 },
   *           FieldPathElement { fieldName: 'create' },
   *           FieldPathElement { fieldName: 'user_identifiers', index: 0 },
   *           FieldPathElement { fieldName: 'hashed_email' }
   *         ]
   *       }
   *     }
   *   ],
   *   requestId: 'xxxxxxxxxxxxxxx'
   * }
   *
   * @param {!BatchResult} batchResult
   * @param {(!GoogleAdsFailure|!Error)} error
   * @param {!Array<string>} lines The original input data.
   * @param {number} fieldPathIndex The index of 'FieldPathElement' in the array
   *     'field_path_elements'. This is used to get the original line related to
   *     this GoogleAdsError.
   */
  updateBatchResultWithError(batchResult, error, lines, fieldPathIndex) {
    batchResult.result = false;
    if (error.metadata) {
      const failures = this.getGoogleAdsFailures_(error.metadata);
      if (failures.length > 0) {
        debugGoogleAdsFailure(failures, lines)
        this.extraFailedLines_(batchResult, failures, lines, fieldPathIndex);
        return;
      }
      this.logger.warn(
        'Got an error with metadata but no GoogleAdsFailure in it', error);
    }
    this.logger.warn('Got an error without metadata', error);
    batchResult.errors = [error.message || error.toString()];
  }

  /**
   * Returns the GoogleAdsFailure from a wrapped gRPC error's metadata.
   * @param {Metadata} metadata
   * @return {!Array<!GoogleAdsFailure>}
   */
  getGoogleAdsFailures_(metadata) {
    const errors = metadata.getMap();
    const googleAdsFailures = Object.keys(errors)
      .filter((key) => key.indexOf('googleadsfailure') > -1)
      .map((key) => GoogleAdsFailure.decode(errors[key]));
    return googleAdsFailures;
  }


  /**
   * Extras failed lines based on the Array of GoogleAdsFailure.
   *
   * Different errors have different 'fieldPathIndex' which is the index of
   * failed lines in original input data (an array of a string).
   *
   * For conversions, the ErrorLocation is like:
   * ErrorLocation {
   *   fieldPathElements: [
   *     FieldPathElement { fieldName: 'operations', index: 0 },
   *     FieldPathElement { fieldName: 'create' }
   *   ]
   * }
   *
   * For customer match upload, the ErrorLocation is like:
   * ErrorLocation {
   *   fieldPathElements: [
   *     FieldPathElement { fieldName: 'operations', index: 0 },
   *     FieldPathElement { fieldName: 'create' },
   *     FieldPathElement { fieldName: 'userIdentifiers', index: 0 },
   *     FieldPathElement { fieldName: 'hashedEmail' }
   *   ]
   * }
   *
   * The fieldPathElements can help to locate the details of the data cause the
   * error.
   * Currently, just leverage the first `FieldPathElement` as it is enough to
   * locate the origin line in the input data.
   * Note, this can be used to extract the precise information of the root cause
   * of the error.
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
   *     'fieldPathElements'. This is used to get the original line related to
   *     this GoogleAdsError.
   * @private
   */
  extraFailedLines_(batchResult, failures, lines, fieldPathIndex) {
    batchResult.result = false;
    batchResult.failedLines = [];
    batchResult.groupedFailed = {};
    const errors = new Set();
    failures.forEach((failure) => {
      this.logger.error(`API requestId[${failure.requestId}]`,
        failure.errors.map(({ message }) => message));
      failure.errors.forEach(({ message, location }) => {
        errors.add(message);
        if (location && location.fieldPathElements[fieldPathIndex]) {
          const { index } = location.fieldPathElements[fieldPathIndex];
          if (index === null) {
            this.logger.warn(`Unknown field path index: ${fieldPathIndex}`,
              location.fieldPathElements);
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
   * Uploads call conversions to google ads account.
   * In DEBUG mode, this function will only validate the conversions.
   * @see https://developers.google.com/google-ads/api/reference/rpc/v15/CallConversion
   * @param {!Array<string>} lines Data for single request. It should be
   *     guaranteed that it doesn't exceed quota limitation.
   * @param {!ConversionConfig} config Default conversion parameters.
   * @return {!Promise<!UploadCallConversionsResponse>}
   */
  async uploadCallConversions(lines, config) {
    const { customerId, loginCustomerId } = config;
    this.logger.debug('Upload call conversions for:', config);
    const conversions =
      buildConversionJsonList(lines, config, CONVERSION_FIELDS.CALL);
    const callConversions =
      conversions.map((conversion) => new CallConversion(conversion));
    const client = await this.getConversionUploadServiceClient_();
    const request = new UploadCallConversionsRequest({
      conversions: callConversions,
      customerId,
      validateOnly: this.debugMode, // when true makes no changes
      partialFailure: true, // Will still create the non-failed entities
    });
    const callOptions = this.getCallOptions_(loginCustomerId);
    const [response] = await client.uploadCallConversions(request, callOptions);
    return response;
  }

  /**
   * Uploads click conversions to google ads account.
   * In DEBUG mode, this function will only validate the conversions.
   * @param {!Array<string>} lines Data for single request. It should be
   *     guaranteed that it doesn't exceed quota limitation.
   * @param {!ConversionConfig} config Default conversion parameters.
   * @return {!Promise<!UploadClickConversionsResponse>}
   */
  async uploadClickConversions(lines, config) {
    const { customerId, loginCustomerId } = config;
    this.logger.debug('Upload click conversions for:', config);
    const conversions = buildConversionJsonList(lines, config,
      CONVERSION_FIELDS.CLICK, IDENTIFIERS.CLICK_CONVERSION, MAX_IDENTIFIERS.CONVERSION);
    const clickConversions =
      conversions.map((conversion) => new ClickConversion(conversion));
    const client = await this.getConversionUploadServiceClient_();
    const request = new UploadClickConversionsRequest({
      conversions: clickConversions,
      customerId,
      validateOnly: this.debugMode, // when true makes no changes
      partialFailure: true, // Will still create the non-failed entities
    });
    const callOptions = this.getCallOptions_(loginCustomerId);
    const [response] = await client.uploadClickConversions(request, callOptions);
    return response;
  }

  /**
   * Uploads conversion adjustments to google ads account.
   * In DEBUG mode, this function will only validate the conversion adjustments.
   * @param {!Array<string>} lines Data for single request. It should be
   *     guaranteed that it doesn't exceed quota limitation.
   * @param {!ConversionConfig} config Default conversion parameters.
   * @return {!Promise<!UploadConversionAdjustmentsResponse>}
   */
  async uploadConversionAdjustments(lines, config) {
    const { customerId, loginCustomerId } = config;
    this.logger.debug('Upload conversion adjustments for:', config);
    const conversions = buildConversionJsonList(
      lines, config, CONVERSION_FIELDS.ADJUSTMENT,
      IDENTIFIERS.CONVERSION_ADJUSTMENT, MAX_IDENTIFIERS.CONVERSION);
    const conversionAdjustments =
      conversions.map((conversion) => new ConversionAdjustment(conversion));
    const client = await this.getConversionAdjustmentUploadServiceClient_();
    const request = new UploadConversionAdjustmentsRequest({
      conversionAdjustments,
      customerId,
      validateOnly: this.debugMode, // when true makes no changes
      partialFailure: true, // Will still create the non-failed entities
    });
    const callOptions = this.getCallOptions_(loginCustomerId);
    const [response] =
      await client.uploadConversionAdjustments(request, callOptions);
    return response;
  }

  /**
   * Returns the id of Conversion Custom Variable with the given tag.
   * @param {string} tag Custom Variable tag.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @return {Promise<string|undefined>} Returns undefined if can't find tag.
   */
  async getConversionCustomVariableId(tag, customerId, loginCustomerId) {
    const query = `
      SELECT conversion_custom_variable.id,
             conversion_custom_variable.tag
      FROM conversion_custom_variable
      WHERE conversion_custom_variable.tag = "${tag}" LIMIT 1
  `;
    const customVariables =
      await this.getReport(customerId, loginCustomerId, query);
    if (customVariables.length > 0) {
      return customVariables[0].conversionCustomVariable.id;
    }
  }

  /**
   * Gets the user listId of a given list name and upload key type. It
   * only looks for a CRM_BASED and OPEN list.
   * @param {!CustomerMatchConfig} customerMatchConfig
   * @return {number|undefined} User list_id if it exists.
   */
  async getCustomerMatchUserListId(customerMatchConfig) {
    const config = this.getCamelConfig_(customerMatchConfig);
    const { customerId, loginCustomerId, listName, uploadKeyType, } = config;
    const query = `
      SELECT user_list.id, user_list.resource_name
      FROM user_list
      WHERE user_list.name = '${listName}'
        AND customer.id = ${getCleanCid(customerId)}
        AND user_list.type = CRM_BASED
        AND user_list.membership_status = OPEN
        AND user_list.crm_based_user_list.upload_key_type = ${uploadKeyType}
    `;
    const userlists = await this.getReport(customerId, loginCustomerId, query);
    return userlists.length === 0 ? undefined : userlists[0].userList.id;
  }

  /**
   * Creates the user list based on a given customerMatchConfig and returns the
   * Id. The user list would be a CRM_BASED type.
   * Trying to create a list with an used name will fail.
   * The Google Ads service behind this function (UserListService) supports
   * `partial_failure`. Here, we only create one userlist at one time, so
   * `partial_failure` is disabled to simplified the error handleing process.
   * @see getUploadConversionFnBase_ for more details of error handling.
   * @param {!CustomerMatchConfig} customerMatchConfig
   * @return {number} The created user list id. Note this is not the resource
   *   name.
   */
  async createCustomerMatchUserList(customerMatchConfig) {
    const config = this.getCamelConfig_(customerMatchConfig);
    const { customerId, loginCustomerId, listName, uploadKeyType, } = config;
    const userList = new UserList({
      name: listName,
      type: UserListType.CRM_BASED,
      crmBasedUserList: { uploadKeyType },
    });
    const request = new MutateUserListsRequest({
      customerId: getCleanCid(customerId),
      operations: [{ create: userList }],
      validateOnly: this.debugMode, // when true makes no changes
      partialFailure: false, // Simplify error handling in creating userlist
    });
    const client = await this.getUserListServiceClient_();
    const options = this.getCallOptions_(loginCustomerId);
    try {
      /**
       * @type {!MutateUserListsResponse}
       * @see https://developers.google.com/google-ads/api/reference/rpc/latest/MutateUserListsResponse
       */
      const [response] = await client.mutateUserLists(request, options);
      const { results } = response; // No `partialFailureError` here.
      this.logger.debug(`Created crm userlist from`, customerMatchConfig);
      if (!results[0]) {
        if (this.debugMode) {
          throw new Error('No UserList was created in DEBUG mode.');
        } else {
          throw new Error('No UserList was created.');
        }
      }
      const { resourceName } = results[0];
      const splitted = resourceName.split('/');
      return splitted[splitted.length - 1];
    } catch (error) {
      // Get the details from a wrapped GoogleAdsFailure
      if (error.metadata) {
        const failures = this.getGoogleAdsFailures_(error.metadata);
        if (failures.length > 0) {
          if (this.logger.isDebugEnabled())
            debugGoogleAdsFailure(failures, request);
          const message = failures.map((failure) => {
            return failure.errors.map(({ message }) => message).join(' ');
          }).join(';');
          throw new Error(message);
        }
        this.logger.warn(
          'Got an error with metadata but no GoogleAdsFailure in it', error);
      }
      throw error;
    }
  }

  /**
   * Returns the function to send out a request to Google Ads API with
   * user ids for Customer Match upload.
   * This API does NOT support the field 'userIdentifierSource'.
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
      /** @const {BatchResult} */ const batchResult = {
        result: true,
        numberOfLines: lines.length,
      };
      try {
        const response =
          await this.uploadUserDataToUserList(lines, customerMatchConfig);
        this.logger.debug(`Customer Match upload batch[${batchId}]`, response);
        return batchResult;
      } catch (error) {
        this.logger.error(
          `Error in Customer Match upload batch[${batchId}]`, error);
        this.updateBatchResultWithError(batchResult, error, lines, 0);
        return batchResult;
      }
    }
  }

  /**
   * Uploads a user data to a user list (aka customer match). The service
   * doesn't support partial failure. So it would throw out a wrapped
   * GoogleAdsFailure if error happens. The error will be handled by function
   * `getUploadCustomerMatchFn`.
   * @see getUploadCustomerMatchFn
   * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserDataService
   * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserDataOperation
   * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserData
   * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserIdentifier
   * @see https://developers.google.com/google-ads/api/reference/rpc/latest/CustomerMatchUserListMetadata
   * Please note: The UserDataService has a limit of 10 UserDataOperations
   * and 100 user IDs per request
   * @see https://developers.google.com/google-ads/api/docs/migration/user-data-service#rate_limits
   * @param {!Array<string>} lines An array of JSON string for user Ids
   * @param {!CustomerMatchConfig} customerMatchConfig
   * @return {!Promise<UploadUserDataResponse>}
   */
  async uploadUserDataToUserList(lines, customerMatchConfig) {
    const config = this.getCamelConfig_(customerMatchConfig);
    const { customerId, loginCustomerId, listId, operation, } = config;
    const client = await this.getUserDataServiceClient_();
    const userDataList = buildUserDataList(
      lines, config, IDENTIFIERS.CUSTOMER_MATCH, MAX_IDENTIFIERS.USER_DATA);
    const operations = userDataList.map(
      (userData) => new UserDataOperation({ [operation]: new UserData(userData) })
    );
    const metadata = this.buildCustomerMatchUserListMetadata_(config);
    const request = new UploadUserDataRequest({
      customerId: getCleanCid(customerId),
      operations,
      customerMatchUserListMetadata: metadata,
    });
    const options = this.getCallOptions_(loginCustomerId);
    const [response] = await client.uploadUserData(request, options);
    return response;
  }

  /**
   * Creates CustomerMatchUserListMetadata.
   * @see https://developers.google.com/google-ads/api/reference/rpc/latest/CustomerMatchUserListMetadata
   * @param {{
   *     customerId: string,
   *     listId: string,
   *     customerMatchUserListMetadata: undefined|!CustomerMatchUserListMetadata
   *   }}  config Configuration for CustomerMatchUserListMetadata
   * @return {!CustomerMatchUserListMetadata}
   * @private
   */
  buildCustomerMatchUserListMetadata_(config) {
    const { customerId, listId, customerMatchUserListMetadata } = config;
    const resourceName = `customers/${getCleanCid(customerId)}/userLists/${listId}`;
    return new CustomerMatchUserListMetadata(Object.assign({
      userList: resourceName,
    }, customerMatchUserListMetadata));
  }

  /**
   * Get OfflineUserDataJob status.
   * @param {OfflineUserDataJobConfig} offlineUserDataJobConfig
   * @param {string} resourceName
   * @return {string} Job status @see OfflineUserDataJobStatus
   */
  async getOfflineUserDataJob(offlineUserDataJobConfig, resourceName) {
    const config = this.getCamelConfig_(offlineUserDataJobConfig);
    const { customerId, loginCustomerId, } = config;
    const query = `
      SELECT offline_user_data_job.id,
             offline_user_data_job.status,
             offline_user_data_job.type,
             offline_user_data_job.customer_match_user_list_metadata.user_list,
             offline_user_data_job.failure_reason
      FROM offline_user_data_job
      WHERE offline_user_data_job.resource_name = '${resourceName}'
    `;
    const jobs = await this.getReport(customerId, loginCustomerId, query);
    if (jobs.length === 0) {
      throw new Error(`Can't find the OfflineUserDataJob: ${resourceName}`);
    }
    const { failureReason, status } = jobs[0].offlineUserDataJob;
    if (OfflineUserDataJobFailureReason[failureReason] > 0) {
      this.logger.warn(
        `OfflineUserDataJob [${resourceName}] failed: `, failureReason);
    }
    return status;
  }

  /**
   * Creates a OfflineUserDataJob and returns resource name.
   * @param {OfflineUserDataJobConfig} offlineUserDataJobConfig
   * @return {string} The resouce name of the creaed job.
   */
  async createOfflineUserDataJob(offlineUserDataJobConfig) {
    const config = this.getCamelConfig_(offlineUserDataJobConfig);
    const { customerId, loginCustomerId, listId, type, } = config;
    this.logger.debug('Creating OfflineUserDataJob from:',
      offlineUserDataJobConfig);
    const jobData = { type };
    // https://developers.google.com/google-ads/api/rest/reference/rest/latest/OfflineUserDataJobs?hl=en#CustomerMatchUserListMetadata
    if (type.startsWith('CUSTOMER_MATCH')) {
      const metadata = this.buildCustomerMatchUserListMetadata_(config);
      jobData.customerMatchUserListMetadata = metadata;
      // https://developers.google.com/google-ads/api/rest/reference/rest/latest/OfflineUserDataJob?hl=en#StoreSalesMetadata
    } else if (type.startsWith('STORE_SALES')) {
      // Support previous property 'StoreSalesMetadata' for compatibility.
      if (config.storeSalesMetadata || config.StoreSalesMetadata) {
        jobData.storeSalesMetadata = config.storeSalesMetadata
          || config.StoreSalesMetadata;
      }
    } else {
      throw new Error(`UNSUPPORTED OfflineUserDataJobType: ${type}.`);
    }
    const job = new OfflineUserDataJob(jobData);
    const request = new CreateOfflineUserDataJobRequest({
      customerId: getCleanCid(customerId),
      job,
      validateOnly: this.debugMode, // when true makes no changes
      enableMatchRateRangePreview: true,
    });
    const client = await this.getOfflineUserDataJobServiceClient_();
    const options = this.getCallOptions_(loginCustomerId);
    try {
      /**
       * @type {!CreateOfflineUserDataJobResponse}
       * @see https://developers.google.com/google-ads/api/reference/rpc/latest/CreateOfflineUserDataJobResponse
       */
      const [response] = await client.createOfflineUserDataJob(request, options);
      const { resourceName } = response;
      if (!resourceName) {
        if (this.debugMode) {
          throw new Error('No offlineUserDataJob was created in DEBUG mode.');
        } else {
          throw new Error('No offlineUserDataJob was created.');
        }
      }
      this.logger.info('Created OfflineUserDataJob:', resourceName);
      return resourceName;
    } catch (error) {
      if (error.metadata) {
        const failures = this.getGoogleAdsFailures_(error.metadata);
        if (failures.length > 0) {
          if (this.logger.isDebugEnabled())
            debugGoogleAdsFailure(failures, request);
          const message = failures.map((failure) => {
            return failure.errors.map(({ message }) => message).join(' ');
          }).join(';');
          throw new Error(message);
        }
        this.logger.warn(
          'Got an error with metadata but no GoogleAdsFailure in it', error);
      }
      throw error;
    }
  }

  /**
   * Returns the function to send out a request to Google Ads API with
   * user data as operations in OfflineUserDataJob.
   * @param {!OfflineUserDataJobConfig} config
   * @param {string} jobResourceName
   * @return {!SendSingleBatch} Function which can send a batch of hits to
   *     Google Ads API.
   */
  getAddOperationsToOfflineUserDataJobFn(config, jobResourceName) {
    /**
     * Sends a batch of hits to Google Ads API.
     * @param {!Array<string>} lines Data for single request. It should be
     *     guaranteed that it doesn't exceed quota limitation.
     * @param {string} batchId The tag for log.
     * @return {!Promise<BatchResult>}
     */
    return async (lines, batchId) => {
      /** @const {BatchResult} */ const batchResult = {
        result: true,
        numberOfLines: lines.length,
      };
      try {
        const response = await this.addOperationsToOfflineUserDataJob(
          lines, config, jobResourceName);
        this.logger.debug(`Add operation to job batch[${batchId}]`, response);
        const { partialFailureError: failed } = response;
        if (failed) {
          this.logger.info(`Job[${jobResourceName}] faile:`, failed.message);
          const failures = failed.details.map(
            ({ value }) => GoogleAdsFailure.decode(value));
          this.extraFailedLines_(batchResult, failures, lines, 0);
        }
        return batchResult;
      } catch (error) {
        this.logger.error(
          `Error in OfflineUserDataJob add operations batch[${batchId}]`, error);
        this.updateBatchResultWithError(batchResult, error, lines, 0);
        return batchResult;
      }
    }
  }

  /**
   * Adds user data in to the OfflineUserDataJob.
   * @param {!Array<string>} lines An array of JSON string for user Ids
   * @param {OfflineUserDataJobConfig} config Offline user data job config.
   * @param {string} jobResourceName
   * @return {!Promise<!AddOfflineUserDataJobOperationsResponse>}
   */
  async addOperationsToOfflineUserDataJob(lines, offlineUserDataJobConfig,
    jobResourceName) {
    const config = this.getCamelConfig_(offlineUserDataJobConfig);
    const { loginCustomerId, operation, type } = config;
    const client = await this.getOfflineUserDataJobServiceClient_();
    const identifierTypes = type.startsWith('CUSTOMER_MATCH')
      ? IDENTIFIERS.CUSTOMER_MATCH : IDENTIFIERS.STORE_SALES;
    const userDataList = buildUserDataList(
      lines, config, identifierTypes, MAX_IDENTIFIERS.USER_DATA);
    const operations = userDataList.map((userData) => {
      return new OfflineUserDataJobOperation(
        { [operation]: new UserData(userData) });
    });
    const request = new AddOfflineUserDataJobOperationsRequest({
      resourceName: jobResourceName,
      operations,
      validateOnly: this.debugMode,
      enablePartialFailure: true,
      enableWarnings: true,
    });
    const options = this.getCallOptions_(loginCustomerId);
    const [response] =
      await client.addOfflineUserDataJobOperations(request, options);
    return response;
  }

  /**
   * Starts the OfflineUserDataJob.
   * @param {OfflineUserDataJobConfig} offlineUserDataJobConfig Offline user data job config.
   * @param {string} jobResourceName
   * @return {!Promise<!Operation>}
   */
  async runOfflineUserDataJob(offlineUserDataJobConfig, jobResourceName) {
    const config = this.getCamelConfig_(offlineUserDataJobConfig);
    const { loginCustomerId } = config;
    const client = await this.getOfflineUserDataJobServiceClient_();
    const request = new RunOfflineUserDataJobRequest({
      resourceName: jobResourceName,
      validateOnly: this.debugMode,
    });
    const options = this.getCallOptions_(loginCustomerId);
    try {
      const [response] = await client.runOfflineUserDataJob(request, options);
      this.logger.debug('runOfflineUserDataJob response: ', response);
      return response;
    } catch (error) {
      if (error.metadata) {
        const failures = this.getGoogleAdsFailures_(error.metadata);
        if (failures.length > 0) {
          if (this.logger.isDebugEnabled())
            debugGoogleAdsFailure(failures, request);
          const message = failures.map((failure) => {
            return failure.errors.map(({ message }) => message).join(' ');
          }).join(';');
          throw new Error(message);
        }
        this.logger.warn(
          'Got an error with metadata but no GoogleAdsFailure in it', error);
      }
      throw error;
    }
  }

  /**
   * Historically, we used a 3rd party library that adopted snake naming
   * convention as the protobuf files and API documents. However, the
   * automatically generated NodeJS library requires lowerCamel naming
   * convention. To reduce the impact to existing users, the function can
   * convert a 'snake' object to a 'lowerCamel' object.
   * @param {Object} config
   * @return {object}
   */
  getCamelConfig_(config) {
    return changeObjectNamingFromSnakeToLowerCamel(config);
  }

  /**
   * Returns a HTTP header object contains the authentication information for
   * Google Ads API, include: `developer-token` and `ogin-customer-id`.
   * @param {string} loginCustomerId
   * @return {object} The HTTP header object.
   * @private
   */
  getGoogleAdsHeaders_(loginCustomerId) {
    const headers = { 'developer-token': this.developerToken };
    if (loginCustomerId) {
      headers['login-customer-id'] = getCleanCid(loginCustomerId);
    }
    return headers;
  }

  /**
   * Returns an option object as gRPC call options. This is used to add Google
   * Ads API required authentication information to requests.
   * @param {string} loginCustomerId
   * @return {{otherArgs:object}}
   * @private
   */
  getCallOptions_(loginCustomerId) {
    return {
      otherArgs: {
        headers: this.getGoogleAdsHeaders_(loginCustomerId),
      },
    };
  }

  /**
   * Prepares the feach data service client instance.
   * @return {!GoogleAdsServiceClient}
   * @private
   */
  async getGoogleAdsServiceClient_() {
    if (this.googleAdsClient) return this.googleAdsClient;
    await this.authClient.prepareCredentials();
    this.googleAdsClient = new GoogleAdsServiceClient({
      authClient: this.authClient.getDefaultAuth(),
    });
    return this.googleAdsClient;
  }

  /**
   * Prepares the Google Ads field service client instance.
   * @return {!GoogleAdsFieldServiceClient}
   * @private
   */
  async getGoogleAdsFieldServiceClient_() {
    await this.authClient.prepareCredentials();
    return new GoogleAdsFieldServiceClient({
      authClient: this.authClient.getDefaultAuth(),
    });
  }

  /**
   * Prepares the conversion upload service client instance.
   * @return {!ConversionUploadServiceClient}
   * @private
   */
  async getConversionUploadServiceClient_() {
    await this.authClient.prepareCredentials();
    return new ConversionUploadServiceClient({
      authClient: this.authClient.getDefaultAuth(),
    });
  }

  /**
   * Prepares the conversion adjustment upload service client instance.
   * @return {!ConversionAdjustmentUploadServiceClient}
   * @private
   */
  async getConversionAdjustmentUploadServiceClient_() {
    await this.authClient.prepareCredentials();
    return new ConversionAdjustmentUploadServiceClient({
      authClient: this.authClient.getDefaultAuth(),
    });
  }

  /**
   * Prepares the user list manage service client instance.
   * @return {!UserListServiceClient}
   * @private
   */
  async getUserListServiceClient_() {
    await this.authClient.prepareCredentials();
    return new UserListServiceClient({
      authClient: this.authClient.getDefaultAuth(),
    });
  }

  /**
   * Prepares the user data upload service client instance.
   * @return {!UserDataServiceClient}
   * @private
   */
  async getUserDataServiceClient_() {
    await this.authClient.prepareCredentials();
    return new UserDataServiceClient({
      authClient: this.authClient.getDefaultAuth(),
    });
  }

  /**
   * Prepares the offline user data job manage service client instance.
   * @return {!OfflineUserDataJobServiceClient}
   * @private
   */
  async getOfflineUserDataJobServiceClient_() {
    await this.authClient.prepareCredentials();
    return new OfflineUserDataJobServiceClient({
      authClient: this.authClient.getDefaultAuth(),
    });
  }
}

/**
 * Returns an arrary of UserIdentifier object based the given JSON object.
 * @param {Object} record An object contains user indentifier information.
 * @param {!Array<string>} identifierTypes An list of user identifier types that
 *   are supported by the target service.
 * @param {number} maximumNumOfIdentifiers The maximum number of user
 *   identifiers in a conversion or a UserData.
 * @param {!UserIdentifierSource} defaultSource The value of `
 *   UserIdentifierSource` in config. This is not supported by UserDataService
 *   or 'CUSTOMER_MATCH' type OfflineUserDataJob.
 * @return {!Array<!UserData>}
 * @see UserData
 */
const buildUserIdentifierList = (record, identifierTypes,
  maximumNumOfIdentifiers, defaultSource) => {
  const { userIdentifierSource = defaultSource } = record;
  const userIdentifiers = [];
  identifierTypes.forEach((identifierType) => {
    const identifierValue = record[identifierType];
    if (identifierValue) {
      if (Array.isArray(identifierValue)) {
        identifierValue.forEach((id) => {
          userIdentifiers.push(new UserIdentifier({
            [identifierType]: id,
            userIdentifierSource,
          }));
        });
      } else {
        userIdentifiers.push(new UserIdentifier({
          [identifierType]: identifierValue,
          userIdentifierSource,
        }));
      }
    }
  });
  if (userIdentifiers.length <= maximumNumOfIdentifiers) {
    return userIdentifiers;
  } else {
    this.logger.warn(
      `Too many user identifiers, will only send ${maximumNumOfIdentifiers}:`,
      JSON.stringify(record));
    return userIdentifiers.slice(0, MAX_IDENTIFIERS_PER_USER);
  }
}

/**
 * Returns an array of conversion JSON objects based on the given arran of JSON
 * strings. The result can be used to create objects of ClickConversion,
 * CallConversion or ConversionAdjustment.
 * @param {!Array<string>} lines An array of JSON strings of conversions.
 * @param {ConversionConfig} config Conversion configuration.
 * @param {!Array<string>} conversionFields An list of user identifier types that
 *   are supported by the target service.
 * @param {!Array<string>} identifierTypes An list of user identifier types that
 *   are supported by the target service.
 * @param {number} maximumNumOfIdentifiers The maximum number of user
 *   identifiers in a conversion or a UserData.
 * @return {!Array<object>} An array of objects to create ClickConversion,
 *   CallConversion or ConversionAdjustment
 */
const buildConversionJsonList = (lines, config, conversionFields,
  identifierTypes = [], maximumNumOfIdentifiers = 0) => {
  const { customerId, customVariables, userIdentifierSource } = config;
  const configProperties = lodash.pick(config, conversionFields);
  return lines.map((line) => JSON.parse(line))
    .map(changeObjectNamingFromSnakeToLowerCamel)
    //XXX: extra steps here to keep compatibility for snake convention
    .map((record) => {
      const conversion = lodash.merge({}, configProperties,
        lodash.pick(record, conversionFields));
      if (customVariables) {
        const variables = Object.keys(customVariables);
        conversion.customVariables = variables.map((variable) => {
          return new CustomVariable({
            conversionCustomVariable:
              `customers/${getCleanCid(customerId)}/conversionCustomVariables/${customVariables[variable]}`,
            value: record[variable],
          });
        });
      }
      if (identifierTypes.length > 0) {
        const userIdentifiers = buildUserIdentifierList(
          record,
          identifierTypes,
          maximumNumOfIdentifiers,
          userIdentifierSource
        );
        if (userIdentifiers.length > 0) {
          conversion.userIdentifiers = userIdentifiers;
        }
      }
      return conversion;
    })
}
/**
 * Returns an arrary of UserData object based on the given arran of JSON strings.
 * @param {!Array<string>} lines An array of JSON strings of UserData.
 * @param {{
 *   additionalAttributes: (!UserIdentifierSource|undefined)
 *   transactionAttribute: (!TransactionAttribute|undefined),
 *   userAttribute: (!UserAttribute|undefined),
 *   consent: (!Consent|undefined),
 * }} config
 * @see USERDATA_ADDITIONAL_ATTRIBUTES
 * @see UserIdentifierSource Not available in UserDataService or 'CUSTOMER_MATCH'
 *     type OfflineUserDataJob.
 * @return {!Array<!UserData>}
 * @see https://developers.google.com/google-ads/api/reference/rpc/latest/UserData
 */
const buildUserDataList = (lines, config, identifierTypes,
  maximumNumOfIdentifiers) => {
  const configAttributes = lodash.pick(config, USERDATA_ADDITIONAL_ATTRIBUTES);
  const { userIdentifierSource } = config;
  return lines.map((line) => JSON.parse(line))
    //XXX: extra steps here to keep compatibility for snake conversion
    .map(changeObjectNamingFromSnakeToLowerCamel)
    .map((record) => {
      const userData = lodash.merge({}, configAttributes,
        lodash.pick(record, USERDATA_ADDITIONAL_ATTRIBUTES));
      userData.userIdentifiers = buildUserIdentifierList(
        record,
        identifierTypes,
        maximumNumOfIdentifiers,
        userIdentifierSource
      );
      return new UserData(userData);
    });
}

/**
 * Prints the information of GoogleAdsFailure from a request.
 * @param {!Array<!GoogleAdsFailure>} failures
 * @param {(object|undefined)} request
 */
const debugGoogleAdsFailure = (failures, request) => {
  failures.forEach(({ requestId, errors }) => {
    console.log('GoogleAdsFailure request id:', requestId);
    errors.forEach(({ message, location }, index) => {
      console.log(`    GoogleAdsError[${index}]:`, message);
      console.log(`        location:`, location);
    });
  });
  if (request) {
    console.log('Original request:',
      request.toJSON ? request.toJSON() : request);
  }
}

module.exports = {
  GoogleAdsField,
  ConversionConfig,
  CustomerMatchRecord,
  CustomerMatchConfig,
  OfflineUserDataJobConfig,
  GoogleAdsApi,
  buildUserIdentifierList,
  buildConversionJsonList,
  buildUserDataList,
  CONVERSION_FIELDS,
  IDENTIFIERS,
  MAX_IDENTIFIERS,
};
