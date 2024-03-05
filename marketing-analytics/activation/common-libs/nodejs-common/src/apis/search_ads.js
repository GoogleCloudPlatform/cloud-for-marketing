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
 * @fileoverview Google Search Ads 360 Reporting on GoogleAPI Client Library.
 */

'use strict';

const { request: gaxiosRequest } = require('gaxios');
const { google } = require('googleapis');
const AuthClient = require('./auth_client.js');
const { getLogger } = require('../components/utils.js');
const { getCleanCid, RestSearchStreamTransform }
  = require('./base/ads_api_common.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/doubleclicksearch',
]);
const API_ENDPOINT = 'https://searchads360.googleapis.com';
const API_VERSION = 'v0';

/**
 * Search Ads 360 Reporting API stub.
 * See: https://developers.google.com/search-ads/reporting/api/reference/release-notes
 */
class SearchAds {

  /**
   * @constructor
   * @param {!Object<string,string>=} env The environment object to hold env
   *     variables.
   */
  constructor(env = process.env) {
    this.authClient = new AuthClient(API_SCOPES, env);
    this.logger = getLogger('API.SA');
  }

  /**
   * Prepares the Search Ads 360 Reporting API instance.
   * OAuth 2.0 application credentials is required for calling this API.
   * For Search Ads Reporting API calls made by a manager to a client account,
   * a HTTP header named `login-customer-id` is required in the request. This
   * value represents the Search Ads 360 customer ID of the manager making the
   * API call. Be sure to remove any hyphens (—), for example: 1234567890, not
   * 123-456-7890.
   * @see https://developers.google.com/search-ads/reporting/api/reference/rest/auth
   * @return {!google.searchads360}
   * @private
   */
  async getApiClient_(loginCustomerId) {
    this.logger.debug(`Initialized SA reporting for ${loginCustomerId}`);
    const options = {
      version: API_VERSION,
      auth: await this.getAuth_(),
    };
    if (loginCustomerId) {
      options.headers = { 'login-customer-id': getCleanCid(loginCustomerId) };
    }
    return google.searchads360(options);
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
   * Gets a report synchronously from a given Customer account.
   * If there is a `nextPageToken` in the response, it means the report is not
   * finished and there are more pages.
   * @see https://developers.google.com/search-ads/reporting/api/reference/rpc/google.ads.searchads360.v0.services#searchads360service
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {string} query
   * @param {object=} options Options for `SearchSearchAds360Request`.
   * @see https://developers.google.com/search-ads/reporting/api/reference/rpc/google.ads.searchads360.v0.services#searchsearchads360request
   * @return {!SearchAds360Field}
   * @see https://developers.google.com/search-ads/reporting/api/reference/rpc/google.ads.searchads360.v0.services#searchsearchads360response
   */
  async getPaginatedReport(customerId, loginCustomerId, query, options = {}) {
    const searchads = await this.getApiClient_(loginCustomerId);
    const requestBody = Object.assign({
      query,
      pageSize: 10000,
    }, options);
    const response = await searchads.customers.searchAds360.search({
      customerId: getCleanCid(customerId),
      requestBody,
    });
    return response.data;
  }

  /**
   * Gets a report stream from a Search Ads 360 reporting API.
   * The streamed content is not NDJSON format, but an array of JSON objects
   * with each element has a property `results`.
   * `data` support `batchSize` to set how many rows in one result element.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {string} query
   * @return {!Promise<stream>}
   * @see https://developers.google.com/search-ads/reporting/api/reference/rest/search
   */
  async restStreamReport(customerId, loginCustomerId, query) {
    const auth = await this.getAuth_();
    const headers = Object.assign(
      await auth.getRequestHeaders(), {
      'login-customer-id': getCleanCid(loginCustomerId),
    });
    const options = {
      baseURL: `${API_ENDPOINT}/${API_VERSION}/`,
      url: `customers/${getCleanCid(customerId)}/searchAds360:searchStream`,
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
   * @param {boolean=} snakeCase Output JSON objects in snake_case.
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
   * Returns the requested field or resource (artifact) used by SearchAds360Service.
   * This service doesn't require `login-customer-id` HTTP header.
   * @see https://developers.google.com/search-ads/reporting/api/reference/rest/v0/searchAds360Fields/get
   * @param {string} fieldName
   * @return {!SearchAds360Field}
   * @see https://developers.google.com/search-ads/reporting/api/reference/rest/v0/searchAds360Fields#SearchAds360Field
   */
  async getReportField(fieldName) {
    const searchads = await this.getApiClient_();
    const resourceName = `searchAds360Fields/${fieldName}`;
    const response =
      await searchads.searchAds360Fields.get({ resourceName });
    return response.data;
  }

  /**
   * Returns resources information from Search Ads API.
   * @see: https://developers.google.com/search-ads/reporting/api/reference/rest/v0/searchAds360Fields
   * Note, it looks like this function doesn't check the CID, just using OAuth.
   * @param {Array<string>} adFields Array of Ad fields.
   * @param {Array<string>} metadata Select fields, default values are:
   *     name, data_type, is_repeated, type_url.
   * @return {!Promise<!Array<GoogleAdsField>>}
   * @see GoogleAdsApi.searchReportField
   */
  async searchReportField(adFields,
    metadata = ['name', 'data_type', 'is_repeated', 'type_url',]) {
    const searchads = await this.getApiClient_();
    const selectClause = metadata.join(',');
    const fields = adFields.join('","');
    const query = `SELECT ${selectClause} WHERE name IN ("${fields}")`;
    const response =
      await searchads.searchAds360Fields.search({ query, pageSize: 10000 });
    return response.data.results;
  }

  /**
   * Returns all the custom columns associated with the customer in full detail.
   * @see https://developers.google.com/search-ads/reporting/api/reference/rest/v0/customers.customColumns/list
   * @param {string} customerId - The ID of the customer.
   * @param {string} loginCustomerId - The ID of the manager.
   * @return {!Array<CustomColumn>}
   * @see https://developers.google.com/search-ads/reporting/api/reference/rest/v0/customers.customColumns#CustomColumn
   */
  async listCustomColumns(customerId, loginCustomerId) {
    const searchads = await this.getApiClient_(loginCustomerId);
    const response = await searchads.customers.customColumns.list({ customerId });
    return response.data.customColumns;
  }

  /**
   * Returns the requested custom column in full detail.
   * @see https://developers.google.com/search-ads/reporting/api/reference/rest/v0/customers.customColumns/get
   * @param {string} columnId - The ID of the customColumn.
   * @param {string} customerId - The ID of the customer.
   * @param {string} loginCustomerId - The ID of the manager.
   * @return {!CustomColumn}
   * @see https://developers.google.com/search-ads/reporting/api/reference/rest/v0/customers.customColumns#CustomColumn
   */
  async getCustomColumn(columnId, customerId, loginCustomerId) {
    const resourceName = `customers/${customerId}/customColumns/${columnId}`;
    const searchads = await this.getApiClient_(loginCustomerId);
    const response = await searchads.customers.customColumns.get({ resourceName });
    return response.data;
  }

}

module.exports = {
  SearchAds,
  API_VERSION,
  API_SCOPES,
};
