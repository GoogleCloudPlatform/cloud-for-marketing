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
 * @fileoverview ADH API connector.
 */

'use strict';

const {request, GaxiosOptions} = require('gaxios');
const AuthClient = require('./auth_client.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/adsdatahub',
]);
const API_VERSION = 'v1';
const API_ENDPOINT = 'https://adsdatahub.googleapis.com';

/**
 * Ads Data Hub (ADH) API connector class based on ADH REST API.
 * @see: https://developers.google.com/ads-data-hub/reference/rest
 */
class AdsDataHub {
  /**
   * Constructor.
   *
   * @param {GaxiosOptions|undefined=} options Used to setup for tests.
   * @param {string|undefined=} customerId ADH customer id.
   * @param {!Object<string,string>=} env The environment object to hold env
   *     variables.
   */
  constructor(options, customerId = undefined, env = process.env) {
    this.authClient = new AuthClient(API_SCOPES, env);
    /** @const{GaxiosOptions} */ this.options = options || {};
    /** @const{string|undefined=} */ this.customerId = customerId;
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
   * Query name has the form
   * 'customers/[customerId]/analysisQueries/[resource_id]'. For better
   * flexibility, this functions returns the unique query name no matter the
   * input is 'resource Id + customerId' or 'unique query name'.
   * @param {string} queryName
   * @param {string=} customerId
   * @return {string} Unique query name.
   * @private
   */
  getUniqueQueryName_(queryName, customerId = this.customerId) {
    if (queryName.startsWith('customers')) {
      return queryName;
    }
    return `customers/${customerId}/analysisQueries/${queryName}`;
  }

  /**
   * Returns the base Url of API service.
   * @return {string} Base Url.
   * @private
   */
  getRequestBaseUrl_() {
    return `${API_ENDPOINT}/${API_VERSION}/`;
  }

  /**
   * Sends a request to server and return the response body.
   * @param {string} path Request relative path.
   * @param {string=} method HTTP method, default is 'GET'.
   * @param {Object|undefined=} data HTTP data to send
   * @return {!Promise<Object>} Response body.
   * @private
   */
  async sendRequestAndReturnResponse_(path, method = 'GET', data = undefined) {
    const auth = await this.getAuth_();
    const headers = await auth.getRequestHeaders();
    const url = this.getRequestBaseUrl_() + path;
    const response = await request(/** @type {GaxiosOptions} */
        Object.assign({}, this.options, {
          url,
          method,
          headers,
          data,
        }));
    return response.data;
  }

  /**
   * Retrieves the requested analysis query. If query doesn't exist, it will
   * throw an error.
   * @see https://developers.google.com/ads-data-hub/reference/rest/v1/customers.analysisQueries/get
   * @param {string} queryName Resource ID or unique name of the query.
   * @param {string=} customerId
   * @return {!Promise<Object>} A promise of AnalysisQuery object if found.
   *     @see https://developers.google.com/ads-data-hub/reference/rest/v1/customers.analysisQueries#AnalysisQuery
   */
  async getQuery(queryName, customerId = this.customerId) {
    const uniqueQueryName = this.getUniqueQueryName_(queryName, customerId);
    return this.sendRequestAndReturnResponse_(uniqueQueryName);
  }

  /**
   * Lists the analysis queries owned by the specified customer.
   * @see https://developers.google.com/ads-data-hub/reference/rest/v1/customers.analysisQueries/list
   * @param {Object=} parameters The query parameter object.
   * @param {string=} customerId
   * @return {!Promise<{
   *   queries:Array<Object>,
   *   nextPageToken:string,
   * }>}
   */
  async listQuery(parameters = {}, customerId = this.customerId) {
    const querystring = this.getQueryString_(parameters);
    const path = `customers/${customerId}/analysisQueries`
      + (querystring ? `?${querystring}` : '');
    return this.sendRequestAndReturnResponse_(path);
  }

  /**
   * Starts execution on a transient analysis query. The results will be written
   * to the specified BigQuery destination table. The returned operation name
   * can be used to poll for query completion status.
   * @see https://developers.google.com/ads-data-hub/reference/rest/v1/customers.analysisQueries/startTransient
   * @param {string} queryText The content of the query.
   * @param {Object} spec Defines the query execution parameters.
   *     @see https://developers.google.com/ads-data-hub/reference/rest/v1/QueryExecutionSpec
   * @param {string} destTable Destination BigQuery table for query results with
   *     the format 'project.dataset.table_name'. If specified, the project must
   *     be explicitly whitelisted for the customer's ADH account. If project is
   *     not specified, uses default project for the provided customer. If
   *     neither project nor dataset is specified, uses the default project and
   *     dataset.
   * @param {string=} customerId
   * @return {!Promise<Object>} Promised operation object.
   *     @see https://developers.google.com/ads-data-hub/reference/rest/v1/operations#Operation
   */
  async startTransientQuery(queryText, spec, destTable, customerId = this.customerId) {
    const path = `customers/${customerId}/analysisQueries:startTransient`;
    const data = { query: { queryText }, spec, destTable };
    return this.sendRequestAndReturnResponse_(path, 'POST', data);
  }

  /**
   * Creates a query and returns the unique query name in the form of
   * 'customers/[customerId]/analysisQueries/[resource_id]'.
   * @see https://developers.google.com/ads-data-hub/reference/rest/v1/customers.analysisQueries/create
   * @param {string} title The title of the query.
   * @param {string} queryText The content of the query
   * @return {!Promise<string>} Promised unique name of created query.
   */
  async createQuery(title, queryText) {
    const path = `customers/${this.customerId}/analysisQueries`;
    const data = {title, queryText,};
    const query = await this.sendRequestAndReturnResponse_(path, 'POST', data);
    return query.name;
  }

  /**
   * Starts execution on a stored analysis query. The results will be written to
   * the specified BigQuery destination table. The returned operation name can
   * be used to poll for query completion status.
   * @see https://developers.google.com/ads-data-hub/reference/rest/v1/customers.analysisQueries/start
   * @param {string} uniqueQueryName The unique name of query.
   * @param {Object} spec Defines the query execution parameters.
   *     @see https://developers.google.com/ads-data-hub/reference/rest/v1/QueryExecutionSpec
   * @param {string} destTable Destination BigQuery table for query results with
   *     the format 'project.dataset.table_name'. If specified, the project must
   *     be explicitly whitelisted for the customer's ADH account. If project is
   *     not specified, uses default project for the provided customer. If
   *     neither project nor dataset is specified, uses the default project and
   *     dataset.
   * @param {string|undefined=} customerId Optional. Ads Data Hub customer
   *     executing the query. If not specified, defaults to the customer that
   *     owns the query.
   * @return {!Promise<Object>} Promised operation object.
   *     @see https://developers.google.com/ads-data-hub/reference/rest/v1/operations#Operation
   */
  async startQuery(uniqueQueryName, spec, destTable, customerId = undefined) {
    const path = `${uniqueQueryName}:start`;
    const data = {spec, destTable, customerId};
    return this.sendRequestAndReturnResponse_(path, 'POST', data);
  }

  /**
   * Deletes an analysis query. If successful, the response body will be '{}'.
   * @see https://developers.google.com/ads-data-hub/reference/rest/v1/customers.analysisQueries/delete
   * @param {string} queryName Resource ID or unique name of the query.
   * @param {string=} customerId
   * @return {!Promise<Object>} If successful, the response body will be empty.
   */
  async deleteQuery(queryName, customerId = this.customerId) {
    const uniqueQueryName = this.getUniqueQueryName_(queryName, customerId);
    return this.sendRequestAndReturnResponse_(uniqueQueryName, 'DELETE');
  }

  /**
   * Gets the latest state of a long-running operation.
   * @see: https://developers.google.com/ads-data-hub/reference/rest/v1/operations/get
   * @param {string} operationName The name of the operation resource.
   * @return {!Promise<Object>} Promised operation object.
   *     @see https://developers.google.com/ads-data-hub/reference/rest/v1/operations#Operation
   */
  async getQueryStatus(operationName) {
    return this.sendRequestAndReturnResponse_(operationName);
  }

  /**
   * Gets the query string of a parameter object.
   * @param {Object} parameters The object of key-value pairs which will be
   *   converted into query string format.
   * @return string
   * @private
   */
  getQueryString_(parameters = {}) {
    return Object.keys(parameters)
      .map((key) => key + '=' + encodeURIComponent(parameters[key]))
      .join('&');
  }
}

exports.AdsDataHub = AdsDataHub;
