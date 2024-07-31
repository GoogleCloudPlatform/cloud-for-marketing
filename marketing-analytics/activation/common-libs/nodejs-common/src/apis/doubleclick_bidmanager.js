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
 * @fileoverview Google DoubleClick Bid Manager API adapter based on Google
 * API Client Library.
 */

'use strict';

const { GoogleApiClient } = require('./base/google_api_client.js');
const { getLogger } = require('../components/utils.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/doubleclickbidmanager',
]);
const API_VERSION = 'v2';

/**
 * RequestBody controls the data range of reports.
 * see:
 * https://developers.google.com/bid-manager/reference/rest/v2/queries/run#RunQueryRequest
 * @typedef {{
 *   dataRange: {
 *     range: Range,
 *     customStartDate: {
 *       year: integer,
 *       month: integer,
 *       day: integer,
 *     },
 *     customEndDate: {
*       year: integer,
*       month: integer,
*       day: integer,
*     },
 *   }
 * }}
 */
let RequestBody;

/**
 * DoubleClick Bid Manager (DV360) Ads 360 API stub.
 * Note: DV360 report API only support OAuth 2.0, see:
 * https://developers.google.com/bid-manager/how-tos/authorizing
 */
class DoubleClickBidManager extends GoogleApiClient {
  /**
   * @constructor
   * @param {!Object<string,string>=} env The environment object to hold env
   *     variables.
   */
  constructor(env = process.env) {
    super(env);
    this.googleApi = 'doubleclickbidmanager';
    this.logger = getLogger('API.DV3');
  }

  /** @override */
  getScope() {
    return API_SCOPES;
  }

  /** @override */
  getVersion() {
    return API_VERSION;
  }

  /**
   * Starts to run a query.
   * See https://developers.google.com/bid-manager/reference/rest/v2/queries/run
   * @param {number} queryId
   * @param {!RequestBody|undefined=} requestBody Data range of the report.
   * @return {!Promise<number>} Report Id.
   */
  async runQuery(queryId, requestBody = undefined) {
    const doubleclickbidmanager = await this.getApiClient();
    const response = await doubleclickbidmanager.queries.run(
        {queryId, requestBody});
    return response.data.key.reportId;
  }

  /**
   * Gets a query metadata.
   * See https://developers.google.com/bid-manager/reference/rest/v2/queries/get
   * @param {number} queryId Id of the query.
   * @return {!Promise<!Query>} Query, see
   *     https://developers.google.com/bid-manager/reference/rest/v2/queries#Query
   */
  async getQuery(queryId) {
    const doubleclickbidmanager = await this.getApiClient();
    const response = await doubleclickbidmanager.queries.get({ queryId });
    return response.data;
  }

  /**
   * Creates a query.
   * @param {Object} query The DV360 query object, for more details, see:
   *     https://developers.google.com/bid-manager/reference/rest/v2/queries#Query
   * @return {!Promise<number>} Id of created query.
   */
  async createQuery(query) {
    const doubleclickbidmanager = await this.getApiClient();
    const response = await doubleclickbidmanager.queries.create(
        {requestBody: query});
    return response.data.queryId;
  }

  async getQueryReport(queryId, reportId) {
    const doubleclickbidmanager = await this.getApiClient();
    const response = await doubleclickbidmanager.queries.reports.get(
      { queryId, reportId });
    return response.data.metadata;
  }

  /**
   * Deletes a query.
   * @param {number} queryId
   * @return {!Promise<boolean>} Whether the query was deleted.
   */
  async deleteQuery(queryId) {
    const doubleclickbidmanager = await this.getApiClient();
    try {
      const { status } = await doubleclickbidmanager.queries.delete({ queryId });
      return status === 200;
    } catch (error) {
      this.logger.error(error);
      return false;
    }
  }
}

module.exports = {
  RequestBody,
  DoubleClickBidManager,
};
