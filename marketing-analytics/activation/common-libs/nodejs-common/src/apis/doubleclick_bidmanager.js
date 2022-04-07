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

const {google} = require('googleapis');
const AuthClient = require('./auth_client.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/doubleclickbidmanager',
]);
const API_VERSION = 'v1.1';

/**
 * The returned information of get a query.
 * @typedef {{
 *   running:boolean,
 *   latestReportRunTimeMs:string,
 *   googleCloudStoragePathForLatestReport:string,
 * }}
 */
let QueryResource;

/**
 * RequestBody controls the data range of reports.
 * see:
 * https://developers.google.com/bid-manager/v1.1/queries/runquery#request-body
 * @typedef {{
 *   dataRange: string,
 *   reportDataStartTimeMs: long,
 *   reportDataEndTimeMs: long,
 *   timezoneCode: string
 * }}
 */
let RequestBody;

/**
 * DoubleClick Bid Manager (DV360) Ads 360 API stub.
 * Note: DV360 report API only support OAuth 2.0, see:
 * https://developers.google.com/bid-manager/how-tos/authorizing
 */
class DoubleClickBidManager {
  /**
   * @constructor
   * @param {!Object<string,string>=} env The environment object to hold env
   *     variables.
   */
  constructor(env = process.env) {
    const authClient = new AuthClient(API_SCOPES, env);
    const auth = authClient.getDefaultAuth();
    /** @const {!google.doubleclickbidmanager} */
    this.instance = google.doubleclickbidmanager({
      version: API_VERSION,
      auth,
    });
  }

  /**
   * Starts to run a query.
   * See https://developers.google.com/bid-manager/v1.1/queries/runquery
   * This API returns empty HTTP content.
   * @param {number} queryId
   * @param {!RequestBody|undefined=} requestBody Data range of the report.
   * @return {!Promise<boolean>} Whether it starts successfully.
   */
  async runQuery(queryId, requestBody = undefined) {
    const response = await this.instance.queries.runquery(
        {queryId, requestBody});
    return response.status >= 200 && response.status < 300;
  }

  /**
   * Gets a query resource.
   * See https://developers.google.com/bid-manager/v1.1/queries/getquery
   * @param {number} queryId
   * @return {!Promise<!QueryResource>} Query resource, see
   *     https://developers.google.com/bid-manager/v1.1/queries#resource
   */
  async getQuery(queryId) {
    const response = await this.instance.queries.getquery({queryId});
    return response.data.metadata;
  }

  /**
   * Creates a query.
   * @param {Object} query The DV360 query object, for more details, see:
   *     https://developers.google.com/bid-manager/v1.1/queries#resource
   * @return {!Promise<number>} Id of created query.
   */
  async createQuery(query) {
    const response = await this.instance.queries.createquery(
        {requestBody: query});
    return response.data.queryId;
  }

  /**
   * Deletes a query.
   * @param {number} queryId
   * @return {!Promise<boolean>} Whether the query was deleted.
   */
  async deleteQuery(queryId) {
    try {
      await this.instance.queries.deletequery({ queryId });
      return true;
    } catch (error) {
      console.error(error);
      return false;
    }
  }
}

module.exports = {
  QueryResource,
  RequestBody,
  DoubleClickBidManager,
};
