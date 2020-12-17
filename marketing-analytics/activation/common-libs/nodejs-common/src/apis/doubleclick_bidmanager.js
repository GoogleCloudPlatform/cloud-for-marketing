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
 * DoubleClick Bid Manager (DV360) Ads 360 API stub.
 * Note: DV360 report API only support OAuth 2.0, see:
 * https://developers.google.com/bid-manager/how-tos/authorizing
 */
class DoubleClickBidManager {
  constructor() {
    const authClient = new AuthClient(API_SCOPES);
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
   * @return {!Promise<boolean>} Whether it starts successfully.
   */
  runQuery(queryId) {
    return this.instance.queries.runquery({queryId})
        .then((response) => response.status >= 200 && response.status < 300);
  }

  /**
   * Gets a query resource.
   * See https://developers.google.com/bid-manager/v1.1/queries/getquery
   * @param {number} queryId
   * @return {!Promise<!QueryResource>} Query resource, see
   *     https://developers.google.com/bid-manager/v1.1/queries#resource
   */
  getQuery(queryId) {
    return this.instance.queries.getquery({queryId})
        .then((response) => response.data.metadata);
  }

}

module.exports = {
  QueryResource,
  DoubleClickBidManager,
};
