// Copyright 2022 Google Inc.
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

/** @fileoverview Display & Video 360 Reporting API handler class.*/

/**
 * Report DV360 is known as DoubleClick Bid Manager API.
 * @see https://developers.google.com/bid-manager/reference/rest
 */
class DoubleclickBidManager extends ExternalApi {

  constructor(option) {
    super(option);
    this.name = 'DoubleClick Bid Manager API';
    this.api = 'doubleclickbidmanager.googleapis.com';
    this.apiUrl = `https://${this.api}`;
    this.version = 'v2';
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /** @override */
  getScope() {
    return 'https://www.googleapis.com/auth/doubleclickbidmanager';
  }

  /**
   * Verifies the existence of the query and the query is exported as CSV.
   * @see https://developers.google.com/bid-manager/reference/rest/v2/queries/get
   * @param {string} queryId
   * @return {VerifyResult}
   */
  verifyQuery(queryId) {
    const { error, metadata } = this.getQuery(queryId);
    if (error) {
      return {
        valid: false,
        reason: error.message,
      };
    }
    if (metadata.format !== 'CSV') {
      return {
        valid: false,
        reason: `Query[${queryId}]'s format is not CSV`,
      };
    }
    return { valid: true };
  }

  /**
   * Retrieves a query.
   * @see https://developers.google.com/bid-manager/reference/rest/v2/queries/get
   * @param {string} queryId
   * @return {Query}
   */
  getQuery(queryId) {
    return this.get(`queries/${queryId}`);
  }


  /**
   * Creates a query.
   * @see https://developers.google.com/bid-manager/reference/rest/v2/queries/create
   * @param {string} query
   * @return {Query}
   */
  createQuery(query) {
    return this.mutate('queries', query);
  }

  /**
   * Deletes a query as well as the associated reports.
   * @see https://developers.google.com/bid-manager/reference/rest/v2/queries/delete
   * @param {string} queryId
   * @return {object} Should be empty.
   */
  deleteQuery(queryId) {
    return this.mutate(`queries/${queryId}`, undefined, 'DELETE');
  }
}
