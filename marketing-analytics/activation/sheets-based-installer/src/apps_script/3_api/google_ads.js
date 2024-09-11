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

/** @fileoverview Google Ads API handler class.*/

class GoogleAds extends ExternalApi {

  /**
   * @constructor
   * @param {Object} option Authorization option.
   * @param {string} mccCid Google Ads MCC account Id.
   * @param {string} developerToken Google Ads developer token.
   */
  constructor(option, mccCid, developerToken) {
    super(option);
    this.apiUrl = 'https://googleads.googleapis.com';
    this.version = 'v16';
    this.mccCid = GoogleAds.getCleanedId(mccCid);
    this.developerToken = developerToken;
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /** @override */
  getScope() {
    return 'https://www.googleapis.com/auth/adwords';
  }

  /**
   * Gets the default HTTP header for Google Ads API requests.
   * @return {Object}
   */
  getDefaultHeader() {
    const headers = super.getDefaultHeader();
    headers['developer-token'] = this.developerToken;
    headers['login-customer-id'] = this.mccCid;
    return headers;
  }

  /**
   * Converts Google Ads API error to a VerifyResult.
   * @param {Object} error
   * @return {VerifyResult}
   * @private
   */
  getErrorResult_(error) {
    return {
      valid: false,
      reason: error.details[0].errors.map(({ message }) => message).join(' '),
    }
  }

  /**
   * Verifies the accessiblity of current user to the Google Ads account.
   * @return {VerifyResult}
   */
  verifyReportAccess() {
    const payload = { query: 'SELECT customer.id FROM customer' };
    const { error } = this.mutate(
      `customers/${this.mccCid}/googleAds:search`, payload);
    if (error) return this.getErrorResult_(error);
    return { valid: true };
  }

  /**
   * Verifies the Google Ads conversion.
   * @param {string} conversionId
   * @param {string=} cid
   * @return {VerifyResult}
   */
  verifyConversion(conversionId, cid = this.mccCid) {
    const cleanedId = GoogleAds.getCleanedId(cid);
    const payload = {
      query: `SELECT customer.id FROM conversion_action WHERE conversion_action.id = ${conversionId}`,
    };
    const response = this.mutate(`customers/${cleanedId}/googleAds:search`,
      payload);
    const { error, results } = response;
    if (error) return this.getErrorResult_(error);
    if (!results || results.length === 0) {
      return {
        valid: false,
        reason: `Can not find the conversion ${conversionId} for account ${cid}`,
      };
    }
    return { valid: true };
  }

  /**
   * Verifies the user list.
   * @param {string} listId
   * @param {string } cid
   * @return {VerifyResult}
   */
  verifyUserList(listId, cid = this.mccCid) {
    const cleanedId = GoogleAds.getCleanedId(cid);
    const payload = {
      query: `SELECT customer.id FROM user_list WHERE user_list.id = ${listId}`,
    };
    const response = this.mutate(`customers/${cleanedId}/googleAds:search`,
      payload);
    const { error, results } = response;
    if (error) return this.getErrorResult_(error);
    if (!results || results.length === 0) {
      return {
        valid: false,
        reason: `Can not find the user list ${listId} for account ${cid}`,
      };
    }
    return { valid: true };
  }
}

/**
 * Returns the CID without hyphens.
 * @param {string} cid
 * @return {string}
 */
GoogleAds.getCleanedId = (cid) => {
  return cid.replace(/-/g, '');
}
