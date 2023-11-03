// Copyright 2023 Google Inc.
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

/** @fileoverview External (non Google Cloud) REST API base class. */

const OAUTH_BASE_URL = 'https://accounts.google.com/o/oauth2/';


/**
 * OAuth2 refresh token.
 *
 * @typedef {{
 *   client_id: string,
 *   client_secret: string,
 *   token: {
 *     refresh_token: string,
*   },
 * }}
 */
let OAuthRefreshToken;

/**
 * Authorization option for external Api classes.
 *
 * @typedef {{
 *   oauth: !OAuthRefreshToken|undefined,
 *   serviceAccount: string|undefined,
 * }}
 */
let ExternalApiAuthOption;

/**
 * This class contains basic methods to interact with general APIs using the
 * @link {UrlFetchApp} and @link {ScriptApp} classes.
 * @see https://developers.google.com/apps-script/reference/url-fetch/url-fetch-app?hl=en
 *
 * This class can consume a given OAuth token with fallback to the Apps Script
 * default one. This can be used to verify access to external systems.
 *
 * @abstract
 */
class ExternalApi extends ApiBase {

  /**
   * @param {!ExternalApiAuthOption=} option
   */
  constructor(option = {}) {
    super();
    this.oauth = option.oauth;
    this.serviceAccount = option.serviceAccount;
  }

  /**
   * Returns a string or an array of strings of OAuth2 APIs.
   * @return {string|!Array<string>}
   */
  getScope() {
    throw new Error('Need to be implemented by subclass.');
  }

  /**
   * @override
   * Returns an OAuth access key baded on the given OAuth refresh token.
   * @return {string} access token.
   */
  getAccessToken() {
    if (!this.oauth && !this.serviceAccount) return super.getAccessToken();
    if (this.oauth) {
      const { client_id, client_secret, token: { refresh_token } } = this.oauth;
      const payload = {
        client_id,
        client_secret,
        refresh_token,
        grant_type: 'refresh_token',
      }
      const params = {
        method: 'POST',
        payload: JSON.stringify(payload),
        contentType: 'application/json',
        muteHttpExceptions: true,
        headers: { Accept: 'application/json', },
      }
      const { error, access_token } =
        this.fetchAndReturnJson(`${OAUTH_BASE_URL}token`, params);
      if (error) throw error;
      console.log(`${this.constructor.name} generated OAuth2 access token for`,
        client_id);
      return access_token;
    }
    if (this.serviceAccount) {
      const iamCredentials = new IamCredentials();
      const { accessToken, error } =
        iamCredentials.generateAccessToken(this.serviceAccount, this.getScope());
      if (error) throw error;
      console.log(`${this.constructor.name} generated JWT access token for`,
        this.serviceAccount);
      return accessToken;
    }
  }
}

/**
 * Type definition of the API settings verification result.
 *
 * @typedef {{
 *   valid: boolean,
 *   label: string|undefined,
 *   reason: string|undefined,
 * }}
 */
let VerifyResult;
