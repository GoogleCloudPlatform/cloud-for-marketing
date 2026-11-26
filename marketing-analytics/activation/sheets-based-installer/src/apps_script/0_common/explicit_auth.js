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

/** @fileoverview Google Sheets explicit authorization functions. */

/**
 * @typedef {{
 *   explicitAuthStatus: (string|undefined),
 *   clientId: (string|undefined),
 *   clientSecret: (string|undefined),
 *   refreshToken: (string|undefined),
 * }}
 */
/** @type {!ExplicitAuthInfo} */
let ExplicitAuthInfo;

/**
 * The class to manage the explicit authorization.
 * There is a known issue that the Apps Script project can not be updated to a
 * GCP project which is owned by the organization other than the one that owns
 * the Sheets. To solve this issue, an explicit authorization can be applied in
 * the Sheets to support using a GCP from other organization.
 */
/** @unrestricted */
class ExplicitAuthorization {
  /**
   * Status of the explicit authorization.
   * @return {!Object<string, string>}
   */
  static get STATUS() {
    return {
      OK: 'OK',
      INVALID: 'INVALID',
    };
  }

  /**
   * The url for get new tokens from a refresh token.
   * @return {string}
   */
  static get REFRESH_TOKEN_URL() {
    return 'https://accounts.google.com/o/oauth2/token';
  }

  /**
   * The url for get the information of an access token.
   * This is used to get the Apps Script authorization scope.
   * @return {string}
   */
  static get ACCESS_TOKEN_INFO() {
    return 'https://www.googleapis.com/oauth2/v3/tokeninfo?access_token=';
  }

  /**
   * Property names of explicit authorization.
   * @return {!Array<string>}
   */
  static get PROPERTIES() {
    return [
      'explicitAuthStatus',
      'clientId',
      'clientSecret',
      'refreshToken',
      'accessToken',
      'idToken',
      'expiredAt',
    ];
  }

  /**
   * Loads explicit authorization properties.
   * `UserProperties` is used to limit the authorization information to be
   * accessible only to the current user and only within this script.
   * @see https://developers.google.com/apps-script/reference/properties/properties-service#getUserProperties()
   */
  constructor() {
    console.log('Init explicit authorization instance');
    this.properties = PropertiesService.getUserProperties();
    ExplicitAuthorization.PROPERTIES.forEach(key => {
      this[key] = this.properties.getProperty(key);
    });
    ExplicitAuthorization.activeInstance = this;
    this.menuItem = [
      {separator: true},
      {
        name: 'Explicit authorization',
        method: 'showExplicitAuthSidebar', // Points to the global router stub
        // below
      },
    ];
  }

  /**
   * Shows the sidebar to manage authorization for the current Sheet.
   */
  showSidebar() {
    const html = HtmlService.createHtmlOutputFromFile('pages/explicit_auth')
      .setTitle('Explicit Authorization')
      .setWidth(300);
    SpreadsheetApp.getUi().showSidebar(html);
  }

  /**
   * Returns whether explicit authorization is enabled.
   * It will throw an error if the status is 'INVALID'.
   * @return {boolean}
   */
  enabled() {
    if (this.explicitAuthStatus === ExplicitAuthorization.STATUS.INVALID) {
      throw new Error('Invalid explicit authorization.');
    }
    return this.explicitAuthStatus === ExplicitAuthorization.STATUS.OK;
  }

  /**
   * Returns the explicit authorization information for the sidebar.
   * @return {!ExplicitAuthInfo}
   */
  getAuth() {
    return {
      explicitAuthStatus: this.explicitAuthStatus,
      clientId: this.clientId,
      clientSecret: this.clientSecret,
      refreshToken: this.refreshToken,
    };
  }

  /**
   * Deletes the explicit authorization.
   * @return {!ExplicitAuthInfo}
   */
  deleteAuth() {
    console.log('Delete explicit auth');
    ExplicitAuthorization.PROPERTIES.forEach(key => {
      this.deleteProperty_(key);
    });
    return this.getAuth();
  }

  /**
   * Saves the explicit authorization.
   * @param {!Object} auth
   * @return {!ExplicitAuthInfo}
   */
  saveAuth(auth) {
    console.log('Save explicit auth', auth);
    const {client_id, client_secret, token} = auth;
    const {refresh_token} = token;
    this.updateProperty_('clientId', client_id);
    this.updateProperty_('clientSecret', client_secret);
    this.updateProperty_('refreshToken', refresh_token);
    this.updateTokens_(token);
    this.updateProperty_('explicitAuthStatus', ExplicitAuthorization.STATUS.OK);
    return this.getAuth();
  }

  /**
   * Returns the access token of the explicit authorization.
   * @return {string}
   */
  getAccessToken() {
    if (Date.now() < this.expiredAt) {
      console.log('Get available explicit access token');
      return this.accessToken;
    }
    this.refreshToken_();
    return this.accessToken;
  }

  /**
   * Returns the OpenID Connect ID token of the explicit authorization.
   * @return {string}
   */
  getIdentityToken() {
    if (Date.now() < this.expiredAt) {
      console.log('Get available explicit Id token');
      return this.idToken;
    }
    this.refreshToken_();
    return this.idToken;
  }

  /**
   * Refreshes tokens and set to user properties.
   * @private
   */
  refreshToken_() {
    const response = this.getResponseOfRefreshedToken_();
    const {access_token, error, error_description} = response;
    if (error || !access_token) {
      this.invalidateAuth_(error_description || error);
      throw new Error('Invalid explicit authorization.');
    }
    this.updateTokens_(response);
  }

  /**
   * Gets the response of refreshed tokens.
   * @return {!Object}
   * @private
   */
  getResponseOfRefreshedToken_() {
    console.log('Try to refresh explicit tokens...');
    const params = {
      method: 'POST',
      contentType: 'application/json',
      muteHttpExceptions: true,
    };
    params.payload = JSON.stringify({
      client_id: this.clientId,
      client_secret: this.clientSecret,
      refresh_token: this.refreshToken,
      grant_type: 'refresh_token',
    });
    const response = UrlFetchApp.fetch(
      ExplicitAuthorization.REFRESH_TOKEN_URL,
      params,
    );
    if (response.error) throw new Error(error);
    return JSON.parse(response.getContentText() || '{}');
  }

  /**
   * Invalidates explicit authentication.
   * @param {string} error
   * @private
   */
  invalidateAuth_(error) {
    console.error('Fail to refresh tokens for explicit auth:', error);
    this.updateProperty_(
      'explicitAuthStatus',
      ExplicitAuthorization.STATUS.INVALID,
    );
  }

  /**
   * Updates explicit authentication tokens.
   * @param {!Object} response
   * @private
   */
  updateTokens_(response) {
    const {access_token, id_token, expires_in} = response;
    // Reduce the new token expire time for 60 seconds for operation safety.
    const newExpiredTime = Date.now() + (expires_in - 60) * 1000;
    console.log('Refresh explicit tokens', new Date(newExpiredTime));
    this.updateProperty_('accessToken', access_token);
    this.updateProperty_('idToken', id_token);
    this.updateProperty_('expiredAt', newExpiredTime);
  }

  /**
   * Updates a user property.
   * @param {string} key
   * @param {string} value
   * @private
   */
  updateProperty_(key, value) {
    this.properties.setProperty(key, value);
    this[key] = value;
  }

  /**
   * Deletes a user property.
   * @param {string} key
   * @private
   */
  deleteProperty_(key) {
    this.properties.deleteProperty(key);
    delete this[key];
  }
}

// ============================================================================
// Global Router Stubs for Apps Script HTML RPC (google.script.run)
// These MUST be declared in the global scope to be callable by the frontend.
// ============================================================================

/**
 * Gets the explicit authorization information.
 * This is invoked by 'explicit_auth.html'.
 * @see '../pages/explicit_auth.html'
 * @return {!ExplicitAuthInfo|null}
 */
const getExplicitAuth = () => {
  return ExplicitAuthorization.activeInstance
    ? ExplicitAuthorization.activeInstance.getAuth()
    : null;
};

/**
 * Deletes the explicit authorization.
 * This is invoked by 'explicit_auth.html'.
 * @see '../pages/explicit_auth.html'
 * @return {!ExplicitAuthInfo|null}
 */
const deleteExplicitAuth = () => {
  return ExplicitAuthorization.activeInstance
    ? ExplicitAuthorization.activeInstance.deleteAuth()
    : null;
};

/**
 * Saves the explicit authorization.
 * This is invoked by 'explicit_auth.html'.
 * @see '../pages/explicit_auth.html'
 * @param {!Object} auth
 * @return {!ExplicitAuthInfo|null}
 */
const saveExplicitAuth = auth => {
  return ExplicitAuthorization.activeInstance
    ? ExplicitAuthorization.activeInstance.saveAuth(auth)
    : null;
};

/**
 * Gets the scopes of Apps Script authorization.
 * This is used to update explicit authorization.
 * @see '../pages/explicit_auth.html'
 * @return {string}
 */
const getExplicitAuthScope = () => {
  const token = ScriptApp.getOAuthToken();
  const response = UrlFetchApp.fetch(
    ExplicitAuthorization.ACCESS_TOKEN_INFO + token,
  );
  const {scope} = JSON.parse(response.getContentText() || '{}');
  console.log('Get Apps Script auth scope', scope);
  return scope;
};

/**
 * Shows the sidebar to manage authorization for the current Sheet.
 * This is the function behind Authorization menu Item.
 */
const showExplicitAuthSidebar = () => {
  if (ExplicitAuthorization.activeInstance) {
    ExplicitAuthorization.activeInstance.showSidebar();
  }
};

// Register explicit auth to ApiBase transparently to decouple explicit UI from
// foundational API transport
if (typeof ApiBase !== 'undefined') {
  ApiBase.setExplicitTokenProvider(() => {
    const instance = ExplicitAuthorization.activeInstance;
    return instance && instance.enabled() ? instance.getAccessToken() : null;
  });
}
