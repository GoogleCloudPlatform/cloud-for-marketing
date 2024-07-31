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

/** The url for get a new access token from a refresh token. */
const REFRESH_TOKEN_URL = 'https://accounts.google.com/o/oauth2/token';
/**
 * The url for get the information of an access token.
 * This is used to get the Apps Script authorization scope.
 */
const ACCESS_TOKEN_INFO = 'https://www.googleapis.com/oauth2/v3/tokeninfo?access_token=';

/** Status of the explicit authorization. */
const EXPLICIT_AUTH_STATUS = {
  OK: 'OK',
  INVALID: 'INVALID', // Invalid token, failed to refresh, etc.
};

/** Property names of explicit authorization. */
const EXPLICIT_AUTH_PROPERTIES = [
  'explicitAuthStatus',
  'clientId',
  'clientSecret',
  'refreshToken',
  'accessToken',
  'expiredAt',
];

/**
 * @typedef {{
 *   explicitAuthStatus: string|undefined,
 *   clientId: string|undefined,
 *   clientSecret: string|undefined,
 *   refreshToken: string|undefined,
 * }}
 */
let ExplicitAuthInfo;

/**
 * The class to manage the explicit authorization.
 * There is a known issue that the Apps Script project can not be updated to a
 * GCP project which is owned by the organization other than the one that owns
 * the Sheets. To solve this issue, an explicit authorzation can be applied in
 * the Sheets to support using a GCP from other organization.
 */
class ExplicitAuthorization {

  /**
   * Loads explicit authorization properties.
   * `UserProperties` is used to limit the authorization information to be
   * accessible only to the current user and only within this script.
   * @see https://developers.google.com/apps-script/reference/properties/properties-service#getUserProperties()
   * @constructor
   */
  constructor() {
    console.log('Init explicit authorization instance');
    this.properties = PropertiesService.getUserProperties();
    EXPLICIT_AUTH_PROPERTIES.forEach((key) => {
      this[key] = this.properties.getProperty(key);
    });
  }

  /**
   * Returns whether explicit authorization is enabled.
   * It will throw an error if the status is 'INVALID'.
   * @return {boolean}
   */
  enabled() {
    if (this.explicitAuthStatus === EXPLICIT_AUTH_STATUS.INVALID) {
      throw new Error('Invalid explicit authorization.');
    };
    return this.explicitAuthStatus === EXPLICIT_AUTH_STATUS.OK;
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
    EXPLICIT_AUTH_PROPERTIES.forEach((key) => {
      this.deleteProperty_(key);
    });
    return this.getAuth();
  }

  /**
   * Saves the explicit authorization.
   * @return {!ExplicitAuthInfo}
   */
  saveAuth(auth) {
    console.log('Save explicit auth', auth);
    const { client_id, client_secret, token } = auth;
    const { refresh_token, access_token, expires_in } = token;
    this.updateProperty_('clientId', client_id);
    this.updateProperty_('clientSecret', client_secret);
    this.updateProperty_('refreshToken', refresh_token);
    this.updateAccessToken_(access_token, expires_in);
    this.updateProperty_('explicitAuthStatus', EXPLICIT_AUTH_STATUS.OK);
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
    const { access_token, expires_in, error, error_description }
      = this.getResponseOfAccessToken_();
    if (error || !access_token) {
      this.invalidateAuth_(error_description || error);
      throw new Error('Invalid explicit authorization.');
    } else {
      this.updateAccessToken_(access_token, expires_in);
      return this.accessToken;
    }
  }

  /** Gets the response of refeshing for an access token. */
  getResponseOfAccessToken_() {
    console.log('Try to refresh explicit access token...');
    const params = {
      method: 'POST',
      contentType: 'application/json',
      muteHttpExceptions: true,
    };
    params.payload = {
      client_id: this.clientId,
      client_secret: this.clientSecret,
      refresh_token: this.refreshToken,
      grant_type: 'refresh_token',
    };
    const response = UrlFetchApp.fetch(REFRESH_TOKEN_URL, params);
    return JSON.parse(response.getContentText() || '{}');
  }

  invalidateAuth_(error) {
    console.error(
      'Fail to refresh access token for explicit auth:', error);
    this.updateProperty_('explicitAuthStatus', EXPLICIT_AUTH_STATUS.INVALID);
  }

  updateAccessToken_(accessToken, expiresIn) {
    // Reduce the new token expire time for 60 seconds for operation safety.
    const newExpiredTime = Date.now() + (expiresIn - 60) * 1000;
    console.log('Refresh explicit access token', new Date(newExpiredTime));
    this.updateProperty_('accessToken', accessToken);
    this.updateProperty_('expiredAt', newExpiredTime);
  }

  updateProperty_(key, value) {
    this.properties.setProperty(key, value);
    this[key] = value;
  }

  deleteProperty_(key) {
    this.properties.deleteProperty(key);
    delete this[key];
  }
}

/** The instance of ExplicitAuthorization for this script. */
const EXPLICIT_AUTH = new ExplicitAuthorization();

/**
 * Gets the explicit authorization information.
 * This is invoked by 'authorization.html'.
 * @see '../pages/authorization.html'
 */
const getExplicitAuth = () => {
  return EXPLICIT_AUTH.getAuth();
};

/**
 * Deletes the explicit authorization.
 * This is invoked by 'authorization.html'.
 * @see '../pages/authorization.html'
 */
const deleteExplicitAuth = () => {
  return EXPLICIT_AUTH.deleteAuth();
};

/**
 * Saves the explicit authorization.
 * This is invoked by 'authorization.html'.
 * @see '../pages/authorization.html'
 */
const saveExplicitAuth = (auth) => {
  return EXPLICIT_AUTH.saveAuth(auth);
};

/**
 * Gets the scopes of Apps Script authorization.
 * This is used to update explicit authorization.
 * @see '../pages/authorization.html'
 * @return {string}
 */
const getExplicitAuthScope = () => {
  const token = ScriptApp.getOAuthToken();
  const response = UrlFetchApp.fetch(ACCESS_TOKEN_INFO + token);
  const { scope } = JSON.parse(response.getContentText() || '{}');
  console.log('Get Apps Script auth scope', scope);
  return scope;
};

/**
 * Shows the sidebar to manage authorization for the current Sheet.
 * This is the function behind Authorization menu Item.
 */
const showExplicitAuthSidebar = () => {
  const html = HtmlService.createHtmlOutputFromFile('pages/explicit_auth')
    .setTitle('Explicit Authorization')
    .setWidth(300);
  SpreadsheetApp.getUi().showSidebar(html);
};

/**
 * Menu item to show the sidebar for explicit authorization.
 */
const EXPLICIT_AUTH_MENUITEM = {
  menuItem: [
    { separator: true },
    {
      name: 'Explicit authorization',
      method: 'showExplicitAuthSidebar',
    },
  ],
};
