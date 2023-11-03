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

/** @fileoverview OAuth 2.0 scope information. */

/**
 * Definition of APIs that users can grant access through OAuth process.
 * @typedef {{
 *   name: string,
 *   scope: string|!Array<string>,
 *   oauthOnly: boolean|undefined,
 * }}
 */
let OAuthApi;

/**
 * All supported OAuth APIs by Cyborg.
 * @const {!Object<string,!OAuthApi>}
 */
const OAUTH_API_SCOPE = Object.freeze({
  GAds: {
    name: 'Google Ads API',
    scope: 'https://www.googleapis.com/auth/adwords',
    oauthOnly: true,
  },
  GA: {
    name: 'Google Analytics Management API',
    scope: 'https://www.googleapis.com/auth/analytics',
  },
  CM: {
    name: 'Campaign Manager 360 API',
    scope: [
      'https://www.googleapis.com/auth/ddmconversions',
      'https://www.googleapis.com/auth/dfareporting',
      'https://www.googleapis.com/auth/dfatrafficking',
    ],
  },
  SA360: {
    name: 'Search Ads 360 API',
    scope: 'https://www.googleapis.com/auth/doubleclicksearch',
  },
  DV360: {
    name: 'Display & Video 360 API',
    scope: [
      'https://www.googleapis.com/auth/doubleclickbidmanager',
      'https://www.googleapis.com/auth/display-video',
    ],
    oauthOnly: true,
  },
  Sheets: {
    name: 'Google Sheets API',
    scope: 'https://www.googleapis.com/auth/spreadsheets',
  },
  ADH: {
    name: 'Ads Data Hub API',
    scope: 'https://www.googleapis.com/auth/adsdatahub',
  },
  YouTube: {
    name: 'YouTube Reporting API',
    scope: 'https://www.googleapis.com/auth/youtube.force-ssl',
    oauthOnly: true,
  },
  Gmail: {
    name: 'GMail Notification API',
    scope: 'https://www.googleapis.com/auth/gmail.readonly',
    oauthOnly: true,
  }
});

/**
 * The array to hold OAuth scopes. In different solutions, there might be
 * different scopes.
 * @const {!Array<!OAuthApi>}
 */
const DEFAULT_OAUTH_SCOPE = [];

/**
 * Returns the array of OAuth scopes. This function is called by the web page
 * in sidebar to generate the OAuth API list for the user to select.
 * @see '../pages/oauth.html'
 * @return {Array<string>}
 */
const getOAuthScopes = () => {
  return DEFAULT_OAUTH_SCOPE;
};

/**
 * Saves the token as a secret in Secret Manager. This function is called by the
 * web page in sidebar to generate the OAuth API list for the user to select.
 * @see '../pages/oauth.html'
 * @param {string} secretName
 * @param {string} token
 */
const saveOAuthToken = (secretName, token) => {
  const projectId = getDocumentProperty('projectId');
  const secretManager = new SecretManager(projectId);
  secretManager.addSecretVersion(secretName, token);
  MENU_OBJECTS.secretmanagersheet.initialize();
};

/**
 * Shows the sidebar to create OAuth token.
 * This is the function behind OAuth menu Item.
 */
const showOAuthSidebar = () => {
  const html = HtmlService.createHtmlOutputFromFile('pages/oauth')
    .setTitle('OAuth2 token generator')
    .setWidth(300);
  SpreadsheetApp.getUi().showSidebar(html);
};

/**
 * Menu item to show the sidebar for OAuth generation.
 */
const OAUTH_MENUITEM = {
  menuItem: [
    { seperateLine: true },
    {
      name: 'Generate an OAuth token',
      method: 'showOAuthSidebar',
    },
  ],
};
