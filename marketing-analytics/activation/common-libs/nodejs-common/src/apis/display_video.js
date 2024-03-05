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
 * @fileoverview Google DoubleClick Search Ads Conversions uploading on Google
 * API Client Library.
 */

'use strict';

const { google } = require('googleapis');
const AuthClient = require('./auth_client.js');
const {
  getLogger,
  getObjectByPath,
  SendSingleBatch,
  BatchResult,
} = require('../components/utils.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/display-video',
]);
const API_VERSION = 'v3';

/**
 * Display and Video 360 API v3 stub.
 * @see https://developers.google.com/display-video/api/reference/rest/v3
 * This is not the same to Reports Display & Video 360 API which is from Google
 * Bid Manager API.
 * @see https://developers.google.com/bid-manager/reference/rest
 */
class DisplayVideo {

  /**
   * @constructor
   * @param {!Object<string,string>=} env The environment object to hold env
   *     variables.
   */
  constructor(env = process.env) {
    this.authClient = new AuthClient(API_SCOPES, env);
    this.logger = getLogger('API.DV3API');
  }

  /**
   * Prepares the Google DV3 instance.
   * @return {!google.displayvideo}
   * @private
   */
  async getApiClient_() {
    if (this.displayvideo) return this.displayvideo;
    this.logger.debug(`Initialized ${this.constructor.name} instance.`);
    this.displayvideo = google.displayvideo({
      version: API_VERSION,
      auth: await this.getAuth_(),
    });
    return this.displayvideo;
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
   * Gets the instance of function object based on Google API client library.
   * @param {string|undefined} path
   * @return {Object}
   */
  async getFunctionObject(path) {
    const instance = await this.getApiClient_();
    return getObjectByPath(instance, path);
  }

}

module.exports = {
  DisplayVideo,
  API_VERSION,
  API_SCOPES,
};
