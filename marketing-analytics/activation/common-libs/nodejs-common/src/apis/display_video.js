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

const { GoogleApiClient } = require('./base/google_api_client.js');
const { getLogger } = require('../components/utils.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/display-video',
]);
const API_VERSION = 'v4';

/**
 * Display and Video 360 API v4 stub.
 * @see https://developers.google.com/display-video/api/reference/rest/v4
 * This is not the same to Reports Display & Video 360 API which is from Google
 * Bid Manager API.
 * @see https://developers.google.com/bid-manager/reference/rest
 */
class DisplayVideo extends GoogleApiClient {

  /**
   * @constructor
   * @param {!Object<string,string>=} env The environment object to hold env
   *     variables.
   */
  constructor(env = process.env) {
    super(env);
    this.googleApi = 'displayvideo';
    this.logger = getLogger('API.DV3API');
  }

  /** @override */
  getScope() {
    return API_SCOPES;
  }

  /** @override */
  getVersion() {
    return API_VERSION;
  }

}

module.exports = {
  DisplayVideo,
  API_VERSION,
  API_SCOPES,
};
