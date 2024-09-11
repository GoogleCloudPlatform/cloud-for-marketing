// Copyright 2021 Google Inc.
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
 * @fileoverview Youtube API Client Library.
 */

'use strict';

const {google} = require('googleapis');
const { GoogleApiClient } = require('./base/google_api_client.js');
const {
  Schema$Channel,
  Schema$Video,
  Schema$CommentThread,
  Schema$Playlist,
  Schema$Search,
} = google.youtube;
const { getLogger } = require('../components/utils.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/youtube.force-ssl'
]);
const API_VERSION = 'v3';

/**
 * Youtube API v3 stub.
 * See: https://developers.google.com/youtube/v3/docs
 */
class YouTube extends GoogleApiClient {
  /**
   * @constructor
   * @param {!Object<string,string>=} env The environment object to hold env
   *     variables.
   */
  constructor(env = process.env) {
    super(env);
    this.googleApi = 'youtube';
    this.logger = getLogger('API.YT');
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
  YouTube,
  API_VERSION,
  API_SCOPES,
};
