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

/**
 * @fileoverview A base class for Google Api client library class.
 */
const { google } = require('googleapis');
const { getLogger } = require('../../components/utils.js');
const { AuthRestfulApi } = require('./auth_restful_api.js');

/**
 * A Google Api client library class.
 */
class GoogleApiClient extends AuthRestfulApi {

  /** @constructor */
  constructor(env = process.env, options = {}) {
    super(env, options);
    this.logger = getLogger('API.default');
  }

  /**
   * Returns the Api version of the Api in the current library.
   * @return {string}
   * @abstract
   */
  getVersion() { }

  /**
   * Gets the default options to initialize an Api object in Google Api client
   * library. It contains the Api version and auth information.
   * @param {object|undefined} initOptions
   * @return {object}
   */
  async getApiClientInitOptions(initOptions) {
    return {
      version: this.getVersion(),
      auth: await this.getAuth(),
    };
  }

  /**
   * Returns the Api instance.
   * This function expects an instance name for the object in Google Api client
   * library, e.g. searchads360 for 'Search Ads 360'.
   * @return {!Promise<object>} The Api instance.
   */
  async getApiClient(initOptions = {}) {
    if (this.apiClient) return this.apiClient;
    this.logger.info(`Initialized ${this.constructor.name} instance.`);
    const options = await this.getApiClientInitOptions(initOptions);
    this.apiClient = google[this.googleApi](options);
    return this.apiClient;
  }
}

module.exports = { GoogleApiClient };
