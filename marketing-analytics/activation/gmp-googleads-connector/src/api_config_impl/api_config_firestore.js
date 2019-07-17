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
 * @fileoverview API configuration based on Firestore Native mode.
 */

'use strict';

const {NativeModeAccess} = require('nodejs-common');
const {ApiConfigHost} = require('./api_config_host.js');

/**
 * ApiConfigHost based on Firestore Native mode.
 *
 * @implements {ApiConfigHost}
 */
class ApiConfigOnFirestore {
  /**
   * Initializes ApiConfigOnFirestore.
   */
  constructor() {
    /**
     * Path prefix for API configuration collections. This prefix combines the
     * API name making the full collection path for all this API configurations.
     * @const {string}
     */
    this.pathPrefix = 'tentacles/ApiConfig';
    console.log('Init ApiConfig based on Firestore.');
  }

  /**
   * Gets the Firestore Native mode access instance for the given API name.
   * @param {string} apiName API name.
   * @return {!NativeModeAccess}
   * @private
   */
  getFirestoreInstance_(apiName) {
    return new NativeModeAccess(`${this.pathPrefix}/${apiName}`);
  }

  /** @override */
  getConfig(apiName, configName) {
    return this.getFirestoreInstance_(apiName).getObject(configName);
  }

  /** @override */
  saveConfig(apiName, configName, configObject) {
    return this.getFirestoreInstance_(apiName).saveObject(
        configObject, configName);
  }

  /** @override */
  deleteConfig(apiName, configName) {
    return this.getFirestoreInstance_(apiName).deleteObject(configName);
  }
}

exports.ApiConfigOnFirestore = ApiConfigOnFirestore;
