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
 * @fileoverview API configuration based on Firestore Datastore mode.
 */

'use strict';

const {DatastoreModeAccess} = require('nodejs-common');
const {ApiConfigHost} = require('./api_config_host.js');

/**
 * ApiConfigHost based on Firestore Datastore mode..
 *
 * @implements {ApiConfigHost}
 */
class ApiConfigOnDatastore {
  /**
   * Initializes ApiConfigOnDatastore.
   */
  constructor() {
    /**
     * Instance of Firestore to operate API configurations on Datastore mode.
     * @const {!DatastoreModeAccess}
     */
    this.apiConfigs = new DatastoreModeAccess('tentacles', 'ApiConfig');
    console.log('Init ApiConfig based on Datastore.');
  }

  /**
   * Returns the id of the Datastore entity based on given API name and
   * configuration name. The naming convention is `API.config`.
   * @param {string} apiName API name.
   * @param {string} configName Configuration name.
   * @return {string} The entity id based on apiName and configName.
   * @private
   */
  getId_(apiName, configName) {
    return `${apiName}.${configName}`;
  }

  /** @override */
  getConfig(apiName, configName) {
    return this.apiConfigs.getObject(this.getId_(apiName, configName));
  }

  /** @override */
  saveConfig(apiName, configName, configObject) {
    return this.apiConfigs.saveObject(configObject,
        this.getId_(apiName, configName));
  }

  /** @override */
  deleteConfig(apiName, configName) {
    return this.apiConfigs.deleteObject(this.getId_(apiName, configName));
  }
}

exports.ApiConfigOnDatastore = ApiConfigOnDatastore;
