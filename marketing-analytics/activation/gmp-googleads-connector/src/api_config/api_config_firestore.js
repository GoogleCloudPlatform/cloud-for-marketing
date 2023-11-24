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
 * @fileoverview API configuration implementation class which is based on
 *     Firestore (native mode or Datastore mode).
 */

'use strict';

const { firestore: { Database, DataAccessObject } }
  = require('@google-cloud/nodejs-common');
const {ApiConfig} = require('./api_config.js');

/**
 * ApiConfig based on Firestore Native mode.
 *
 * @implements {ApiConfig}
 */
class ApiConfigOnFirestore extends DataAccessObject {

  /**
   * Initializes ApiConfig Dao instance.
   * @param {!Database} database The database.
   * @param {string} namespace The namespace of the data.
   */
  constructor(database, namespace = 'tentacles') {
    super('ApiConfig', namespace, database);
  }

  /**
   * Returns the id of the document or entity based on given API name and
   * configuration name. The naming convention is `API.config`.
   * @param {string} apiName API name.
   * @param {string} configName Configuration name.
   * @return {string} The document or entity id based on apiName and configName.
   * @private
   */
  getId_(apiName, configName) {
    return `${apiName}.${configName}`;
  }

  /** @override */
  getConfig(apiName, configName) {
    return this.load(this.getId_(apiName, configName));
  }

  /** @override */
  async saveConfig(apiName, configName, configObject) {
    await this.update(configObject, this.getId_(apiName, configName));
    return true;
  }

  /** @override */
  deleteConfig(apiName, configName) {
    return this.remove(this.getId_(apiName, configName));
  }
}

exports.ApiConfigOnFirestore = ApiConfigOnFirestore;
