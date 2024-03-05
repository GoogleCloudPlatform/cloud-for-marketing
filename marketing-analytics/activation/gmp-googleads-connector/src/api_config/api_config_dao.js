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
const { ApiConfigItem } = require('../api_handlers/index.js');

/**
 * ApiConfig data access object.
 *
 */
class ApiConfigDao extends DataAccessObject {

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

  /**
   * Gets a configuration for a given API and configuration name.
   * @param {string} apiName API name.
   * @param {string} configName Configuration name.
   * @return {!Promise<!ApiConfigItem>} Configuration for the given API name and
   *     configuration name.
   */
  getConfig(apiName, configName) {
    return this.load(this.getId_(apiName, configName));
  }

  /**
   * Saves a configuration based on a given API and configuration name.
   * @param {string} apiName API name.
   * @param {string} configName Configuration name.
   * @param {!ApiConfigItem} configObject Configuration content in JSON format.
   * @return {!Promise<boolean>} Whether this operation succeeded or not.
   */
  async saveConfig(apiName, configName, configObject) {
    await this.update(configObject, this.getId_(apiName, configName));
    return true;
  }

  /**
   * Deletes a configuration based on a given API and configuration name.
   * @param {string} apiName API name.
   * @param {string} configName Configuration name.
   * @return {!Promise<boolean>} Whether this operation succeeded or not.
   */
  deleteConfig(apiName, configName) {
    return this.remove(this.getId_(apiName, configName));
  }
}

module.exports = { ApiConfigDao };
