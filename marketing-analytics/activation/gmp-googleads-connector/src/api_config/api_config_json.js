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
 * @fileoverview API configuration based on a JSON object.
 */

'use strict';

const {ApiConfigItem} = require('../api_handlers/index.js');
const {ApiConfig} = require('./api_config.js');

/** @typedef {{string:!ApiConfigItem}|undefined} */
let ApiConfigJsonItem;

/**
 * The JSON objects contains different API configurations. It's organized in
 * API name then configuration name levels. See 'config_api.json.template' for
 * example.
 *
 * @typedef {{
 *   GA:!ApiConfigJsonItem,
 *   MP:!ApiConfigJsonItem,
 *   CM:!ApiConfigJsonItem,
 *   SFTP:!ApiConfigJsonItem,
 *   GS:!ApiConfigJsonItem,
 *   SA:!ApiConfigJsonItem,
 *   ACLC:!ApiConfigJsonItem,
 *   ACM:!ApiConfigJsonItem,
 * }}
 */
let ApiConfigJson;

/**
 * ApiConfig based on an 'ApiConfigJson' object.
 * @implements {ApiConfig}
 */
class ApiConfigOnJson {
  /**
   * Initializes ApiConfigOnJson.
   * @param {!ApiConfigJson} apiConfig Api configuration Json.
   */
  constructor(apiConfig) {
    this.apiConfig = apiConfig;
    console.log('Init ApiConfig based on a given JSON object.');
  }

  /** @override */
  getConfig(apiName, configName) {
    return Promise.resolve(
        this.apiConfig[apiName] ? this.apiConfig[apiName][configName]
            : this.apiConfig[apiName]);
  }

  /** @override */
  saveConfig(apiName, configName, configObject) {
    return Promise.reject(
        `Save [${apiName}.${configName}] is unsupported for ApiConfigOnJson.`);
  }

  /** @override */
  deleteConfig(apiName, configName) {
    return Promise.reject(
        `Delete [${apiName}.${configName}] is unsupported for ApiConfigOnJson.`);
  }
}

module.exports = {
  ApiConfigJson,
  ApiConfigOnJson,
};
