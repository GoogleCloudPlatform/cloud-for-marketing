// Copyright 2019 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this fileAccessObject except in compliance with the License.
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
 * @fileoverview Interface for operattions of API configurations.
 */

'use strict';

/**
 * Tentacles supports using a single JSON file or Firestore/Datastore as the
 * data source for API configurations. Regardless the difference of sources,
 * this class offers a unified interface here to offer the operations of API
 * configurations.
 * 1. For all of those data sources, 'get' returns the configuration based on
 * the given API and configuration name.
 * 2. For Firestore/Datastore, corresponding classes also implement 'save' and
 * 'delete' functions to save the configuration from a JSON file to backend
 * Firestore/Datastore or delete the configuration.
 * @interface
 */
class ApiConfigHost {
  /**
   * Gets a configuration for a given API and configuration name.
   * @param {string} apiName API name.
   * @param {string} configName Configuration name.
   * @return {!Promise<!ApiConfigItem>} Configuration for the given API name and
   *     configuration name.
   */
  getConfig(apiName, configName) {}

  /**
   * Saves a configuration based on a given API and configuration name.
   * @param {string} apiName API name.
   * @param {string} configName Configuration name.
   * @param {!ApiConfigItem} configObject Configuration content in JSON format.
   * @return {!Promise<boolean>} Whether this operation succeeded or not.
   */
  saveConfig(apiName, configName, configObject) {}

  /**
   * Deletes a configuration based on a given API and configuration name.
   * @param {string} apiName API name.
   * @param {string} configName Configuration name.
   * @return {!Promise<boolean>} Whether this operation succeeded or not.
   */
  deleteConfig(apiName, configName) {}
}

exports.ApiConfigHost = ApiConfigHost;
