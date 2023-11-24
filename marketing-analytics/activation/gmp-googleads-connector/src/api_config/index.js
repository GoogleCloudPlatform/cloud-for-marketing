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
 * @fileoverview Offers a unified interface to operate API configurations.
 */

'use strict';

const {firestore: {DataSource}} = require('@google-cloud/nodejs-common');

const {ApiConfig} = require('./api_config.js');
const {ApiConfigJson, ApiConfigOnJson} = require('./api_config_json.js');
const {ApiConfigOnFirestore} = require('./api_config_firestore.js');

/**
 * Returns the ApiConfig object based on the given param.
 *
 * @param {!ApiConfigJson|!DataSource} apiConfigSource Source of the ApiConfig.
 * @param {string|undefined=} namespace
 * @return {!ApiConfig} Api configuration host object.
 * @deprecated
 */
const getApiConfig = (apiConfigSource, namespace = undefined) => {
  switch (apiConfigSource) {
    case DataSource.FIRESTORE:
    case DataSource.DATASTORE:
      return new ApiConfigOnFirestore(apiConfigSource, namespace);
    default:
      if (typeof apiConfigSource === 'object') {
        return new ApiConfigOnJson(apiConfigSource);
      }
      throw new Error(
          `Fail to get the API Config host with ${apiConfigSource}`);
  }
};

module.exports = {
  ApiConfig,
  ApiConfigOnJson,
  getApiConfig,
  ApiConfigOnFirestore,
};
