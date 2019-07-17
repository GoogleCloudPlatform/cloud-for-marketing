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

const {FirestoreAccessBase: {DataSource, isNativeMode}} =
    require('nodejs-common');

const {ApiConfigHost} = require('./api_config_impl/api_config_host.js');
const {ApiConfigJson, ApiConfigOnJson} =
    require('./api_config_impl/api_config_json.js');
const {ApiConfigOnFirestore} =
    require('./api_config_impl/api_config_firestore.js');
const {ApiConfigOnDatastore} =
    require('./api_config_impl/api_config_datastore.js');

exports.ApiConfigJson = ApiConfigJson;
exports.ApiConfigHost = ApiConfigHost;

/**
 * Returns the ApiConfigHost object based on the given param.
 *
 * @param {!ApiConfigJson|!DataSource} apiConfig Source of the ApiConfigHost.
 * @return {!ApiConfigHost} Api configuration host object.
 */
const getApiConfig = (apiConfig) => {
  switch (apiConfig) {
    case DataSource.FIRESTORE:
      return new ApiConfigOnFirestore();
    case DataSource.DATASTORE:
      return new ApiConfigOnDatastore();
    default:
      if (typeof apiConfig === 'object') return new ApiConfigOnJson(apiConfig);
      throw new Error(`Fail to get the API Config host with ${apiConfig}`);
  }
};

exports.getApiConfig = getApiConfig;

/**
 * Probes the Google Cloud Project's Firestore mode (Native or Datastore), then
 * uses it to create an object implemented ApiConfigHost.
 *
 * @return {!Promise<!ApiConfigHost>} A promise to return the Api configuration
 *     host object.
 */
exports.guessApiConfig = () => {
  return isNativeMode()
      .then((nativeMode) => {
        return (nativeMode) ? DataSource.FIRESTORE : DataSource.DATASTORE;
      })
      .then(getApiConfig);
};
