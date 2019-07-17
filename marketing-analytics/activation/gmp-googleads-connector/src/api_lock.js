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
 * @fileoverview Offers a factory function to get an instance of ApiLockBase.
 */

'use strict';

const {FirestoreAccessBase: {DataSource}} = require('nodejs-common');
const {ApiLockBase} = require('./api_lock_impl/api_lock_base.js');
const {ApiLockOnDatastore} = require('./api_lock_impl/api_lock_datastore.js');
const {ApiLockOnFirestore} = require('./api_lock_impl/api_lock_firestore.js');

exports.ApiLockBase = ApiLockBase;

/**
 * Returns the ApiLockBase object based on the given dataSource type.
 * @param {!DataSource|undefined=} dataSource
 * @return {!ApiLockBase}
 */
exports.getApiLock = (dataSource = undefined) => {
  switch (dataSource) {
    case DataSource.FIRESTORE:
      return new ApiLockOnFirestore();
    case DataSource.DATASTORE:
      return new ApiLockOnDatastore();
    default:
      console.log(`Unknown dataSource[${dataSource}] for Lock. Use dummy one.`);
      return new ApiLockBase();
  }
};
