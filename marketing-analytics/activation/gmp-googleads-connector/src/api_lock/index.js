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
 * @fileoverview Offers a factory function for ApiLock.
 */

'use strict';

const {firestore: {DataSource}} = require('@google-cloud/nodejs-common');
const {ApiLock} = require('./api_lock.js');
const {ApiLockDummy} = require('./api_lock_dummy.js');
const {ApiLockDao} = require('./api_lock_dao.js');

/**
 * Returns the ApiLock object based on the given dataSource type.
 * @param {!DataSource|undefined=} dataSource
 * @param {string|undefined=} namespace
 * @return {!ApiLock}
 * @deprecated
 */
const getApiLock = (dataSource = undefined, namespace = undefined) => {
  switch (dataSource) {
    case DataSource.FIRESTORE:
    case DataSource.DATASTORE:
      return new ApiLockDao(dataSource, namespace);
    default:
      console.log(`Unknown dataSource[${dataSource}] for Lock. Use dummy one.`);
      return new ApiLockDummy();
  }
};

module.exports = {
  ApiLock,
  getApiLock,
  ApiLockDao,
};
