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
 * @fileoverview Tentacles API lock empty implementation.
 */

'use strict';

const {ApiLock} = require('./api_lock.js');

/**
 * Tentacles API Locks implementation with no external data source.
 * Without external data source, this implementation can't manager locks really.
 * It is useful when there won't be multiple simultaneous files and no Firestore
 * or Datastore is available.
 * @implements {ApiLock}
 */
class ApiLockDummy {

  /** @override */
  async getLock(lockId, token) {
    console.log(`[Dummy AppLock] lock [${lockId}] token[${token}]: OK.`);
    return Promise.resolve(true);
  }

  /** @override */
  async unlock(lockId, token) {
    console.log(`[Dummy AppLock] unlock [${lockId}] token[${token}]: OK.`);
    return Promise.resolve(true);
  }

  /** @override */
  async hasAvailableLock(lockId) {
    console.log(`[Dummy AppLock] has available lock [${lockId}]: NO.`);
    return Promise.resolve(false);
  }
}

module.exports = {ApiLockDummy};
