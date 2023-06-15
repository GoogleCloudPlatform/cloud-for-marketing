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
 * @fileoverview Interface for operations of API lock.
 */

'use strict';

/**
 * Tentacles queues data by the API name. Multiple files with the same API will
 * be stored in the same Pub/sub's topic. However, multiple files will also
 * trigger multiple times 'sending task'. Hence, it may result exceeding the QPS
 * limit for the specific API.
 *
 * To reduce this risk, 'sending task' only works after successfully getting the
 * lock of that API. 'Sending task' will return the lock after sending data.
 *
 * @interface
 */
class ApiLock {
  /**
   * Gets the lock of a given Pubsub topic name.
   *
   * @param {string} lockId The name of Pub/sub topic is used as lock Id.
   * @param {string} token The token of a lock. A token stands for an instance
   *   of a lock. It is used to support multiple locks.
   * @return {!Promise<boolean>} Whether successfully gets the lock.
   * @abstract
   */
  async getLock(lockId, token) { }

  /**
   * Returns the lock of a given Pubsub topic name.
   *
   * @param {string} lockId The name of Pub/sub topic is used as lock Id.
   * @param {string} token The token of a lock. A token stands for an instance
   *   of a lock. It is used to support multiple locks.
   * @return {!Promise<boolean>} Whether successfully returns the lock.
   * @abstract
   */
  async unlock(lockId, token) { }

  /**
   * Returns whethere there is available locks. This is a direct check and does
   * not guarantee the lock will be available when try to get it. It is used
   * to ramp up sending instance when tasks are just started.
   *
   * @param {string} lockId The name of Pub/sub topic is used as lock Id.
   * @return {!Promise<boolean>} Whether there is an available lock.
   * @abstract
   */
  async hasAvailableLock(lockId) { }
}

module.exports = {ApiLock};
