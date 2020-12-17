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
   * @param {string} topicName The name of Pub/sub topic that need to be locked.
   * @return {!Promise<boolean>} Whether successfully gets the lock.
   * @abstract
   */
  getLock(topicName) {}

  /**
   * Returns the lock of a given Pubsub topic name.
   *
   * @param {string} topicName The name of Pub/sub topic that need to be locked.
   * @return {!Promise<boolean>} Whether successfully returns the lock.
   * @abstract
   */
  unlock(topicName) {}
}

module.exports = {ApiLock};
