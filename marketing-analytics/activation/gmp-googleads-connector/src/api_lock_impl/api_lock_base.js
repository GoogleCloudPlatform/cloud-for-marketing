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
 * @fileoverview Offers a unified interface to operate API locks.
 */

'use strict';

/** Maximum time (milliseconds) for a process to hold a ApiLock. */
const API_LOCK_TIMEOUT = 10 * 60 * 1000;

/**
 * Tentacles queues data by the API name. Multiple files of the same API will
 * be stored in the same Pub/sub's topic. However they will trigger the sending
 * tasks for multiple times. In this case, concurrent sending-tasks may exceed
 * the QPS limit for the specific API which works on the whole Cloud Project.
 *
 * To solve this risk, the sending-task only works after successfully getting
 * the lock of that API (ApiLock). The sending-task will return the lock after
 * sending data.
 *
 * The 'ApiLock' is a document in Firestore (or entity in Datastore) with the
 * unique key from the name of API. Getting lock will turn the object with the
 * property 'available' as 0, while unlocking changes it back to 1.
 *
 * @typedef {{
 *   available: number,
 *   updatedAt: !Date,
 * }}
 */
let ApiLock;

/**
 * API Locks implementation base class.
 * Without external data source, this base class is a kinda dummy ApiLock
 * implementation which actually doesn't manager the locks. It is useful when
 * there won't be multiple simultaneous files and no Firestore or Datastore is
 * available.
 */
class ApiLockBase {
  /**
   * Returns the object will be updated to current lock for taking the lock.
   * @return {!ApiLock} Locked lock data.
   * @protected
   */
  getLockObject() {
    return {
      available: 0,
      updatedAt: new Date(),
    };
  }

  /**
   * Returns the object will be updated to current lock for returning the lock.
   * @return {!ApiLock} Unlocked lock data.
   * @protected
   */
  getUnlockObject() {
    return {
      available: 1,
      updatedAt: new Date(),
    };
  }

  /**
   * Returns whether the lock is available. There are two cases for a lock to
   * be available:
   * 1. The available number equals one.
   * 2. The lock has been locked longer than the maximum time, which means
   * there is something wrong happened and the last lock holder didn't
   * successfully return the lock. In this case, the lock is also available.
   *
   * @param {number} available Current value of property 'available'.
   * @param {!Date} lockedTime When the lock was locked up latest.
   * @return {boolean} Whether the lock is available.
   * @protected
   */
  isLockAvailable(available, lockedTime) {
    if (available === 1) return true;
    const timePassed = new Date().getTime() - lockedTime.getTime();
    return timePassed >= API_LOCK_TIMEOUT;
  }

  /**
   * Gets the lock of a given Pubsub topic name.
   *
   * @param {string} topicName The name of Pub/sub topic that need to be locked.
   * @return {!Promise<boolean>} Whether successfully gets the lock.
   */
  getLock(topicName) {
    console.log(`[Dummy AppLock] lock: [${topicName}]: OK.`);
    return Promise.resolve(true);
  }

  /**
   * Returns the lock of a given Pubsub topic name.
   *
   * @param {string} topicName The name of Pub/sub topic that need to be locked.
   * @return {!Promise<boolean>} Whether successfully returns the lock.
   */
  unlock(topicName) {
    console.log(`[Dummy AppLock] unlock [${topicName}]: OK.`);
    return Promise.resolve(true);
  }
}

exports.ApiLockBase = ApiLockBase;
