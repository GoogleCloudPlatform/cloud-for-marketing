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
 * @fileoverview Tentacles API lock implementation class which is based on
 *     Firestore (native mode or Datastore mode).
 */

'use strict';

const {
  firestore: {TransactionOperation, DataAccessObject},
  utils: {getLogger},
} = require('nodejs-common');
const {ApiLock} = require('./api_lock.js');

/**
 * The 'ApiLock' is a document in Firestore (or entity in Datastore) with the
 * unique key from the name of API. Getting lock will turn the object with the
 * property 'available' as 0, while unlocking changes it back to 1.
 *
 * @typedef {{
 *   available: number,
 *   updatedAt: number,
 * }}
 */
let ApiLockObject;

/**
 * Tentacles API Locks implementation base on Firestore.
 * Transaction is required to fulfill the lock.
 * @implements {ApiLock}
 */
class ApiLockDao extends DataAccessObject {

  constructor(dataSource, namespace = 'tentacles') {
    super('Lock', namespace, dataSource);
    /**
     * Maximum time (milliseconds) for a process to hold an ApiLock.
     * @type {number}
     */
    this.maxTimeForLocks = 10 * 60 * 1000;
    this.logger = getLogger(`LOCK.${dataSource}`);
  }

  /** @override */
  getLock(topicName) {
    return this.runTransaction(
        this.wrapInTransaction(topicName, this.getLockOperation(topicName)))
        .catch((error) => {
          console.log(`Error in getLock ${topicName}. Reason:`, error);
          return false;
        });
  }

  /** @override */
  unlock(topicName) {
    return this.runTransaction(
        this.wrapInTransaction(topicName, this.getUnlockOperation(topicName)))
        .catch((error) => {
          console.log(`Error in unlock ${topicName}. Reason:`, error);
          return false;
        });
  }

  /**
   * Get the operation that gets a lock in a transaction. It returns true when
   * 1. There is no such lock. Or
   * 2. The lock is available;
   * It will change lock to unavailable to get the lock.
   *
   * @param {string} topicName
   * @return {!TransactionOperation}
   * @private
   */
  getLockOperation(topicName) {
    return (documentSnapshot, documentReference, transaction) => {
      if (!documentSnapshot.exists) {
        console.log(`Create the lock for [${topicName}].`);
        transaction.create(documentReference, this.getLockObject());
        return true;
      }
      if (!this.isLockAvailable(
          documentSnapshot.get('available'),
          documentSnapshot.get('updatedAt'))) {
        console.log(`There is no available Lock for [${topicName}].`);
        return false;
      }
      transaction.update(documentReference, this.getLockObject(),
          {updatedAt: documentSnapshot.updatedAt});
      this.logger.debug(`Get lock of ${topicName} successfully.`);
      return true;
    };
  }

  /**
   * Releases the lock. It returns false when there is already available lock.
   * Otherwise, it will create the available lock (if it doesn't exist) or
   * update the lock to available status.
   *
   * @param {string} topicName
   * @return {!TransactionOperation}
   * @private
   */
  getUnlockOperation(topicName) {
    return (documentSnapshot, documentReference, transaction) => {
      if (!documentSnapshot.exists) {
        console.log(`The lock for [${topicName}] doesn't exist. Create now.`);
        transaction.create(documentReference, this.getUnlockObject());
        return true;
      }
      if (documentSnapshot.get('available') === 1) {
        console.log(`There is an available lock for [${topicName}].`);
        return false;
      }
      this.logger.debug(`${topicName} has lock, try to unlock.`);
      transaction.update(documentReference, this.getUnlockObject(),
          {updatedAt: documentSnapshot.updatedAt});
      this.logger.debug(`${topicName} unlock successfully.`);
      return true;
    };
  }

  /**
   * Returns the object to be updated to current lock for taking the lock.
   * @return {!ApiLockObject} Locked lock data.
   * @private
   */
  getLockObject() {
    return {
      available: 0,
      updatedAt: Date.now(),
    };
  }

  /**
   * Returns the object will be updated to current lock for returning the lock.
   * @return {!ApiLockObject} Unlocked lock data.
   * @private
   */
  getUnlockObject() {
    return {
      available: 1,
      updatedAt: Date.now(),
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
   * @param {number} lockedTime Locked time value in milliseconds.
   * @return {boolean} Whether the lock is available.
   * @private
   */
  isLockAvailable(available, lockedTime) {
    if (available === 1) return true;
    const timePassed = Date.now() - lockedTime;
    return timePassed >= this.maxTimeForLocks;
  }
}

module.exports = {ApiLockDao};
