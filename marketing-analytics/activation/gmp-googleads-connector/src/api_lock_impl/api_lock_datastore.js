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
 * @fileoverview API locks based on Firestore Native mode.
 */

'use strict';

const {
  FirestoreAccessBase: {Transaction},
  DatastoreModeAccess,
  utils: {getLogger},
} = require('nodejs-common');
const {ApiLockBase} = require('./api_lock_base.js');

/**
 * ApiLock based on Datastore.
 */
class ApiLockOnDatastore extends ApiLockBase {
  /**
   * Initializes ApiLockOnDatastore.
   * @param {string=} logTag A tag for log.
   */
  constructor(logTag = '') {
    super();
    /**
     * Instance of Firestore to operate API Lock on Datastore mode.
     * @type {!DatastoreModeAccess}
     */
    this.lock = new DatastoreModeAccess('tentacles', 'Lock');
    this.logger = getLogger(`LOCK.DS.${logTag}`);
  }

  /** @override */
  getLock(topicName) {
    /**
     * Gets the API lock in a given transaction context.
     * @param {!Transaction} transaction Transaction object which passed to a
     *     transaction's updateFunction provides the methods to read and write
     *     data within the transaction context.
     * @return {!Promise<boolean>} Whether successfully gets the lock.
     */
    const getLockInTransaction = (transaction) => {
      this.logger.debug(`Start to get Lock for ${topicName}`);
      const key = this.lock.getKey(topicName);
      return transaction.get(key).then(([lock]) => {
        /**
         * Whether there could be an available lock. In the cases either the
         * lock doesn't exist or the existent lock is already timed out, it is
         * true; otherwise it is false.
         * @type {boolean}
         */
        const existsAvailableLock =
            !lock || this.isLockAvailable(lock.available, lock.updatedAt);
        if (existsAvailableLock) {
          transaction.save([{key: key, data: super.getLockObject()}]);
        } else {
          // Transaction in Datastore needs 'commit' to quit properly. So here
          // can't return directly.
          console.log(`There is no available Lock for [${topicName}].`);
        }
        return transaction.commit()
            .then(() => {
              this.logger.debug(`Get lock of ${topicName} successfully.`);
              return existsAvailableLock;
            })
            .catch((error) => {
              console.log(
                  `Error in getting the lock [${topicName}]. Reason:`,
                  error.message);
              return false;
            });
      });
    };
    return this.lock.runTransaction(getLockInTransaction);
  }

  /** @override */
  unlock(topicName) {
    const unlockInTransaction = (transaction) => {
      this.logger.debug(`Start to return Lock for ${topicName}`);
      const key = this.lock.getKey(topicName);
      return transaction.get(key).then(([lock]) => {
        /**
         * There is no available lock, hence 'unlock' needs to be carried out.
         * @type {boolean}
         */
        const needsUnlock = lock.available !== 1;
        if (needsUnlock) {
          this.logger.debug(`${topicName} has lock, try to unlock.`);
          transaction.save([{key: key, data: super.getUnlockObject()}]);
        } else {
          console.log(`There is an available lock for [${topicName}].`);
        }
        return transaction.commit()
            .then(() => {
              this.logger.debug(`${topicName} unlock successfully.`);
              return needsUnlock;
            })
            .catch((error) => {
              console.log(
                  `Error in unlocking [${topicName}]. Reason:`, error.message);
              return false;
            });
      });
    };
    return this.lock.runTransaction(unlockInTransaction);
  }
}

exports.ApiLockOnDatastore = ApiLockOnDatastore;
