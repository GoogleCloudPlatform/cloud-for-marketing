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
  NativeModeAccess,
  utils: {getLogger},
} = require('nodejs-common');
const {ApiLockBase} = require('./api_lock_base.js');

/**
 * ApiLock based on Firestore.
 */
class ApiLockOnFirestore extends ApiLockBase {
  /**
   * Initializes ApiLockOnFirestore.
   * @param {string=} logTag A tag for log.
   */
  constructor(logTag = '') {
    super();
    /**
     * Instance of Firestore to operate API Lock on Native mode.
     * @type {!NativeModeAccess}
     */
    this.lock = new NativeModeAccess('tentacles/database/Lock');
    this.logger = getLogger(`LOCK.FS.${logTag}`);
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
      let documentReference = this.lock.getDocumentReference(topicName);
      return transaction.get(documentReference)
          .then((documentSnapshot) => {
            if (!documentSnapshot.exists) {
              console.log(`Create the lock for [${topicName}].`);
              transaction.create(documentReference, super.getLockObject());
              return true;
            } else if (!this.isLockAvailable(
                           documentSnapshot.get('available'),
                           documentSnapshot.updateTime.toDate())) {
              return false;
            }
            transaction.update(
                documentReference, super.getLockObject(),
                {lastUpdateTime: documentSnapshot.updateTime});
            this.logger.debug(`Get lock of ${topicName} successfully.`);
            return true;
          })
          .catch((error) => {
            console.log(
                `Error in getting the lock [${topicName}]. Reason:`, error);
            return false;
          });
    };
    return this.lock.runTransaction(getLockInTransaction);
  }

  /** @override */
  unlock(topicName) {
    /**
     * Releases (returns) the API lock in a given transaction context.
     * @param {!Transaction} transaction Transaction object which passed to a
     *     transaction's updateFunction provides the methods to read and write
     *     data within the transaction context
     * @return {!Promise<boolean>} Whether successfully returns the lock.
     */
    const unlockInTransaction = (transaction) => {
      this.logger.debug(`Start to return Lock for ${topicName}`);
      let documentReference = this.lock.getDocumentReference(topicName);
      return transaction.get(documentReference)
          .then(documentSnapshot => {
            if (documentSnapshot.get('available') === 1) {
              console.log(`There is an available lock for [${topicName}].`);
              return false;
            } else {
              this.logger.debug(`${topicName} has lock, try to unlock.`);
              transaction.update(
                  documentReference, super.getUnlockObject(),
                  {lastUpdateTime: documentSnapshot.updateTime});
              this.logger.debug(`${topicName} unlock successfully.`);
              return true;
            }
          })
          .catch((error) => {
            console.log(`Error in unlocking [${topicName}]. Reason:`, error);
            return false;
          });
    };
    return this.lock.runTransaction(unlockInTransaction);
  }
}

exports.ApiLockOnFirestore = ApiLockOnFirestore;
