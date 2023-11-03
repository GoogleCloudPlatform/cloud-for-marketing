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
  firestore: { TransactionOperation, DatastoreDocumentFacade, DataAccessObject },
  utils: {getLogger},
} = require('@google-cloud/nodejs-common');
const {ApiLock} = require('./api_lock.js');

/**
 * The 'ApiLock' is a document in Firestore (or entity in Datastore) with the
 * unique key(lockId) from the name of API.
 * 'max' is the maximum available instances for this lock. It depends on the
 * API specification.
 * 'tokens' stands for different instances of the lock.
 * Getting a lock will registed the given 'token' to 'tokens' and reduce the
 * the property 'available' for 1, while unlocking a 'token' will remove that
 * element from the array 'tokens' and increases 'available' 1.
 *
 * @typedef {{
 *   max: number,
 *   available: number,
 *   tokens: Array<{
 *     token: string,
 *     updatedAt: number,
 *   }
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
     * By default, it is 10 mintues.
     * @type {number}
     */
    this.maxTimeForLocks = 10 * 60 * 1000;
    this.logger = getLogger(`LOCK.${dataSource}`);
  }

  /** @override */
  async getLock(lockId, token) {
    this.logger.debug(`Try to add Token[${token}] for Lock[${lockId}]`);
    try {
      const transactionOperation = this.getLockOperation(lockId, token);
      const transactionFunction =
        this.wrapInTransaction(lockId, transactionOperation);
      return await this.runTransaction(transactionFunction);
    } catch (error) {
      this.logger.error(
        `Error in getLock ${lockId} for Token[${token}]. Reason:`, error);
      return false;
    }
  }

  /** @override */
  async unlock(lockId, token) {
    this.logger.debug(`Try to release Token[${token}] for Lock[${lockId}]`);
    try {
      const transactionOperation = this.getUnlockOperation(lockId, token);
      const transactionFunction =
        this.wrapInTransaction(lockId, transactionOperation);
      return await this.runTransaction(transactionFunction);
    } catch (error) {
      this.logger.error(
        `Error in unlock ${lockId} for Token[${token}]. Reason:`, error);
      return false;
    }
  }

  /** @override */
  async hasAvailableLock(lockId) {
    const { max, tokens = [] } = await this.load(lockId);
    return max > tokens.length;
  }

  /**
   * Get the operation that gets a lock in a transaction. It returns true when
   * 1. There is no such lock. Or
   * 2. There are available lock(s). If there are no available locks, it will
   * release those expired locks (from the oldest) to get a lock.
   *
   * @param {string} lockId
   * @param {string} token
   * @return {!TransactionOperation}
   */
  getLockOperation(lockId, token) {
    return (documentSnapshot, documentReference, transaction) => {
      if (!documentSnapshot.exists) {
        transaction.create(documentReference, this.getNewLockEntity(lockId, token));
        this.logger.info(`Created new Lock[${lockId}] with Token[${token}]`);
        return true;
      }
      this.logger.debug(`Get Lock[${lockId}], try to register token:`, token);
      const lockEntity =
        this.registerTokenForLock(documentSnapshot, lockId, token);
      if (!lockEntity) {
        this.logger.warn(
          `There is no available Lock[${lockId}] with Token[${token}].`);
        return false;
      }
      transaction.update(documentReference, lockEntity);
      this.logger.debug(`Register Token[${token}] for Lock[${lockId}].`);
      return true;
    };
  }

  /**
   * Releases the lock. It returns false when there is not a such token for the
   * lock.
   * Otherwise, it will create the lock (if it doesn't exist) or release the
   * token of the lock.
   *
   * @param {string} lockId
   * @param {string} token
   * @return {!TransactionOperation}
   * @private
   */
  getUnlockOperation(lockId, token) {
    return (documentSnapshot, documentReference, transaction) => {
      if (!documentSnapshot.exists) {
        transaction.create(documentReference, this.getNewLockEntity(lockId));
        this.logger.info(`The Lock[${lockId}] doesn't exist. Create it now.`);
        return true;
      }
      this.logger.debug(`Get Lock[${lockId}], try to release token:`, token);
      const lockEntity =
        this.releaseTokenForLock(documentSnapshot, lockId, token);
      if (!lockEntity) {
        this.logger.warn(`There is no Token[${token}] for Lock[${lockId}].`);
        return false;
      }
      transaction.update(documentReference, lockEntity);
      this.logger.debug(`Release Token[${token}] for Lock[${lockId}].`);
      return true;
    };
  }

  /**
   * Registers the token for the specified Lock and returns the updated Lock
   * entity to be saved. If there is no available lock, it returns undefined.
   * @param {!DatastoreDocumentFacade} documentSnapshot
   * @param {string} lockId
   * @param {string} token
   * @return {!ApiLockObject|undefined}
   */
  registerTokenForLock(documentSnapshot, lockId, token) {
    let tokens = documentSnapshot.get('tokens') || [];
    if (tokens.some(({ token: existingToken }) => existingToken === token)) {
      throw new Error(`Token[${token}] exists!`);
    }
    const max = documentSnapshot.get('max')
      || this.getMaximumInstanceForLock(lockId);
    const available = max - tokens.length;
    const sortFn = (a, b) => Math.sign(a.updatedAt - b.updatedAt);
    if (available <= 0) {
      const cutoffTime = Date.now() - this.maxTimeForLocks;
      const timeoutedTokens =
        tokens.filter(({ updatedAt }) => updatedAt < cutoffTime);
      if (available + timeoutedTokens.length <= 0) {
        return;
      }
      const sortedTokens = tokens.sort(sortFn);
      for (let i = 0; i < 1 - available; i++) {
        const releasedLock = sortedTokens.shift();
        this.logger.info(`Release lock with token[${releasedLock.token}]`,
          new Date(releasedLock.updatedAt).toISOString());
      }
      tokens = sortedTokens;
    }
    return this.getUpdatedLockEntity(max, token, tokens);
  }

  /**
   * Releases the token for the specified Lock and returns the updated Lock
   * entity to be saved. If there is no such token for the lock, it returns
   * undefined.
   * @param {!DatastoreDocumentFacade} documentSnapshot
   * @param {string} lockId
   * @param {string} token
   * @return {!ApiLockObject|undefined}
   */
  releaseTokenForLock(documentSnapshot, lockId, token) {
    const tokens = documentSnapshot.get('tokens') || [];
    if (!tokens.some(({ token: existingToken }) => existingToken === token)) {
      return;
    }
    const max = documentSnapshot.get('max')
      || this.getMaximumInstanceForLock(lockId);
    const updatedTokens = tokens.filter(
      ({ token: existingToken }) => existingToken !== token);
    return this.getUpdatedLockEntity(max, undefined, updatedTokens);
  }

  /**
   * Returns the new Lock entity to be created for the specified lock Id.
   * @param {string} lockId
   * @param {string|undefined} token If token presents, the new Lock will
   *   regeister the token when it is created.
   * @return {!ApiLockObject} Api Lock entity.
   * @private
   */
  getNewLockEntity(lockId, token) {
    const max = this.getMaximumInstanceForLock(lockId);
    return this.getUpdatedLockEntity(max, token);
  }

  /**
   * Returns the updated Lock entity. If there is a `token`, it will be
   * registered; if there is no `tokens`, it would be intialized as an empty
   * Array which means all instances of the lock are available.
   * @param {number} max
   * @param {string|undefined} token
   * @param {Array<{{token:string, updatedAt: number}}>=} tokens
   * @return {!ApiLockObject} Api Lock entity to be created or updated.
   */
  getUpdatedLockEntity(max, token, tokens = []) {
    if (token) tokens.push({ token, updatedAt: Date.now() });
    const lock = {
      max,
      available: max - tokens.length,
      tokens,
    };
    return lock;
  }

  /**
   * Returns the default value of the maximum instance number for a given Lock.
   * The 'max' value can be changed in Firestore after this Lock object is
   * created.
   * TODO: In a long term, find a way to modify the value based on performance.
   * @param {string} topicName
   * @return {number}
   */
  getMaximumInstanceForLock(topicName) {
    const api = topicName.split('-')[1];
    switch (api) {
      case 'ACLC':
      case 'ACA':
      case 'ACM':
        return 5;
      case 'MP':
      case 'MP_GA4':
        return 40;
      default:
        return 1;
    }
  }
}

module.exports = {ApiLockDao};
