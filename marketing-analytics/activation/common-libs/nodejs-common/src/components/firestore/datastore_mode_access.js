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
 * @fileoverview Google Firestore in 'Datastore' mode utilities class.
 */

'use strict';

/**
 * 'Key' is Datastore Key object. It is defined at
 * https://github.com/googleapis/nodejs-datastore/blob/master/src/entity.ts
 * But 'Key' isn't exported from the package, so it can't be imported directly
 * here: https://github.com/googleapis/nodejs-datastore/blob/master/src/index.ts
 */
const {Datastore} = require('@google-cloud/datastore');
const {
  DatastoreDocumentFacade,
  DatastoreTransactionFacade,
  FirestoreAccessBase,
} = require('./access_base.js');
const {getLogger, wait} = require('../utils.js');

/** @const {number} Max retry times when commit failed in a transcation. */
const MAX_RETRY_TIMES = 5;

/**
 * Implementation of 'FirestoreAccessBase' on the Datastore mode.
 *
 * @implements {FirestoreAccessBase}
 */
class DatastoreModeAccess {

  /**
   * Initializes DatastoreModeAccess instance.
   * @param {string} namespace The namespace for data.
   * @param {string} kind The kind of this entity.
   */
  constructor(namespace, kind) {
    /** @type{Datastore} */
    this.datastore = new Datastore();
    this.kind = kind;
    this.namespace = namespace;
    this.logger = getLogger('DS.ACC');
  }

  /**
   * Gets the 'Key' for the entity.
   * Datastore uses 'Key' to identify entities. A 'Key' composes of Id, entity
   * kind and namespace. The 'id' can be 'undefined' if the next operation is
   * creating a new entity.
   * The default Id of Datastore is a integar. However, Pub/sub can only send
   * attributes with string values. This will cause the Datastore Ids to be
   * converted to strings. So here will try to change the id back to number if
   * possible.
   * @param {string|number|undefined} id Entity Id.
   * @return {!Key}
   */
  getKey(id) {
    const keyPath = [this.kind];
    if (id) keyPath.push(isNaN(id) ? id : parseInt(id));
    return this.datastore.key({
      namespace: this.namespace,
      path: keyPath,
    });
  }

  /** @override */
  getObject(id) {
    const key = this.getKey(id);
    return this.datastore.get(key)
        .then(([entity, error]) => {
          if (!error) {
            this.logger.debug(`Get ${id}@${this.kind}`, entity);
            return entity;
          } else {
            console.log(`Not found ${id}@${this.kind}`, error);
          }
        })
        .catch((error) => {
          console.error(error);
        });
  }

  /**
   * Returns the id only after the related entity exists.
   * @param {string|number} id
   * @return {!Promise<string|number>}
   */
  waitUntilGetObject(id) {
    return this.getObject(id).then((entity) => {
      if (entity) return id;
      this.logger.debug(`Wait 1 more second until the eneity@${id} is ready`);
      return wait(1000, this.waitUntilGetObject(id));
    });
  }

  /** @override */
  saveObject(data, id = undefined) {
    this.logger.debug(`Start to save entity ${id}@${this.kind}`, data);
    const key = this.getKey(id);
    return this.datastore
        .save({
          key: key,
          data: data,
        })
        .then((apiResponse) => {
          // Default key in Datastore is a number in response like following.
          // With a given id, the key in response is null.
          const updatedId = id !== undefined ? id
              : +apiResponse[0]['mutationResults'][0].key.path[0].id;
          this.logger.debug(
              `Result of saving ${updatedId}@${this.kind}: `,
              JSON.stringify(apiResponse));
          // Datastore has a delay to write entity. This method only returns id
          // after it is created. For updating, it always return at once because
          // the entity exists.
          return this.waitUntilGetObject(updatedId);
        });
  }

  /** @override */
  deleteObject(id) {
    const key = this.getKey(id);
    return this.datastore.delete(key).then((apiResponse, error) => {
      this.logger.debug(`Delete ${id}@${this.kind}: `,
          JSON.stringify(apiResponse));
      // Returns true even try to delete a deleted entity. The responses of
      // delete operations only differ at the property 'indexUpdates' for a
      // normal entity and a deleted entity.
      if (!error) return true;
      console.error(`Error in deleting ${id}@${this.kind}`, error);
      return false;
    });
  }

  /** @override */
  queryObjects(filters, order, limit, offset) {
    let query = this.datastore.createQuery(this.namespace, this.kind);
    if (filters) {
      filters.forEach((filter) => {
        query =
            query.filter(filter.property, filter.operator || '=', filter.value);
      });
    }
    if (order) query = query.order(order.name, {descending: order.desc});
    if (limit) query = query.limit(limit);
    if (offset) query = query.offset(offset);
    return new Promise((resolve, reject) => {
      const result = [];
      this.datastore.runQueryStream(query)
          .on('error',
              (error) => {
                console.error(error);
                reject(error);
              })
          .on('data',
              (entity) => {
                const key = entity[this.datastore.KEY];
                result.push({id: key.path[1], entity: entity});
              })
          .on('info',
              (info) => {
                console.log(`Info event: ${JSON.stringify(info)}`);
              })
          .on('end', () => {
            resolve(result);
          });
    });
  }

  /**
   * Datastore API does not automatically retry transactions. This functions
   * will retry {MAX_RETRY_TIMES} times. Between retries, it will wait an
   * incremental time.
   * See: https://cloud.google.com/datastore/docs/concepts/transactions#uses_for_transactions
   *
   * @override
   */
  runTransaction(fn) {
    let leftRetries = MAX_RETRY_TIMES;
    const runFn = () => this.datastore.transaction().run().then(
        ([transaction]) => fn(transaction));
    const retryFn = (error) => {
      console.log(
          `TX ERROR[${error.message}]. Retries left: ${leftRetries} times.`);
      leftRetries--;
      return wait(500 * (MAX_RETRY_TIMES - leftRetries)).then(() => {
        if (leftRetries === 0) return runFn();
        return runFn().catch(retryFn);
      });
    };
    return runFn().catch(retryFn);
  }

  /** @override */
  wrapInTransaction(id, transactionOperation) {
    return (transaction) => {
      console.log(`Transaction starts for ${this.kind}@${id}.`);
      const key = this.getKey(id);
      return transaction.get(key).then(([entity]) => {
        const result = transactionOperation(
            new DatastoreDocumentFacade(entity),
            key,
            new DatastoreTransactionFacade(transaction, entity)
        );
        return transaction.commit().then(() => result);
      });
    };
  }
}

module.exports = DatastoreModeAccess;
