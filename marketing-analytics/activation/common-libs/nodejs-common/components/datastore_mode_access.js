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
const {FirestoreAccessBase} = require('./firestore_access_base.js');
const {getLogger} = require('./utils.js');

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
   * @param {string|number|undefined} id Entity Id.
   * @return {!Key}
   */
  getKey(id) {
    const keyPath = [this.kind];
    if (id) keyPath.push(id);
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
          this.logger.debug(
              `Result of saving ${id}@${this.kind}: `,
              JSON.stringify(apiResponse));
          if (id) return id;
          return apiResponse[0]['mutationResults'][0].key.path[0].id;
        });
  }

  /** @override */
  deleteObject(id) {
    const key = this.getKey(id);
    return this.datastore.delete(key).then((apiResponse, error) => {
      this.logger.debug(`Delete ${id}@${this.kind}: `, apiResponse);
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
    const promise = new Promise((resolve, reject) => {
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
    return promise;
  }

  /** @override */
  runTransaction(fn) {
    return this.datastore.transaction().run().then(
        ([transaction]) => fn(transaction));
  }
}

module.exports = DatastoreModeAccess;
