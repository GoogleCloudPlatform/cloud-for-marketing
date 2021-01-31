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
 * @fileoverview Interface for operations on different modes of Firestore.
 */

'use strict';

const {
  Entity: DatastoreModeEntity,
  Transaction: DatastoreModeTransaction,
} = require('@google-cloud/datastore');
const {
  Firestore,
  DocumentData,
  Transaction: NativeModeTransaction,
} = require('@google-cloud/firestore');

/**
 * Document in Native mode or Entity in Datastore mode.
 * @typedef {(!DocumentData|!DatastoreModeEntity)}
 */
let Entity;

/**
 * Transaction from Native mode or Datastore mode.
 * @typedef {(!NativeModeTransaction|!DatastoreModeTransaction)}
 */
let Transaction;

/**
 * Query filter definition. Though the default operator is 'equals', Firestore
 * and Datastore have different operation symbols and requests. Check the
 * documents for details:
 * Datastore see:
 * https://cloud.google.com/nodejs/docs/reference/datastore/2.0.x/Query#filter
 * Firestore see: https://cloud.google.com/firestore/docs/query-data/get-data
 * @typedef {{
 *   property:string,
 *   value:!object,
 *   operator:(string|undefined),
 * }}
 */
let Filter;

/**
 * Offers a facade of the Document on Firestore Native mode based on Datastore
 * mode entity.
 */
class DatastoreDocumentFacade {
  /**
   * Initializes DocumentFacade for Datasotre.
   * @param {!DatastoreModeEntity} entity
   */
  constructor(entity) {
    this.entity = entity;
    this.exists = !!entity;
  }

  get(property) {
    return this.entity[property];
  }
}

/**
 * Offers a facade of Firestore Native mode transaction based on Datastore mode
 * transaction.
 * Currently, this facade is used in a context with a given Transaction and
 * a loaded entity. This entity is required to fulfill some functions, e.g.
 * 'update'.
 */
class DatastoreTransactionFacade {
  /**
   * Initializes Firstore TransactionFacade for Datasotre Transaction.
   * @param {!DatastoreModeTransaction} transaction
   * @param {!DatastoreModeEntity} entity
   */
  constructor(transaction, entity) {
    this.entity = entity;
    this.transaction = transaction;
  }

  create(documentReference, data) {
    this.transaction.save([{key: documentReference, data}]);
  }

  /**
   * Firestore support to 'update' partial properties of the object which
   * Datastore doesn't. Need to used the original entity to complete the final
   * updated entity.
   */
  update(documentReference, data) {
    const updatedEntity = Object.assign({}, this.entity, data);
    this.transaction.save([{key: documentReference, data: updatedEntity}]);
  }
}

/**
 * Operations will be proceeded in a Transaction.
 * @typedef {function(DatastoreDocumentFacade,Key,DatastoreTransactionFacade):boolean}
 */
let TransactionOperation;

/**
 * Types of data source.
 * @enum {string}
 */
const DataSource = Object.freeze({
  FIRESTORE: 'firestore',
  DATASTORE: 'datastore',
});

/**
 * Firestore has two modes: 'Native' and 'Datastore'. Only one of them can be
 * set in a Cloud Project and it can not be changed in the Cloud Project once
 * it's set.
 * In order not to restrict the users to one of this specific mode. This
 * interface offers unified operations on the data objects in both of these two
 * modes.
 *
 * Firestore Native mode ('Firestore') and Firestore Datastore mode ('Datatore')
 * have different interfaces:
 * 1. 'Firestore' has two kinds objects: 'document' stands for an object (data
 * entity) and 'collection' stands for a group of 'documents'. The
 * data structure organizes in a 'document' criss-crossing with 'collection'
 * way: 'document/collection/document/collection/...'
 * 2. 'Datastore' has 'entity' as the main data object. Entities are managed
 * based on 'namespace' and 'kind'. 'Namespace' is used to organize the data.
 * 'Kind' is the type of the entities. Every entity has its one Key, which
 * combines namespace, kind and a unique Id.
 * 3. Different API in Transaction objects.
 *
 * For more details, see
 * https://cloud.google.com/datastore/docs/concepts/overview#comparison_with_traditional_databases
 *
 * @interface
 */
class FirestoreAccessBase {
  /**
   * Gets the document/entity with the given Id.
   * @param {string|number} id Document/Entity Id.
   * @return {!Promise<(!Entity|undefined)>}
   */
  getObject(id) {
  }

  /**
   * Saves or updates the document/entity with the given data and Id.
   * @param {!Entity} data Data to save or update in the document/entity.
   * @param {string|number|undefined=} id Document/Entity Id. Leaves it empty to
   *     create a new one.
   * @return {!Promise<string|number>} The ID of saved document/entity.
   */
  saveObject(data, id = undefined) {
  }

  /**
   * Deletes the document/entity with the given Id.
   * @param {string|number} id Document/Entity Id.
   * @return {!Promise<boolean>} Whether the operation is succeeded.
   */
  deleteObject(id) {
  }

  /**
   * Returns all matched document/entity with the given filter conditions.
   * @param {!Array<!Filter>|undefined} filters Conditions to filter the list.
   * @param {{name:string, desc:(boolean|undefined)}|undefined} order Sort the
   * results by a property name in ascending or descending order. By default, an
   * ascending sort order will be used which is the Datastore's client library's
   * default behavior. See:
   * https://googleapis.dev/nodejs/datastore/latest/Query.html#order
   * @param {number|undefined} limit A limit on a query.
   * @param {number|undefined} offset An offset on a query.
   * @return {!Promise<!Array<{id:(string|number),entity:!Entity}>>} The
   *     documents or entities.
   */
  queryObjects(filters, order, limit, offset) {
  }

  /**
   * Runs the given function within a transaction. The given function needs to
   * take care the transaction, e.g. commit.
   * @param {function(!Transaction): !Promise<*>} fn Function that will be
   *     invoked with a transaction. It returns any type.
   * @return {!Promise<*>} The return value of the function which is passed in.
   */
  runTransaction(fn) {
  }

  /**
   * Wraps the operation in a transaction and returns a function that can be
   * invoked in `runTransaction`.
   * This function offers a context to proceed transaction operations that are
   * related to a single entity in racing condition. For more complicated use
   * cases, use `runTransaction` to wrap the function directly.
   * @param {(string|number)} id Document/Entity Id.
   * @param {!TransactionOperation} transactionOperation
   * @return {function(!Transaction): Promise<boolean>}
   */
  wrapInTransaction(id, transactionOperation) {
  }

  /**
   * Returns whether the mode of Firestore is 'Native'.
   * @return {!Promise<boolean>}
   */
  static isNativeMode() {
    return new Firestore().listCollections().then(() => true).catch((error) => {
      console.log(`In detecting Firestore mode: `, error.message);
      return false;
    });
  };
}

module.exports = {
  DataSource,
  Entity,
  Transaction,
  Filter,
  TransactionOperation,
  DatastoreDocumentFacade,
  DatastoreTransactionFacade,
  FirestoreAccessBase,
};
