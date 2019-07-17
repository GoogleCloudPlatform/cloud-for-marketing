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
const Firestore = require('@google-cloud/firestore');

const {DocumentData, Transaction: NativeModeTransaction} = Firestore;

/**
 * Document in Native mode or Entity in Datastore mode.
 * @typedef {(!DocumentData|!DatastoreModeEntity)}
 */
let Entity;
exports.Entity = Entity;

/**
 * Transaction from Native mode or Datastore mode.
 * @typedef {(!NativeModeTransaction|!DatastoreModeTransaction)}
 */
let Transaction;
exports.Transaction = Transaction;

/**
 * Types of data source.
 * @enum {string}
 */
const DataSource = {
  FIRESTORE: 'firestore',
  DATASTORE: 'datastore',
};
exports.DataSource = DataSource;

/**
 * Firestore has two modes: 'Native' and 'Datastore'. Only one of them can be
 * set in a Cloud Project and it can not be changed in the Cloud Project once
 * it's set.
 * In order not to restrict the users to one of this specific mode. This
 * interface offers unified operations on the data objects in both of these two
 * modes.
 *
 * However, the difference between Firestore Native mode ('Firestore')
 * and Firestore Datastore mode ('Datatore') is big:
 * 1. 'Firestore' has two kinds objects: 'document' stands for an object (data
 * entity) and 'collection' stands for a group of 'documents'. The
 * data structure organizes in a 'document' criss-crossing with 'collection'
 * way: 'document/collection/document/collection/...'
 * 2. 'Datastore' has 'entity' as the main data object. Entities are managed
 * based on 'namespace' and 'kind'. 'Namespace' is used to organize the data.
 * 'Kind' is the type of the entities. Every entity has its one Key, which
 * combines namespace, kind and a unique Id.
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
  getObject(id) {}

  /**
   * Saves or updates the document/entity with the given data and Id.
   * @param {!Entity} data Data to save or update in the document/entity.
   * @param {string|number|undefined=} id Document/Entity Id. Leaves it empty to
   *     create a new one.
   * @return {!Promise<string|number>} The ID of saved document/entity.
   */
  saveObject(data, id = undefined) {}

  /**
   * Deletes the document/entity with the given Id.
   * @param {string|number} id Document/Entity Id.
   * @return {!Promise<boolean>} Whether the operation is succeeded.
   */
  deleteObject(id) {}

  /**
   * Returns all documents under the collection or entities of this kind.
   * @return {!Promise<{string:!Entity}>} The documents or entitis.
   */
  getAll() {}

  /**
   * Run the given function within a transaction. The given function needs to
   * take care the transaction, e.g. commit.
   * @param {function(!Transaction): !Promise<*>} fn Function that will be
   *     invoked with a transaction. It returns any type.
   * @return {!Promise<*>} The return value of the function which is passed in.
   */
  runTransaction(fn) {}
}

/**
 * Returns whether the mode of Firestore is 'Native'.
 * @return {!Promise<boolean>}
 */
exports.isNativeMode = () => {
  return new Firestore().listCollections().then(() => true).catch((error) => {
    console.log(`In detecting Firestore mode: `, error.message);
    return false;
  });
};
exports.FirestoreAccessBase = FirestoreAccessBase;
