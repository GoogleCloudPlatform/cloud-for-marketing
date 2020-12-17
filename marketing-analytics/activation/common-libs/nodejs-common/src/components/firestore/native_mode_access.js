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
 * @fileoverview Google Firestore in 'Native' mode utilities class.
 */

'use strict';

const {
  Firestore,
  DocumentReference,
  CollectionReference,
  OrderByDirection,
} = require('@google-cloud/firestore');
const {FirestoreAccessBase} = require('./access_base.js');
const {getLogger} = require('../utils.js');

/**
 * Implementation of 'FirestoreAccessBase' on the Native mode.
 *
 * @implements {FirestoreAccessBase}
 */
class NativeModeAccess {
  /**
   * Initializes NativeModeAccess ('Firestore') instance.
   * Firestore uses 'collection' to host entities. This instance holds a
   * specific 'collection' as the parent of all entities that it can operate.
   * Because Firestore organizes data in a 'document/collection/document/...'
   * way. This constructor will check the path to make sure it presents a
   * 'collection', otherwise an Error will be thrown.
   * @param {string} path Path for the 'collection'.
   */
  constructor(path) {
    /** @type {!Firestore} */
    this.firestore = new Firestore();
    if (path.split('/').length % 2 === 0) {
      throw new Error(`Invalid path for Collection: ${path}`);
    }
    /** @type {string} Path of this 'collection'. */
    this.path = path;
    /** @type {!CollectionReference} A Firestore collection. */
    this.collection = this.firestore.collection(path);
    /** @type {!Logger} */
    this.logger = getLogger('FS.ACC');
  }

  /**
   * Gets the 'DocumentReference' object for the given document Id.
   * In Firestore, one document has two kinds of objects: 'DocumentReference'
   * for updating/deleting and 'DocumentReference' for reading.
   *
   * @param {string} id Document Id.
   * @return {!DocumentReference}
   */
  getDocumentReference(id) {
    return this.collection.doc(id);
  }

  /** @override */
  getObject(id) {
    return this.getDocumentReference(id)
        .get()
        .then((documentSnapshot) => {
          if (documentSnapshot.exists) {
            this.logger.debug(`Get ${this.path}/${id}:`, documentSnapshot);
            return documentSnapshot.data();
          } else {
            console.log(`Failed to find doc: ${this.path}/${id}`);
          }
        })
        .catch((error) => {
          console.error(error);
        });
  }

  /** @override */
  saveObject(data, id = undefined) {
    this.logger.debug(`Start to save doc ${this.path}/${id}`, data);
    if (id) {
      return this.getDocumentReference(id).set(data).then((writeResult) => {
        this.logger.debug(
            `Result of saving doc ${this.path}/${id}: `, writeResult);
        return id;
      });
    } else {
      console.log(`Create new doc under ${this.path}`);
      return this.collection.add(data).then((documentReference) => {
        this.logger.debug(
            `Saved ${JSON.stringify(data)} as:`, documentReference);
        return documentReference.id;
      });
    }
  }

  /** @override */
  deleteObject(id) {
    const documentReference = this.getDocumentReference(id);
    return documentReference.get().then((documentSnapshot) => {
      if (!documentSnapshot.exists) return false;
      return documentReference.delete().then((writeResult) => {
        this.logger.debug(`Delete ${this.path}/${id}: `, writeResult);
        return true;
      });
    });
  }

  /**
   * see https://firebase.google.com/docs/firestore/query-data/queries
   * It doesn't support multiple conditions by default, need index.
   * It doesn't work when filter and order have different property names.
   * @override
   */
  queryObjects(filters, order, limit, offset) {
    let query = this.collection;
    if (filters) {
      filters.forEach((filter) => {
        query =
            query.where(filter.property, filter.operator || '==', filter.value);
      });
    }
    if (order) {
      query = query.orderBy(order.name,
          (order.desc ? /** @type {OrderByDirection} */'desc' : 'asc'));
    }
    if (limit) query = query.limit(limit);
    if (offset) query = query.offset(offset);
    return query.get()
        .then((snapshot) => {
          const result = [];
          if (snapshot.empty) {
            console.log('No matching documents.');
            return result;
          }
          snapshot.forEach((document) => {
            result.push({id: document.id, entity: document.data()});
          });
          return result;
        })
        .catch((error) => {
          console.log('Error getting documents', error);
          throw error;
        });
  }

  /** @override */
  runTransaction(fn) {
    return this.firestore.runTransaction(fn);
  }

  /** @override */
  wrapInTransaction(id, transactionOperation) {
    return (transaction) => {
      console.log(`Transaction starts for ${this.path}/${id}.`);
      const documentReference = this.getDocumentReference(id);
      return transaction.get(documentReference).then((documentSnapshot) =>
          transactionOperation(documentSnapshot, documentReference,
              transaction));
    };
  }
}

module.exports = NativeModeAccess;
