// Copyright 2019 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this fileAccessObject except in compliance with the License.
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
 * @fileoverview Base Data Access Object class which is based on Firestore
 * (Native mode or Datastore mode).
 */

'use strict';

const {
  Entity,
  DataSource,
  Filter,
  FirestoreAccessBase,
} = require('../components/firestore_access_base.js');
const NativeModeAccess = require('../components/native_mode_access.js');
const DatastoreModeAccess = require('../components/datastore_mode_access.js');

/**
 * This is the base data-access-object class.
 * It seals the details of different underlying databases, Firestore and
 * Datastore.
 */
class BaseDao {
  /**
   * Initializes the instance based on given data source.
   * @param {string} kind The data model name.
   * @param {string} namespace The namespace of the data.
   */
  constructor(kind, namespace) {
    /** @const {string} */ this.namespace = namespace;
    /** @const {!DataSource} */ this.dataSource = process.env['FIRESTORE_TYPE'];
    /** @type {!FirestoreAccessBase} */ this.accessObject = undefined;
    switch (this.dataSource) {
      case DataSource.FIRESTORE:
        this.accessObject = new NativeModeAccess(
            `${this.namespace}/database/${kind}`);
        break;
      case DataSource.DATASTORE:
        this.accessObject = new DatastoreModeAccess(this.namespace, kind);
        break;
      default:
        throw new Error(`Unknown DataSource item: ${this.dataSource}.`);
    }
  }

  /**
   * Saves the document/entity with the given data and Id.
   * @param {!Entity} entity Data to save or update in the document/entity.
   * @return {!Promise<string|number>} The Id of saved document/entity.
   */
  create(entity) {
    return this.accessObject.saveObject(entity);
  }

  /**
   * Updates the document/entity with the given data and Id.
   * @param {!Entity} entity Data to be updated in the document/entity.
   * @param {string|number|undefined=} id Document/Entity Id.
   * @return {!Promise<string|number>} The ID of updated document/entity.
   */
  update(entity, id) {
    return this.accessObject.saveObject(entity, id);
  }

  /**
   * Merges options to the document/entity of the given Id.
   * @param {!Entity} entity Data to be merged into the document/entity.
   * @param {string|number|undefined=} id Document/Entity Id.
   * @return {!Promise<string|number>} The ID of updated document/entity.
   */
  merge(options, id) {
    return this.accessObject.getObject(id).then((entity) => {
      return this.update(Object.assign(entity, options), id);
    });
  }

  /**
   * Gets the document/entity with the given Id.
   * @param {string|number} id Document/Entity Id.
   * @return {!Promise<(!Entity|undefined)>}
   */
  load(id) {
    return this.accessObject.getObject(id);
  }

  /**
   * Removes the document/entity with the given Id.
   * @param {string|number} id Document/Entity Id.
   * @return {!Promise<boolean>} Whether the operation is succeeded.
   */
  remove(id) {
    return this.accessObject.deleteObject(id);
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
   * @return {!Array<{id:(string|number),entity:!Entity}>} The documents or
   *     entities.
   */
  list(filters, order, limit, offset) {
    return this.accessObject.queryObjects(filters, order, limit, offset);
  }
}

exports.BaseDao = BaseDao;