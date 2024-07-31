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

/** @fileoverview Cloud Datastore API handler class.*/

/**
 * The ultility functions to convert data between Json and Datastore entity.
 */
const DATASTORE_TRANSFORM = {

  toJson: (obj) => {
    const json = {};
    Object.keys(obj).forEach((key) => {
      json[key] = DATASTORE_TRANSFORM.toJsonValue_(obj[key]);
    })
    return json;
  },
  /**
    * Returns a Datastore 'Value' for a Javascript object.
    * @see https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value
    * @param {object} obj A JSON object.
    * @return {!Value} Datastore object.
    * @private
    */
  toDatastoreObject: (obj) => {
    const entity = {};
    Object.keys(obj).forEach((key) => {
      entity[key] = DATASTORE_TRANSFORM.toDatastoreValue_(obj[key]);
    })
    return entity;
  },
  toJsonValue_: (value) => {
    if (typeof value.stringValue !== 'undefined') return value.stringValue;
    if (typeof value.booleanValue !== 'undefined') return value.booleanValue;
    if (typeof value.integerValue !== 'undefined') return Number(value.integerValue);
    if (typeof value.doubleValue !== 'undefined') return Number(value.doubleValue);
    const { arrayValue, entityValue } = value;
    if (arrayValue) {
      return arrayValue.values ?
        arrayValue.values.map(DATASTORE_TRANSFORM.toJsonValue_)
        : arrayValue;
    }
    if (entityValue) {
      return entityValue.properties
        ? DATASTORE_TRANSFORM.toJson(entityValue.properties)
        : entityValue;
    }
    throw new Error(`Unrecognizable value ${value}`);
  },

  /**
   * Returns a Datastore 'Value' for a Javascript value.
   * @see https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value
   * @param {string|number|boolean Array|Object} value A Javascript value.
   * @return {!Value} Datastore object.
   * @private
   */
  toDatastoreValue_: (value) => {
    let propertyName;
    let propertyValue = value;
    switch (typeof value) {
      case 'string':
        propertyName = 'stringValue';
        break;
      case 'number':
        propertyName = Number.isInteger(value) ? 'integerValue' : 'doubleValue';
        break;
      case 'boolean':
        propertyName = 'booleanValue';
        break;
    }
    if (value === null) propertyName = 'nullValue';
    if (typeof value === 'object') {
      if (Array.isArray(value)) {
        propertyName = 'arrayValue';
        propertyValue = { values: value.map(DATASTORE_TRANSFORM.toDatastoreValue_) };
      } else {
        propertyName = 'entityValue';
        propertyValue = { properties: DATASTORE_TRANSFORM.toDatastoreObject(value) };
      }
    }
    if (!propertyName) throw new Error(`Unrecognizable value ${value}`);
    return { [propertyName]: propertyValue };
  }
};

class Datastore extends ApiBase {

  constructor(projectId, databaseId = DEFAULT_DATABASE, namespace, kind) {
    super();
    this.apiUrl = 'https://datastore.googleapis.com';
    this.version = 'v1';
    this.projectId = projectId;
    this.databaseId = databaseId === DEFAULT_DATABASE ? '' : databaseId;
    this.namespace = namespace;
    this.kind = kind;
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/projects/${this.projectId}`;
  }

  /** @override */
  getDefaultHeader() {
    const headers = super.getDefaultHeader();
    headers['x-goog-request-params'] =
      `project_id=${this.projectId}&database_id=${this.databaseId}`;
    return headers;
  }

  /**
   * Saves a group of entities to Datastore in a transaction.
   * @see https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/beginTransaction
   * @see https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/commit
   * @param {Object<string,object>} entities
   * @param {string} kind
   * @param {string} namespace
   * @return {!Datastore.Commit}
   * @see https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/commit#google.datastore.v1.Datastore.Commit
   */
  txSave(entities, kind = this.kind, namespace = this.namespace) {
    const { transaction } = super.mutate(':beginTransaction',
      { databaseId: this.databaseId, transactionOptions: { readWrite: {} } });
    /**
     * Create an array of entities.
     * @const {!Array<!Entity>}
     * @see https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value#Entity
     */
    const mutations = Object.keys(entities).map((key) => {
      return {
        upsert: {
          //https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value#Key
          key: {
            partitionId: {
              projectId: this.projectId,
              databaseId: this.databaseId,
              namespaceId: namespace,
            },
            path: [{ kind: kind, name: key, }]
          },
          properties: DATASTORE_TRANSFORM.toDatastoreObject(entities[key]),
        },
      };
    });
    const payload = {
      mode: 'TRANSACTIONAL',
      databaseId: this.databaseId,
      mutations,
      transaction,
    };
    const response = super.mutate(':commit', payload);
    if (response.error) {
      throw new Error(response.error.message);
    }
    return response;
  }

  /**
   * Returns all documents under the collection.
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases.documents/list
   * @return {!Array<{
   *    id:string,
   *    json:object,
   * }>}
   */
  list() {
    const payload = {
      databaseId: this.databaseId,
      partitionId: {
        projectId: this.projectId,
        databaseId: this.databaseId,
        namespaceId: this.namespace,
      },
      query: {
        kind: [{ name: this.kind }],
      },
    };
    const results = [];
    let hasMoreResult = false;
    do {
      const { batch } = super.mutate(':runQuery', payload);
      const { entityResults = [], endCursor, moreResults } = batch;
      results.push(...entityResults);
      hasMoreResult = moreResults !== 'NO_MORE_RESULTS';
      payload.query.startCursor = endCursor;
    } while (hasMoreResult)
    return results.map(({ entity: { key, properties } }) => {
      const id = key.path[0].name;
      const json = DATASTORE_TRANSFORM.toJson(properties);
      return { id, json };
    });
  }
}
