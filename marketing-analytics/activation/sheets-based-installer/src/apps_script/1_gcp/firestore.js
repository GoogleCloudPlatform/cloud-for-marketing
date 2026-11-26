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

/** @fileoverview Cloud Firestore API handler class.*/

/**
 * The name of default database.
 * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases#resource:-database
 * @const {string}
 */
const DEFAULT_DATABASE = '(default)';

/**
 * The type of the database. Mode changes are only allowed if the database is
 * empty.
 * @see https://cloud.google.com/datastore/docs/firestore-or-datastore
 * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases#DatabaseType
 * @enum {string}
 */
const FIRESTORE_MODE = Object.freeze({
  FIRESTORE_NATIVE: 'Native Mode',
  DATASTORE_MODE: 'Datastore Mode',
});

/**
 * The type of the database for DataAccessObject objects in Cloud Functions.
 */
const FIRESTORE_MODE_FOR_DAO = Object.freeze({
  FIRESTORE_NATIVE: 'FIRESTORE',
  DATASTORE_MODE: 'DATASTORE',
});

/**
 * The type of App Engine integration mode.
 * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases#DatabaseType
 * @enum {string}
 */
const APP_ENGINE_INTEGRATION_MODE = Object.freeze({
  UNSPECIFIED: 'APP_ENGINE_INTEGRATION_MODE_UNSPECIFIED',
  ENABLED: 'ENABLED',
  DISABLED: 'DISABLED',
});

/**
 * The utility functions to convert data between Json and Firestore document.
 */
const FIRESTORE_TRANSFORM = {
  /**
   * Returns a JSON object.
   * @param {!Object} obj A JSON object.
   * @return {!Value} Firestore object.
   */
  toJson: obj => {
    const json = {};
    Object.keys(obj).forEach(key => {
      json[key] = FIRESTORE_TRANSFORM.toJsonValue_(obj[key]);
    });
    return json;
  },
  /**
   * Returns a Firestore 'Value' for a Javascript object.
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases.documents#Document
   * @param {!Object} obj A JSON object.
   * @return {!Value} Firestore object.
   */
  toFirestoreObject: obj => {
    const document = {};
    Object.keys(obj).forEach(key => {
      document[key] = FIRESTORE_TRANSFORM.toFirestoreValue_(obj[key]);
    });
    return document;
  },
  toJsonValue_: value => {
    if (typeof value.stringValue !== 'undefined') return value.stringValue;
    if (typeof value.booleanValue !== 'undefined') return value.booleanValue;
    if (typeof value.integerValue !== 'undefined')
      return Number(value.integerValue);
    if (typeof value.doubleValue !== 'undefined')
      return Number(value.doubleValue);
    if (typeof value.timestampValue !== 'undefined')
      return new Date(value.timestampValue);
    const {arrayValue, mapValue} = value;
    if (arrayValue) {
      return arrayValue.values
        ? arrayValue.values.map(FIRESTORE_TRANSFORM.toJsonValue_)
        : arrayValue;
    }
    if (mapValue) {
      return mapValue.fields
        ? FIRESTORE_TRANSFORM.toJson(mapValue.fields)
        : mapValue;
    }
    throw new Error(`Unrecognizable value ${value}`);
  },
  /**
   * Returns a Firestore 'Value' for a Javascript value.
   * @see https://cloud.google.com/firestore/docs/reference/rest/Shared.Types/ArrayValue#Value
   * @param {string|number|boolean|!Array|!Object} value A Javascript value.
   * @return {!Value} Firestore object.
   * @private
   */
  toFirestoreValue_(value) {
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
        propertyValue = {
          values: value.map(FIRESTORE_TRANSFORM.toFirestoreValue_),
        };
      } else {
        propertyName = 'mapValue';
        propertyValue = {fields: FIRESTORE_TRANSFORM.toFirestoreObject(value)};
      }
    }
    if (!propertyName) throw new Error(`Unrecognizable value ${value}`);
    return {[propertyName]: propertyValue};
  },
};

class Firestore extends GcpApiBase {
  /**
   * @param {string=} projectId
   * @param {string=} databaseId = DEFAULT_DATABASE
   * @param {string=} path
   * @param {string=} kind
   */
  constructor(projectId, databaseId = DEFAULT_DATABASE, path, kind) {
    super();
    this.name = 'Cloud Firestore API';
    this.api = 'firestore.googleapis.com';
    this.permissions = [
      'datastore.databases.create',
      'datastore.databases.get',
    ];
    this.adminRole = 'Cloud Datastore Owner';
    this.runtimeRoles = ['roles/datastore.user'];
    this.apiUrl = `https://${this.api}`;
    this.version = 'v1';
    this.projectId = projectId;
    this.databaseId = databaseId;
    this.path = path || ''; // ${namespace}/database
    if (this.path && !this.path.endsWith('/')) {
      this.path += '/';
    }
    this.kind = kind;
  }

  /**
   * @override
   * @return {string}
   */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/projects/${this.projectId}`;
  }

  /**
   * @override
   * @return {!Object<string, string>}
   */
  getDefaultHeader() {
    const headers = super.getDefaultHeader();
    headers['x-goog-request-params'] =
      `project_id=${this.projectId}&database_id=${this.databaseId}`;
    return headers;
  }

  /**
   * Gets the information of the default database.
   * If the database is not created, will return:
   * {
   *   error: {
   *     code: 404,
   *     message: 'Project \'xx\' or database \'(default)\' does not exist.',
   *     status: 'NOT_FOUND'
   *   }
   * }
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases/get
   * @param {string=} databaseId
   * @return {!Database}
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases#Database
   */
  getDatabase(databaseId = this.databaseId) {
    return this.get(`databases/${databaseId}`);
  }

  /**
   * Lists information about the supported locations for this service.
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.locations/list
   * @see https://cloud.google.com/datastore/docs/locations
   * @return {!Array<!Location>}
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.locations#Location
   */
  // listLocations() {
  //   return super.list('locations', 'locations');
  // }

  /**
   * Create the default database.
   * For GCP already has the default database, it will return an error:
   * {
   *   error: {
   *     code: 409,
   *     message: 'Database already exists. Please use another database_id',
   *     status: 'ALREADY_EXISTS'
   *   }
   * }
   * If change the database name other than `(default)`, will return error:
   * {
   *   error: {
   *     code: 429,
   *     message: 'Project xxx exceeded its quota. Resource Exhausted (...)',
   *     status: 'RESOURCE_EXHAUSTED'
   *   }
   * }
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases/create
   * @param {string} locationId
   * @param {!DatabaseType} type
   * @param {string=} databaseId
   * @param {!AppEngineIntegrationMode=} appEngineIntegrationMode
   * @return {!Operation}
   * @see https://cloud.google.com/firestore/docs/reference/rest/Shared.Types/Operation
   */
  createDatabase(
    locationId,
    type,
    databaseId = this.databaseId,
    appEngineIntegrationMode = APP_ENGINE_INTEGRATION_MODE.DISABLED,
  ) {
    const payload = {
      locationId,
      type,
      appEngineIntegrationMode,
    };
    return this.mutate(`databases?databaseId=${databaseId}`, payload);
  }

  /**
   * Saves a group of entities to Firestore in a transaction.
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases.documents/beginTransaction
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases.documents/commit
   * @param {!Object<string,!Object>} entities
   * @param {string=} kind
   * @param {string=} path
   * @return {!Firestore.Commit}
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases.documents/commit#google.firestore.v1.Firestore.Commit
   */
  txSave(entities, kind = this.kind, path = this.path) {
    const urlPath = `databases/${this.databaseId}/documents`;
    const {transaction} = this.mutate(`${urlPath}:beginTransaction`, {
      options: {readWrite: {}},
    });
    const writes = Object.keys(entities).map(key => {
      return {
        update: {
          name: `projects/${this.projectId}/${urlPath}/${path}${kind}/${key}`,
          fields: FIRESTORE_TRANSFORM.toFirestoreObject(entities[key]),
        },
      };
    });
    const payload = {
      writes,
      transaction,
    };
    const response = this.mutate(`${urlPath}:commit`, payload);
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
   *    json:!Object,
   * }>}
   */
  listDocuments() {
    const uri = `databases/${this.databaseId}/documents/${this.path}${this.kind}`;
    const results = super.list('documents', uri);
    return results.map(({name, fields}) => {
      const id = name.substring(name.lastIndexOf('/') + 1);
      const json = FIRESTORE_TRANSFORM.toJson(fields);
      return {id, json};
    });
  }

  /**
   * Lists composite indexes for the specified collection group.
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases.collectionGroups.indexes/list
   * @param {string} collectionGroupId - Collection ID to inspect indexes for.
   * @return {!Array<!Object>} Array of existing index objects.
   */
  listCompositeIndexes(collectionGroupId) {
    const listUri = `databases/${this.databaseId}/collectionGroups/${
      collectionGroupId
    }/indexes`;
    try {
      return super.list('indexes', listUri);
    } catch (error) {
      if (error && (error.code === 404 || error.status === 'NOT_FOUND')) {
        return [];
      }
      throw error;
    }
  }

  /**
   * Idempotently creates a composite index for a Firestore collection group if
   * it does not already exist.
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases.collectionGroups.indexes/create
   * @param {string} collectionGroupId - Target collection group name.
   * @param {!Array<!Object>} fields - Ordered array of index field specs, e.g.
   *     [{fieldPath: 'status', order: 'ASCENDING'}].
   * @param {string=} queryScope - Index query scope, defaults to 'COLLECTION'.
   * @return {!Object} Operation response or existing index reference.
   */
  createOrUpdateCompositeIndex(
    collectionGroupId,
    fields,
    queryScope = 'COLLECTION',
  ) {
    const currentIndexes = this.listCompositeIndexes(collectionGroupId);

    const hasExplicitNameField = (fields || []).some(
      f => f.fieldPath === '__name__',
    );
    const normalizeFields = fieldsList => {
      if (!Array.isArray(fieldsList)) return [];
      if (hasExplicitNameField) return fieldsList;
      return fieldsList.filter(f => f.fieldPath !== '__name__');
    };

    const matchesExpectedFields = (existingFields, desiredFields) => {
      const efList = normalizeFields(existingFields);
      const dfList = normalizeFields(desiredFields);
      if (efList.length !== dfList.length) {
        return false;
      }
      for (let i = 0; i < dfList.length; i++) {
        const ef = efList[i];
        const df = dfList[i];
        if (
          ef.fieldPath !== df.fieldPath ||
          (ef.order !== df.order && ef.arrayConfig !== df.arrayConfig)
        ) {
          return false;
        }
      }
      return true;
    };
    const indexUri = `projects/${this.projectId}/databases/${
      this.databaseId
    }/collectionGroups/${collectionGroupId}/indexes`;
    for (const idx of currentIndexes) {
      if (
        idx.name.startsWith(indexUri) &&
        (idx.queryScope || 'COLLECTION') === queryScope &&
        matchesExpectedFields(idx.fields, fields)
      ) {
        console.log(
          `Matching Firestore composite index already exists on ${
            collectionGroupId
          }:`,
          idx.name,
        );
        return {
          existed: true,
          name: idx.name,
          state: idx.state || 'READY',
        };
      }
    }

    const createUri = `databases/${this.databaseId}/collectionGroups/${
      collectionGroupId
    }/indexes`;
    const payload = {
      queryScope,
      fields,
    };
    console.log(
      `Creating new Firestore composite index on ${collectionGroupId}`,
      JSON.stringify(payload),
    );
    const operation = this.mutate(createUri, payload);
    return this.waitAndResolveOperation(
      operation,
      `Firestore Composite Index on ${collectionGroupId}`,
    );
  }

  /**
   * Returns a link to the Datastore database in Google Cloud Console.
   * @param {string} mode  Firestore mode
   * @param {string} namespace
   * @return {string}
   */
  getLinkToCloudConsole(mode, namespace) {
    const name =
      this.databaseId === DEFAULT_DATABASE ? '-default-' : this.databaseId;
    if (mode === 'FIRESTORE_NATIVE') {
      return (
        'https://console.cloud.google.com/firestore/databases/' +
        `${name}/data/panel/${namespace}/database?project=${this.projectId}`
      );
    } else {
      return (
        'https://console.cloud.google.com/datastore/databases/' +
        `${name}/entities;ns=${namespace}/query/kind?project=${this.projectId}`
      );
    }
  }
}
// Attributes used in Cyborg sheet to manage this resource.
Firestore.ATTRIBUTE_NAMES = Object.freeze({
  mode: 'Mode',
  location: 'Location',
});
Firestore.INDEX_ATTRIBUTE_NAMES = Object.freeze({
  queryScope: 'Query scope',
  fields: 'Fields',
});
