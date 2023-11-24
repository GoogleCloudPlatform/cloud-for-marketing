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
 * The type of the database. Mode changes are only allowed if the database is empty.
 * @see https://cloud.google.com/datastore/docs/firestore-or-datastore
 * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases#DatabaseType
 * @enum {string}
 */
const FIRESTORE_MODE = Object.freeze({
  FIRESTORE_NATIVE: 'Native Mode',
  DATASTORE_MODE: 'Datastore Mode',
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

class Firestore extends ApiBase {

  constructor(projectId, databaseId = DEFAULT_DATABASE, path, kind) {
    super();
    this.apiUrl = 'https://firestore.googleapis.com';
    this.version = 'v1';
    this.projectId = projectId;
    this.databaseId = databaseId;
    this.path = path; // ${namespace}/database
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
   * @return {Database}
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases#Database
   */
  getDatabase(databaseId = this.databaseId) {
    return super.get(`databases/${databaseId}`);
  }

  /**
   * Lists information about the supported locations for this service.
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.locations/list
   * @see https://cloud.google.com/datastore/docs/locations
   * @return {!Array<!Location>}
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.locations#Location
   */
  listLocations() {
    return super.get('locations');
  }

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
  createDatabase(locationId, type, databaseId = this.databaseId,
    appEngineIntegrationMode = APP_ENGINE_INTEGRATION_MODE.DISABLED) {
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
   * @param {Object<string,object>} entities
   * @param {string} kind
   * @param {string} namespace
   * @param {string=} databaseId
   * @return {!Firestore.Commit}
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases.documents/commit#google.firestore.v1.Firestore.Commit
   */
  txSave(entities, kind = this.kind, path = this.path) {
    const urlPath = `databases/${this.databaseId}/documents`;
    const { transaction } = super.mutate(`${urlPath}:beginTransaction`,
      { options: { readWrite: {} } });
    const writes = Object.keys(entities).map((key) => {
      return {
        update: {
          name: `projects/${this.projectId}/${urlPath}/${path}/${kind}/${key}`,
          fields: this.convertObject_(entities[key]),
        },
      };
    });
    const payload = {
      writes,
      transaction,
    };
    const response = super.mutate(`${urlPath}:commit`, payload);
    if (response.error) {
      throw new Error(response.error.message);
    }
    return response;
  }

  /**
   * Returns a Firestore 'Value' for a Javascript object.
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases.documents#Document
   * @param {object} obj A JSON object.
   * @return {!Value} Firestore object.
   * @private
   */
  convertObject_(obj) {
    const converted = {};
    Object.keys(obj).forEach((key) => {
      converted[key] = this.getFirestoreValue_(obj[key]);
    })
    return converted;
  }

  /**
   * Returns a Firestore 'Value' for a Javascript value.
   * @see https://cloud.google.com/firestore/docs/reference/rest/Shared.Types/ArrayValue#Value
   * @param {string|number|boolean Array|Object} value A Javascript value.
   * @return {!Value} Firestore object.
   * @private
   */
  getFirestoreValue_(value) {
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
        propertyValue = { values: value.map(this.getFirestoreValue_.bind(this)) };
      } else {
        propertyName = 'mapValue';
        propertyValue = { fields: this.convertObject_(value) };
      }
    }
    if (!propertyName) throw new Error(`Unrecognizable value ${value}`);
    return { [propertyName]: propertyValue };
  }

}
