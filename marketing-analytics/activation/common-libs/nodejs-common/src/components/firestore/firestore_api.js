// Copyright 2023 Google Inc.
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

const { GoogleCloudApi } = require('../../apis/cloud/google_cloud_api');

/**
 * The name of default database.
 * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases#resource:-database
 * @const {string}
 */
const DEFAULT_DATABASE = '(default)';

/**
 * The modes of Firestore. Mode changes are only allowed if the database is empty.
 * @see https://cloud.google.com/datastore/docs/firestore-or-datastore
 * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases#DatabaseType
 * @enum {string}
 */
const FIRESTORE_MODE = Object.freeze({
  FIRESTORE_NATIVE: 'FIRESTORE',
  DATASTORE_MODE: 'DATASTORE',
});

class FirestoreApi extends GoogleCloudApi {

  constructor(env = process.env, options = {}) {
    super(env, options);
    this.apiUrl = 'https://firestore.googleapis.com';
    this.version = 'v1';
    this.databaseId = options.databaseId || env.DATABASE_ID || DEFAULT_DATABASE;
  }

  /** @override */
  async getBaseUrl() {
    const projectId = await this.getProjectId();
    return `${this.apiUrl}/${this.version}/projects/${projectId}`;
  }

  /**
   * Gets the information of the default database.
   * If the database is ready, will return:
   * {
   *   data: {
   *     name:...
   *     type:...
   *   },
   *   code: 200
   * }
   * If the database is not created, will throw an error: "Request failed with
   * status code 404"
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases/get
   * @return {Database}
   * @see https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases#Database
   */
  async getDatabase(databaseId = this.databaseId) {
    return super.request(`databases/${databaseId}`);
  }

  /**
   * Gets the mode of the Firestore database.
   * @param {string=} databaseId
   * @return {'FIRESTORE'|'DATASTORE'} Mode of the Firestore.
   */
  async getFirestoreMode(databaseId = this.databaseId) {
    const response = await this.getDatabase(databaseId);
    if (!response.data) {
      this.logger.error(`Fail to find database ${databaseId}`, response);
      throw new Error(`Fail to find database ${databaseId}`);
    }
    this.logger.debug(`Get ${databaseId} in mode ${response.data.type}.`);
    return FIRESTORE_MODE[response.data.type];
  }
}

module.exports = {
  DEFAULT_DATABASE,
  FirestoreApi,
};
