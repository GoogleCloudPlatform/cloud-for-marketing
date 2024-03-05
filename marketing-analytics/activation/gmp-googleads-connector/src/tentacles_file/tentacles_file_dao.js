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
 * @fileoverview Tentacles File implementation class which is based on
 *     Firestore (native mode or Datastore mode).
 */

'use strict';

const {
  firestore: { Database, DataAccessObject },
  utils: { getLogger, }
} = require('@google-cloud/nodejs-common');

/**
 * Status of Tentacles File.
 * @enum {string}
 */
const TentaclesFileStatus = {
  QUEUING: 'queuing',
  STARTED: 'started',
  CANCELED: 'canceled',
  DONE: 'done',
  ERROR: 'error',
};

/**
 * The entity object of TentaclesFile
 * @typedef {{
 *   name:string,
 *   bucket:string,
 *   fileSize:string,
 *   updated:string,
 *   startQueuingTime:!Date|undefined,
 *   startSendingTime:!Date|undefined,
 *   status: !TentaclesFileStatus,
 *   numberOfTasks: number,
 *   failedTasks: (Array<{{
 *      start: number|undefined,
 *      end: number|undefined,
 *      slicedFile: string|undefined,
 *      reason: string,
 *   }}>|undefined),
 *   error:(string|undefined),
 *   attributes:{
 *     api:(undefined|string),
 *     config:(undefined|string),
 *     dryRun:(undefined|string),
 *     gcs:(undefined|string),
 *     size:(undefined|string),
 *     topic:(undefined|string),
 *   }
 * }}
 */
let TentaclesFile;

/**
 * Tentacles File data access object.
 */
class TentaclesFileDao extends DataAccessObject {
  /**
   * Initializes TentaclesFile Dao instance.
   * @param {!Database} database The database.
   * @param {string} namespace The namespace of the data.
   */
  constructor(database, namespace = 'tentacles') {
    super('File', namespace, database);
    // This logger is used to send data to BigQuery for the dashboard.
    this.loggerForDashboard = getLogger('TentaclesFile');
  }

  /**
   * Creates the file entity based on the given information.
   * @param {!TentaclesFile} file File information.
   * @return {!Promise<string|number>} Saved File document/entity ID.
   */
  async createFile(file) {
    const entity = Object.assign({}, file,
      { startQueuingTime: new Date(), status: TentaclesFileStatus.QUEUING, });
    const fileId = await this.create(entity);
    this.loggerForDashboard.info(
      JSON.stringify(Object.assign({ action: 'create', fileId }, entity)));
    return fileId;
  }

  /**
   * Updates the file Entity with the given information.
   * @param {string|number} fileId File ID.
   * @param {!TentaclesFile} entity Updated entity information.
   * @return {!Promise<string|number>} Saved File document/entity ID.
   */
  async updateFile(fileId, entity) {
    const result = await this.merge(entity, fileId);
    this.loggerForDashboard.info(
      JSON.stringify(Object.assign({ action: 'update', fileId }, entity)));
    return result;
  }

  /**
   * Saves the error message of this file.
   * @param {string|number} fileId File ID.
   * @param {string} error Error message for a file.
   * @return {!Promise<string|number>} Saved File document/entity ID.
   */
  async logError(fileId, error) {
    const entity = { status: TentaclesFileStatus.ERROR, error };
    const result = await this.merge(entity, fileId);
    this.loggerForDashboard.info(
      JSON.stringify(Object.assign({ action: 'logError', fileId }, entity)));
    return result;
  }
}

module.exports = {
  TentaclesFileStatus,
  TentaclesFile,
  TentaclesFileDao,
};
