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
  utils: {getLogger,}
} = require('@google-cloud/nodejs-common');
const {TentaclesFile} = require('./tentacles_file.js');

/**
 * Tentacles File data object on Firestore.
 * @implements {TentaclesFile}
 */
class TentaclesFileOnFirestore extends DataAccessObject {
  /**
   * Initializes TentaclesFile Dao instance.
   * @param {!Database} database The database.
   * @param {string} namespace The namespace of the data.
   */
  constructor(database, namespace = 'tentacles') {
    super('File', namespace, database);
    this.logger = getLogger('TentaclesFile');
  }

  /** @override */
  async save(file) {
    const fileId = await this.create(file);
    this.logger.info(
        JSON.stringify(Object.assign({action: 'save', fileId}, file)));
    return fileId;
  }

  /** @override */
  saveError(fileId, error) {
    this.logger.info(JSON.stringify({action: 'saveError', fileId, error}));
    return this.merge({error}, fileId);
  }
}

module.exports = {TentaclesFileOnFirestore};
