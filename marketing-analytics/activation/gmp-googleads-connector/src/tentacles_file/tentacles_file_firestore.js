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

const {firestore: {DataSource, DataAccessObject}} = require(
    '@google-cloud/nodejs-common');
const {TentaclesFile} = require('./tentacles_file.js');

/**
 * Tentacles File data object on Firestore.
 * @implements {TentaclesFile}
 */
class TentaclesFileOnFirestore extends DataAccessObject {
  /**
   * Initializes TentaclesFile Dao instance.
   * @param {!DataSource} dataSource The data source type.
   * @param {string} namespace The namespace of the data.
   */
  constructor(dataSource, namespace = 'tentacles') {
    super('File', namespace, dataSource);
  }

  /** @override */
  save(file) {
    const entity = {
      name: file.name,
      bucket: file.bucket,
      size: file.size,
      updated: file.updated,
    };
    return this.create(entity);
  }

  /** @override */
  saveError(fileId, error) {
    return this.merge({error}, fileId);
  }
}

module.exports = {TentaclesFileOnFirestore};
