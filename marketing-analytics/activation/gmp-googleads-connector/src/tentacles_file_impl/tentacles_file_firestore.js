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
  FirestoreAccessBase: {DataSource, FirestoreAccessBase},
  NativeModeAccess,
  DatastoreModeAccess,
} = require('nodejs-common');
const {TentaclesFileBase} = require('./tentacles_file_base.js');

/**
 * Tentacles File data object on Firestore.
 */
class TentaclesFileOnFirestore extends TentaclesFileBase {
  /**
   * Initializes the instance based on given data source.
   * @param {!DataSource} dataSource The name of data source.
   */
  constructor(dataSource) {
    super();
    /** @type {!FirestoreAccessBase} */ this.fileAccessObject;
    switch (dataSource) {
      case DataSource.FIRESTORE:
        this.fileAccessObject = new NativeModeAccess('tentacles/database/File');
        break;
      case DataSource.DATASTORE:
        this.fileAccessObject = new DatastoreModeAccess('tentacles', 'File');
        break;
      default:
        throw new Error(`Unknown DataSource item: ${dataSource}.`);
    }
  }

  /** @override */
  save(file) {
    const entity = {
      name: file.name,
      bucket: file.bucket,
      size: file.size,
      updated: file.updated,
    };
    return this.fileAccessObject.saveObject(entity);
  }

  /** @override */
  saveError(fileId, error) {
    return this.fileAccessObject.getObject(fileId).then((file) => {
      file.error = error;
      return this.fileAccessObject.saveObject(file, fileId);
    });
  }
}

exports.TentaclesFileOnFirestore = TentaclesFileOnFirestore;
