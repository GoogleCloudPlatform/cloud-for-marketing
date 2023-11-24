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
 * @fileoverview Saves information of files that Tentacles handled.
 */

'use strict';
const {firestore: {DataSource}} = require('@google-cloud/nodejs-common');

const {TentaclesFile, TentaclesFileEntity} = require('./tentacles_file.js');
const {TentaclesFileDummy} = require('./tentacles_file_dummy.js');
const {TentaclesFileOnFirestore} = require('./tentacles_file_firestore.js');

/**
 * Gets an instance to manage Tentacles File.
 * @param {!DataSource|undefined=} dataSource The name of data source.
 * @param {string|undefined=} namespace
 * @return {!TentaclesFile} The object to manage Tentacles File.
 * @deprecated
 */
const getTentaclesFile = (dataSource = undefined, namespace = undefined) => {
  switch (dataSource) {
    case DataSource.FIRESTORE:
    case DataSource.DATASTORE:
      return new TentaclesFileOnFirestore(dataSource, namespace);
    default:
      console.log(
          `Unknown dataSource[${dataSource}] for File. Using dummy one.`);
      return new TentaclesFileDummy();
  }
};

module.exports = {
  TentaclesFileEntity,
  TentaclesFile,
  getTentaclesFile,
  TentaclesFileOnFirestore,
};
