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

/**
 * @fileoverview Offers a factory function to get an instance of
 *     TentacleTaskBase.
 */

'use strict';
const {FirestoreAccessBase: {DataSource}} = require('nodejs-common');
const {TentaclesTaskBase, TaskStatus, TentaclesTaskEntity} =
    require('./tentacles_task_impl/tentacles_task_base.js');
const {TentaclesTaskOnFirestore} =
    require('./tentacles_task_impl/tentacles_task_firestore.js');

exports.TaskStatus = TaskStatus;
exports.TentaclesTaskEntity = TentaclesTaskEntity;
exports.TentaclesTaskBase = TentaclesTaskBase;

/**
 * Gets an instance to manage Tentacles Task.
 * @param {!DataSource|undefined=} dataSource The name of data source.
 * @return {!TentaclesTaskBase} The object to manage Tentacles Task.
 */
exports.getTentaclesTask = (dataSource = undefined) => {
  switch (dataSource) {
    case DataSource.FIRESTORE:
    case DataSource.DATASTORE:
      return new TentaclesTaskOnFirestore(dataSource);
    default:
      console.log(
          `Unknown dataSource[${dataSource}] for Task. Using dummy one.`);
      return new TentaclesTaskBase();
  }
};
