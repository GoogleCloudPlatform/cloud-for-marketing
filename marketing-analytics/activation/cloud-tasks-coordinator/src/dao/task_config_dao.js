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
 * @fileoverview Task Configuration implementation class which is based on
 *     Firestore (native mode or Datastore mode).
 */

'use strict';

const {base: {BaseDao}} = require('nodejs-common');

/**
 * Types of task.
 * @enum {string}
 */
const TaskType = {
  LOAD: 'load',
  QUERY: 'query',
  PREDICT: 'predict',
  EXPORT: 'export',
};
exports.TaskType = TaskType;

/**
 * @typedef {{
 *   projectId:(string|undefined),
 *   datasetId:(string|undefined),
 *   tableId:(string|undefined),
 *   keyFilename:(string|undefined),
 * }}
 */
let BigQueryTableConfig;

exports.BigQueryTableConfig = BigQueryTableConfig;

/**
 * @typedef {{
 *   projectId:(string|undefined),
 *   bucket:(string|undefined),
 *   name:(string|undefined),
 *   keyFilename:(string|undefined),
 * }}
 */
let StorageFileConfig;

exports.StorageFileConfig = StorageFileConfig;

/**
 * Behaviors for how to export the data when the target (output) table exists
 * in BigQuery.
 * See:
 * https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.job.WriteDisposition.html
 * @enum {string}
 */
const WriteDisposition = {
  WRITE_APPEND: 'WRITE_APPEND',      // Append to existent.
  WRITE_EMPTY: 'WRITE_EMPTY',        // Only exports data when there is no data.
  WRITE_TRUNCATE: 'WRITE_TRUNCATE',  // Overwrite existent.
};

exports.WriteDisposition = WriteDisposition;

/**
 * Task configuration data access object on Firestore.
 */
class TaskConfigDao extends BaseDao {

  constructor() {
    super('TaskConfig', 'sentinel');
  }

}

exports.TaskConfigDao = TaskConfigDao;
