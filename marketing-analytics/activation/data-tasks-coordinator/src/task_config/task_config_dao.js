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

const lodash = require('lodash');
const {firestore: {DataSource, DataAccessObject}} = require(
    '@google-cloud/nodejs-common');

/**
 * Types of task.
 * @enum {string}
 */
const TaskType = {
  // BigQuery tasks
  LOAD: 'load',
  QUERY: 'query',
  EXPORT: 'export',
  DATA_TRANSFER: 'data_transfer',
  EXPORT_SCHEMA: 'export_schema',
  DELETE_DATASET: 'delete_dataset',
  // GMC Feed convertor tasks
  DOWNLOAD: 'download',
  GMC_XML_FEED_TO_JSONL: 'gmc_xml_feed_to_jsonl',
  GMC_WEBPAGE_FETECHER: 'gmc_webpage_fetcher',
  // Cloud Storage tasks
  COPY_GCS: 'copy_gcs',
  // AutoML Tables API
  PREDICT: 'predict',
  // Report tasks
  REPORT: 'report',
  QUERY_ADH: 'query_adh',
  // Notify task, e.g. send email
  NOTIFY: 'notify',
  // Tasks dependencies management tasks
  KNOT: 'knot',
  MULTIPLE: 'multiple',
};

/**
 * Type for a group of tasks, e.g. 'next', 'embedded'.
 * It can be a string of comma-separated taskIds, an array of taskIds or an
 * array of objects which contain 'taskId' and 'appendedParameters'. The last
 * use case can support passing extra parameters to the corresponding task.
 * @typedef {string|Array<string>|
 *     Array<{taskId:string, appendedParameters:object}>}
 */
let TaskGroup;

/**
 * Options to set the way of error handling.
 *  1. 'retryTimes' retry how many times before failing the task. Note, retries
 *     only attempted after a RetryableError. @see RetryableError.
 *  2. 'ignoreError' when a task is failed, whether to continue to its next
 *     task. One example is in Google Ads report tasks, a MCC account may have
 *     many CIDs, some CIDs may be unavailable through API due to its status. If
 *     this happened, it's ok to ignore this report to exclude this account from
 *     all the reports.
 * @typedef {{
 *   retryTimes:number|undefined,
 *   ignoreError:boolean|undefined,
 * }}
 */
let ErrorOptions;

/** @const{number} Default retry times. */
const DEFAULT_RETRY_TIMES = 3;

/**
 * A task config can inherit settings from its 'ancestor' task config, and so
 * on. To prevent a loop, this is the most allowed hierarchy generations.
 * @const{number}
 */
const MAX_ALLOWED_GENERATIONS = 10;

/**
 * Behaviors for how to export the data when the target (output) table exists
 * in BigQuery.
 * See:
 * https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.job.WriteDisposition.html
 * @enum {string}
 */
const WriteDisposition = {
  WRITE_APPEND: 'WRITE_APPEND',      // Append to existent.
  WRITE_EMPTY: 'WRITE_EMPTY',        // Only export data when there is no data.
  WRITE_TRUNCATE: 'WRITE_TRUNCATE',  // Overwrite existing.
};

/**
 * @typedef {{
 *   projectId:(string|undefined),
 *   datasetId:(string|undefined),
 *   tableId:(string|undefined),
 *   location:(string|undefined),
 *   keyFilename:(string|undefined),
 * }}
 */
let BigQueryTableConfig;

/**
 * @typedef {{
 *   projectId:(string|undefined),
 *   bucket:(string|undefined),
 *   name:(string|undefined),
 *   keyFilename:(string|undefined),
 * }}
 */
let StorageFileConfig;

/**
 * Task configuration data access object on Firestore.
 */
class TaskConfigDao extends DataAccessObject {

  /**
   * Initializes TaskConfig Dao instance.
   * @param {!DataSource} dataSource The data source type.
   * @param {string} namespace The namespace of the data. The default value is
   *   'sentinel'.
   * @param {string=} projectId GCP project Id. The default value is the env
   *   variable 'GCP_PROJECT'.
   */
  constructor(dataSource, namespace = 'sentinel',
    projectId = process.env['GCP_PROJECT']) {
    super('TaskConfig', namespace, dataSource, projectId);
  }

  /**
   * Gets the task configuration with ancestor task configuration is inherited.
   * @param {string|number} id TaskConfig Id.
   * @param {number=} generation The generation of the loaded TaskConfig. It
   *   can't be greater than `MAX_ALLOWED_GENERATIONS` to prevent a hierarchy
   *   loop.
   * @return {!Promise<(!Entity|undefined)>}
   */
  async load(id, generation = 0) {
    if (generation > MAX_ALLOWED_GENERATIONS)
      throw new Error(`TaskConfig ${id} Exceed the max inheritable times.`);
    const config = await super.load(id);
    if (!config || !config.ancestor)
      return config;
    const ancestor = await this.load(config.ancestor, ++generation);
    return lodash.merge({}, ancestor, config);
  }
}

module.exports = {
  TaskType,
  TaskGroup,
  ErrorOptions,
  WriteDisposition,
  BigQueryTableConfig,
  StorageFileConfig,
  TaskConfigDao,
  DEFAULT_RETRY_TIMES,
};
