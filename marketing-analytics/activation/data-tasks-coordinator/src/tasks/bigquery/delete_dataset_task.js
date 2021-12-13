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
 * @fileoverview Delete dataset task class.
 */

'use strict';

const {Dataset} = require('@google-cloud/bigquery');
const {BigQueryAbstractTask} = require('./bigquery_abstract_task.js');
const {
  TaskType,
  BigQueryTableConfig
} = require('../../task_config/task_config_dao.js');

/**
 * @typedef {{
 *   type:TaskType.EXPORT_SCHEMA,
 *   source:!BigQueryTableConfig,
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let DeleteDatasetTaskConfig;

/** BigQuery delete dataset task. */
class DeleteDatasetTask extends BigQueryAbstractTask {

  /** @override */
  getBigQueryForTask() {
    /** @const {BigQueryTableConfig} */
    const sourceTable = this.config.source;
    return this.getBigQuery(sourceTable);
  }

  /**
   * Deletes the given dataset.
   * @override
   */
  async doTask() {
    /** @const {BigQueryTableConfig} */ const source = this.config.source;
    /** @const {Dataset} */
    const dataset = this.getBigQueryForTask().dataset(source.datasetId);
    await dataset.delete({force: true});
    this.logger.info(`Deleted ${source.datasetId}`);
    return {};
  }

  /** @override */
  async isDone() {
    return true;
  }
}

module.exports = {
  DeleteDatasetTaskConfig,
  DeleteDatasetTask,
};
