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

const { Dataset } = require('@google-cloud/bigquery');
const { BigQueryAbstractTask } = require('./bigquery_abstract_task.js');
const {
  TaskType,
  BigQueryTableConfig
} = require('../../task_config/task_config_dao.js');

/**
 * @typedef {{
 *   type:TaskType.DELETE_BIGQUERY,
 *   source:!BigQueryTableConfig,
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let DeleteBigQueryTaskConfig;

/** BigQuery delete dataset/table task. */
class DeleteBigQueryTask extends BigQueryAbstractTask {

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
    if (source.tableId) {
      const table = dataset.table(source.tableId);
      const [tableExists] = await table.exists();
      if (tableExists) {
        await table.delete({ ignoreNotFound: true });
        this.logger.info(`Deleted table ${source.tableId}`);
      } else {
        this.logger.info(
          `Table [${source.tableId}] to be deleted doesn't exist.`);
      }
    } else {
      await dataset.delete({ force: true });
      this.logger.info(`Deleted dataset ${source.datasetId}`);
    }
  }

  /** @override */
  async isDone() {
    return true;
  }

  /** @override */
  completeTask() {
    return {};
  }
}

module.exports = {
  DeleteBigQueryTaskConfig,
  DeleteBigQueryTask,
};
