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
 * @fileoverview Query task class.
 */

'use strict';
const {
  storage: {StorageFile},
  utils: {replaceParameters},
} = require('@google-cloud/nodejs-common');
const {BigQueryAbstractTask} = require('./bigquery_abstract_task.js');
const {
  TaskType,
  BigQueryTableConfig,
  StorageFileConfig,
  WriteDisposition,
} = require('../../task_config/task_config_dao.js');

/**
 * @typedef {{
 *   type:TaskType.QUERY,
 *   source:{
 *     sql:(string|undefined),
 *     file:(!StorageFileConfig|undefined),
 *     external:(boolean|undefined),
 *   },
 *   destination:{
 *     table:!BigQueryTableConfig,
 *     writeDisposition:!WriteDisposition,
 *   },
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let QueryTaskConfig;

/** Executes BigQuery query and exports result to a table task class. */
class QueryTask extends BigQueryAbstractTask {

  /** @override */
  getBigQueryForTask() {
    /** @const {BigQueryTableConfig} */
    const destinationTable = this.config.destination.table;
    const {external} = this.config.source;
    return this.getBigQuery(destinationTable, external);
  }

  /**
   * Gets a SQL from string in config or Cloud Storage file and creates a
   * BigQuery query job to export the result to a BigQuery table.
   * @override
   */
  async doTask() {
    const sql = await this.getSql_();
    /** @const {BigQueryTableConfig} */
    const destinationTable = this.config.destination.table;
    const options = {
      query: sql,
      params: {},
      destinationTable,
      writeDisposition: this.config.destination.writeDisposition,
    };
    if (destinationTable.tableId.includes('$')) {
      options.timePartitioning = {
        type: 'DAY',
        requirePartitionFilter: false,
      };
    }
    try {
      const [job] = await this.getBigQueryForTask().createQueryJob(options);
      const { jobReference } = job.metadata;
      const { jobId } = jobReference;
      const status = job.metadata.status.state;
      const sqlInfo = this.config.source.sql || this.config.source.file.name;
      this.logger.info(`Job[${jobId}] status ${status} on query [${sqlInfo}]`);
      return {
        jobId,
        parameters: this.appendParameter({ destinationTable, jobReference }),
      };
    } catch (error) {
      this.logger.error(`Error in query to BigQuery:`, error);
      this.logger.error(options.query);
      throw (error);
    }
  }

  /**
   * Returns a Promise of the query sql based on configuration.
   * @return {!Promise<string>}
   * @private
   */
  async getSql_() {
    if (this.config.source.sql) return this.config.source.sql;
    if (this.config.source.file) {
      /** @const {StorageFileConfig} */
      const {bucket, name, projectId} = this.config.source.file;
      const sql = await StorageFile
          .getInstance(bucket, name, {projectId}).loadContent(0);
      return replaceParameters(sql, this.parameters);
    }
    throw new Error(`Fail to find source sql or file for Query Task `);
  }
}

module.exports = {
  QueryTaskConfig,
  QueryTask,
};
