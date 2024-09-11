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
 *     reportToRefresh: ({
 *        datasetId: string,
 *        baseTable: string,
 *        deltaTable: string,
 *        reportTask: string
 *     }|undefined),
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
    const destinationTable = this.config.destination
      ? this.config.destination.table : {};
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
    const options = {
      query: sql,
      params: {},
    };
    if (this.config.destination) {
      const { table, writeDisposition } = this.config.destination;
      /** @const {BigQueryTableConfig} */
      options.destinationTable = table;
      options.writeDisposition = writeDisposition;
      if (table.tableId.includes('$')) {
        options.timePartitioning = {
          type: 'DAY',
          requirePartitionFilter: false,
        };
      }
    }
    try {
      const [job] = await this.getBigQueryForTask().createQueryJob(options);
      const { jobReference } = job.metadata;
      const { jobId } = jobReference;
      const status = job.metadata.status.state;
      const sqlInfo = this.getSqlInfo_();
      this.logger.info(`Job[${jobId}] status ${status} on query [${sqlInfo}]`);
      const appendedParameters = { jobReference };
      if (options.destinationTable) {
        appendedParameters.destinationTable = options.destinationTable;
      }
      return {
        jobId,
        parameters: this.appendParameter(appendedParameters),
      };
    } catch (error) {
      this.logger.error(`Error in query to BigQuery:`, error);
      this.logger.error(options.query);
      throw (error);
    }
  }

  /**
   * Returns a string as the label of the sql for logs.
   * @return {!Promise<string>}
   * @private
   */
  getSqlInfo_() {
    const { sql, file, reportToRefresh } = this.config.source;
    if (sql) return sql;
    if (file) return `file: ${file.name}`;
    if (reportToRefresh) {
      return `refresh report: ${reportToRefresh.reportTask}`;
    }
    return 'Unknown sql info';
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
    if (this.config.source.reportToRefresh) {
      const sql =
        await this.getRefreshSqlFromReport_(this.config.source.reportToRefresh);
      return sql;
    }
    throw new Error(`Fail to find source sql or file for Query Task `);
  }

  /**
   * Returns a Promise of the query sql based on configuration.
   * @return {!Promise<string>}
   * @private
   */
  async getRefreshSqlFromReport_(config) {
    const { datasetId, baseTable, deltaTable, reportTask } = config;
    const { source } = await this.options.taskConfigDao.load(reportTask);
    if (source.target !== 'ADS') {
      throw new Error(
        `Unsupported ${taskConfig.source.target} report for refresh query.`);
    }
    const { attributes, segments } = source.config.reportQuery;
    const columns = attributes.concat(segments);
    return `
      WITH date_range AS (
        SELECT MIN(segments.date) startDate, MAX(segments.date) endDate
        FROM \`${datasetId}.${deltaTable}\`
      )
      SELECT * EXCEPT (updated, rank)
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY ${columns.join(',')} ORDER BY updated DESC
          ) AS rank
        FROM (
          SELECT *, 1 AS updated FROM \`${datasetId}.${deltaTable}\`
          UNION ALL
          SELECT *, 0 AS updated FROM \`${datasetId}.${baseTable}\`
        )
      )
      WHERE rank = 1
        AND segments.date BETWEEN
          (SELECT startDate FROM date_range) AND (SELECT endDate FROM date_range)
    `;
  }
}

module.exports = {
  QueryTaskConfig,
  QueryTask,
};
