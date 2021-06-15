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
 * @fileoverview Query Ads Data Hub task class.
 */

'use strict';
const {DateTime} = require('luxon');
const {
  storage: {StorageFile},
  utils: {replaceParameters},
} = require('@google-cloud/nodejs-common');
const {api: {adsdatahub: {AdsDataHub}}} = require(
    '@google-cloud/nodejs-common');
const {BaseTask, RetryableError,} = require('./base_task.js');
const {TaskType, BigQueryTableConfig,} = require(
    '../task_config/task_config_dao.js');
const {ErrorOptions} = require('../task_config/task_config_dao.js');

/**
 * @typedef {{
 *   type:TaskType.QUERY_ADH,
 *   adhConfig:{
 *      customerId:string,
 *   },
 *   source:{
 *     sql:(string|undefined),
 *     file:(!StorageFileConfig|undefined),
 *     endDate:string,
 *     dateRangeInDays:30
 *   },
 *   destination:{
 *     table:!BigQueryTableConfig,
 *   },
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let QueryAdhTaskConfig;

/** @const {string} The query ADH job identity name. */
const QUERY_ID_PARAMETER = 'queryName';
/** @const {string} The query ADH job identity name. */
const JOB_ID_PARAMETER = 'operationName';

/**
 * Query Adh Task acts query clients ADH data and store the result in
 * destination BigQuery table. It will create a query, run with a date spec, and
 * delete the query once the operation is finished.
 */
class QueryAdhTask extends BaseTask {

  /** @override */
  isManualAsynchronous() {
    return true;
  }

  /** @override */
  async doTask() {
    const source = this.config.source;
    const destTable = this.config.destination.table;
    const sql = await this.getSql_();
    const adh = this.getAdhInstance_(this.config.adhConfig)
    const queryName = await adh.createQuery(destTable.tableId, sql);

    const endDateTime = DateTime.fromISO(source.endDate);
    const startDateTime = endDateTime.minus({days: source.dateRangeInDays});
    const spec = {
      startDate:{
        year: startDateTime.year,
        month: startDateTime.month,
        day: startDateTime.day,
      },
      endDate:{
        year: endDateTime.year,
        month: endDateTime.month,
        day: endDateTime.day,
      },
    };

    const tableName = destTable.projectId + '.' + destTable.datasetId + '.'
        + destTable.tableId;
    const {name: opName} = await adh.startQuery(queryName, spec, tableName);

    return {
      parameters: this.appendParameter({
        [QUERY_ID_PARAMETER]: queryName,
        [JOB_ID_PARAMETER]: opName,
      })
    };
  }

  /** @override */
  async isDone() {
    const adhConfig = this.config.adhConfig;
    const param = this.parameters[JOB_ID_PARAMETER];

    // Request the newly created operation too soon will result in 404 error.
    // TODO(xinxincheng): change to graceful wait once confirm the above issue.
    await new Promise(resolve => setTimeout(resolve, 5000));

    const {done, error} = await this.getAdhInstance_(
        this.config.adhConfig).getQueryStatus(param);

    if (error) {
      await this.completeTask();
      throw new Error(error.message);
    }
    return !!done;
  }

  /** @override */
  completeTask() {
    const adhConfig = this.config.adhConfig;
    return this.getAdhInstance_(adhConfig).deleteQuery(
        this.parameters[QUERY_ID_PARAMETER]);
  }

  /**
   * Returns ADH connector instance.
   * @return {!AdsDataHub}
   * @private
   */
  getAdhInstance_(adhConfig) {
    return new AdsDataHub({}, adhConfig.customerId);
  }

  /**
   * Returns a Promise of the query sql based on configuration.
   * @return {!Promise<string>}
   * @private
   */
  async getSql_() {
    if (this.config.source.sql) {
      return this.config.source.sql;
    }
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
  QueryAdhTaskConfig,
  QueryAdhTask,
};
