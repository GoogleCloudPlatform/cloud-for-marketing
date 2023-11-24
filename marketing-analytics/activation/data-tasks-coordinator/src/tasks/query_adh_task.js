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
const { BaseTask } = require('./base_task.js');
const {TaskType, BigQueryTableConfig,} = require(
    '../task_config/task_config_dao.js');
const {ErrorOptions} = require('../task_config/task_config_dao.js');

/**
 * @typedef {{
 *   type:TaskType.QUERY_ADH,
 *   source:{
 *     customerId:string,
 *     title: (string|undefined),
 *     sql:(string|undefined),
 *     file:(!StorageFileConfig|undefined),
 *     parameterTypes:({string:ParameterType}|undefined),
 *     spec: {
 *       startDate: {
 *         year: number,
 *         month: number,
 *         day: number,
 *       },
 *       endDate: {
 *         year: number,
 *         month: number,
 *         day: number,
 *       },
 *       parameterValues:({string:ParameterValue}|undefined),
 *     },
 *   },
 *   destination:{
 *     table:!BigQueryTableConfig,
 *   },
 *   errorOptions:(!ErrorOptions|undefined),
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let QueryAdhTaskConfig;

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
    const tableName = this.getDestTable(this.config.destination.table);
    const { customerId, title } = source;
    const spec = source.spec
      || this.getLegacySpec_(source.endDate, source.dateRangeInDays);
    const sql = await this.getSql_();
    const adh = this.getAdhInstance_(customerId);

    if (sql) {
      const query = {
        queryText: sql,
      };
      if (source.parameterTypes) {
        query.parameterTypes = source.parameterTypes;
      }
      const { name } = await adh.startTransientQuery(query, spec, tableName);
      return {
        parameters: this.appendParameter({ [JOB_ID_PARAMETER]: name })
      };
    }
    if (title) {
      const { queries } = await adh.listQuery({ filter: `title=${title}` });
      if (queries && queries.length > 0) {
        const queryName = queries[0].name;
        const { name } = await adh.startQuery(queryName, spec, tableName);
        return {
          parameters: this.appendParameter({ [JOB_ID_PARAMETER]: name })
        };
      }
    }
    throw new Error(
      `Can not find ADH query for task: ${JSON.stringify(source)}`);
  }

  /** @override */
  async isDone() {
    const { customerId } = this.config.source;
    const operationName = this.parameters[JOB_ID_PARAMETER];
    const adh = this.getAdhInstance_(customerId);
    const response = await adh.getQueryStatus(operationName);
    const { done, error } = response;
    if (error) {
      this.logger.error('ADH query task failed, will NOT retry: ',
        error.toString());
      throw new Error(error.message);
    }
    return done === true;
  }

  /**
   * Returns ADH connector instance.
   * @param {string} customerId
   * @return {!AdsDataHub}
   * @private
   */
  getAdhInstance_(customerId) {
    return new AdsDataHub({}, customerId, this.getOption());
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
  }

  /**
   * Returns ADH query destination table.
   * @return {string}
   */
  getDestTable(table) {
    const { projectId, datasetId, tableId } = table;
    const result = [tableId];
    if (datasetId) {
      result.unshift(datasetId);
      if (projectId) result.unshift(projectId);
    }
    return result.join('.');
  }

  /**
   * Gets option object to create a new API object.
   * By default, the API classes will figure out the authorization from env
   * variables. The authorization can also be set in the 'config' so each
   * integration can have its own authorization. This function is used to get the
   * authorization related information from the 'config' object and form an
   * 'option' object for the API classes.
   * TODO: This function is copied from 'base_report.js'. Refactory required.
   * @return {{SECRET_NAME:(string|undefined)}}
   */
  getOption() {
    const options = {};
    if (this.config.secretName) options.SECRET_NAME = this.config.secretName;
    return options;
  }

  /**
   * Gets the `spec` object for a ADH query. This is a legacy implementation and
   * should not be used.
   * @param {string} endDate The end date.
   * @param {number} dateRangeInDays The number of day of query period.
   * @return {object}
   * @private
   */
  getLegacySpec_(endDate, dateRangeInDays) {
    if (!endDate) return;
    const endDateTime = DateTime.fromISO(endDate);
    const startDateTime = endDateTime.minus({ days: dateRangeInDays });
    return {
      startDate: {
        year: startDateTime.year,
        month: startDateTime.month,
        day: startDateTime.day,
      },
      endDate: {
        year: endDateTime.year,
        month: endDateTime.month,
        day: endDateTime.day,
      },
    };
  }
}

module.exports = {
  QueryAdhTaskConfig,
  QueryAdhTask,
};
