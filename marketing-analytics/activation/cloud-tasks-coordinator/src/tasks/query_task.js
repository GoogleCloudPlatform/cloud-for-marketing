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
 * @fileoverview Task base class.
 */

'use strict';
const {StorageUtils} = require('nodejs-common');
const {BaseTask} = require('./base_task.js');
const {
  TaskType,
  BigQueryTableConfig,
  StorageFileConfig,
  WriteDisposition,
} = require('../dao/task_config_dao.js');
const {utils: {replaceParameters}} = require('nodejs-common');

/**
 * @typedef {{
 *   type:TaskType.QUERY,
 *   source:{
 *     sql:(string|undefined),
 *     file:(!StorageFileConfig|undefined)
 *   },
 *   destination:{
 *     table:!BigQueryTableConfig,
 *     writeDisposition:!WriteDisposition,
 *   },
 *   source:(!BigQueryTableConfig|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let QueryTaskConfig;

exports.QueryTaskConfig = QueryTaskConfig;

class QueryTask extends BaseTask {

  /** @override */
  doTask() {
    let getSql;
    if (this.config.source.sql) {
      getSql = Promise.resolve(this.config.source.sql);
    } else if (this.config.source.file) {
      const file = this.config.source.file;
      const storageUtils = new StorageUtils(file.bucket, file.name);
      getSql = storageUtils.loadContent(0);
    } else {
      throw new Error(`Fail to find source sql or file for Query Task `);
    }
    return getSql.then((sql) => {
      const tableOptions = this.config.destination.table;
      const options = {
        query: replaceParameters(sql, this.parameters),
        params: {},
        destinationTable: tableOptions,
        writeDisposition: this.config.destination.writeDisposition,
      };
      if (tableOptions.tableId.indexOf('$') > -1) {
        options.timePartitioning = {type: 'DAY', requirePartitionFilter: false};
      }
      return super.getBigQuery(tableOptions).createQueryJob(options).then(
          ([job]) => {
            console.log(`Create job[${job.id}] on query [${
                options.query.split('\n')[0]}]`);
            return job.id;
          })
          .catch((error) => {
            console.error(`Error in query to BigQuery:`, error);
            console.error(options.query);
            throw (error);
          });
    });
  }
}

exports.QueryTask = QueryTask;