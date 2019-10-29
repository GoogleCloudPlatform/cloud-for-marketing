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

const {Table} = require('@google-cloud/bigquery');
const {BaseTask} = require('./base_task.js');
const {TaskType, BigQueryTableConfig, WriteDisposition} = require(
    '../dao/task_config_dao.js');

/**
 * @typedef {{
 *     schema:{
 *       fields:!Array<{
 *         mode:string,
 *         name:string,
 *         type:string,
 *       }>,
 *     },
 *     timePartitioning:({
 *       type:string,
 *       requirePartitionFilter:boolean,
 *     }|undefined),
 * }}
 */
let TableSchema;

exports.TableSchema = TableSchema;

/**
 * Options for loading Cloud Storage file into BigQuery.
 * @typedef{{
 *     sourceFormat:string,
 *     writeDisposition:!WriteDisposition,
 *     skipLeadingRows:number,
 *     autodetect:boolean,
 *     compression:(boolean|undefined),
 * }}
 */
let LoadOptions;

exports.LoadOptions = LoadOptions;

/**
 * @typedef {{
 *   type:TaskType.LOAD,
 *   source:({fileNamePattern:string}|undefined),
 *   destination:{
 *     table:!BigQueryTableConfig,
 *     tableSchema:!TableSchema,
 *   },
 *   options:!LoadOptions,
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let LoadTaskConfig;

exports.LoadTaskConfig = LoadTaskConfig;

class LoadTask extends BaseTask {

  /** @override */
  doTask() {
    const fileObj = super.getStorage(this.config.source)
        .bucket(this.config.source.bucket)
        .file(this.config.source.name);
    const metadata = this.config.options;
    const destination = this.config.destination;
    metadata.compression = this.isCompressed_(this.config.source.name);
    console.log(`Import metadata: ${JSON.stringify(metadata)}.`);
    return this.getTableForLoading_(destination.table,
        destination.tableSchema).then((storeTable) => {
      return storeTable.load(fileObj, metadata).then(([job]) => {
        const errors = job.status.errors;
        if (errors && errors.length > 0) throw errors;
        const jobId = job.jobReference.jobId;
        console.log(`Job ${jobId} status ${job.status.state}.`);
        return jobId;
      }).catch((error) => {
        console.error(`Job failed for ${this.config.source.name}: `, error);
        throw error;
      });
    });
  }

  /**
   * Returns the Table object in BigQuery. If it doesn't exist, creates the
   * Table and returns it.
   * @param {!BigQueryTableConfig} tableOptions BigQuery Table configuration.
   * @param {!TableSchema} tableSchema Schema to create the table if it doesn't
   *     exist.
   * @return {!Promise<!Table>} The Table.
   * @private
   */
  getOrCreateTable_(tableOptions, tableSchema) {
    const tableIdWithoutPartition = tableOptions.tableId.split('$')[0];
    return super.getBigQuery(tableOptions)
        .dataset(tableOptions.datasetId)
        .get({autoCreate: true})
        .then(([dataset]) => {
          const table = dataset.table(tableIdWithoutPartition);
          return table.exists()
              .then(([tableExists]) => {
                if (tableExists) {
                  console.log(`[${
                      JSON.stringify(tableOptions)}] Exist. Continue to load.`);
                  return table.get();
                } else {
                  console.log(`[${
                      JSON.stringify(
                          tableOptions)}] doesn't exist. CREATE with:`);
                  console.log(JSON.stringify(tableSchema));
                  return table.create(tableSchema);
                }
              })
              .then(([table]) => table);
        });
  }

  /**
   * Gets the Table object in BigQuery for import. If the table doesn't exist,
   * it creates the table based on the given schema then return the Table
   * object. If it is a partition table, it returns the partitioned table
   * object for import.
   * @param {!BigQueryTableConfig} tableOptions The BigQuery table to store
   *     incoming data.
   * @param {!TableSchema} schema Schema to create the table if it doesn't
   *     exist.
   * @return {!Promise<!Table>}
   * @private
   */
  getTableForLoading_(tableOptions, schema) {
    return this.getOrCreateTable_(tableOptions, schema).then((table) => {
      if (!schema.timePartitioning) return table;
      console.log(`Get partition table: [${tableOptions.tableId}]`);
      return super.getBigQuery(tableOptions)
          .dataset(tableOptions.datasetId)
          .table(tableOptions.tableId)
          .get()
          .then(([table]) => table);
    });
  }

  /**
   * Returns whether the file is compressed.
   * @param {string} name File name.
   * @return {boolean}
   * @private
   */
  isCompressed_(name) {
    const lowerCase = name.toLowerCase();
    return (lowerCase.endsWith('.gz') || lowerCase.endsWith('.zip'));
  }
}

exports.LoadTask = LoadTask;