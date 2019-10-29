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

const {File} = require('@google-cloud/storage');
const {BaseTask} = require('./base_task.js');
const {TaskType, BigQueryTableConfig, StorageFileConfig} = require(
    '../dao/task_config_dao.js');

/**
 * Options for extracts BigQuery Table to Cloud Storage file(s).
 * @typedef {{
 *   destinationFormat:string,
 *   printHeader:boolean,
 * }}
 */
let ExtractOptions;

exports.ExtractOptions = ExtractOptions;

/**
 * @typedef {{
 *   type:TaskType.EXPORT,
 *   source:!BigQueryTableConfig,
 *   destination:!StorageFileConfig,
 *   options:!ExtractOptions,
 * }}
 */
let ExportTaskConfig;

exports.ExportTaskConfig = ExportTaskConfig;

class ExportTask extends BaseTask {

  /** @override */
  doTask() {
    /** @type {StorageFileConfig} */
    const destination = this.config.destination;
    this.bigqueryTable = super.getBigQuery(this.config.source).dataset(
        this.config.source.datasetId).table(this.config.source.tableId);
    const storageFile = super.getStorage(destination).bucket(
        destination.bucket).file(destination.name);
    return this.exportTable_(storageFile).catch((error) => {
      if (destination.name.indexOf('*') < 0 && error.message.indexOf(
          'too large to be exported to a single file') > 0) {
        console.log(
            'Source table is too large to be exported in one file. Put a * in the output file and try again now.');
        const multiStorageFile = super.getStorage(destination).bucket(
            destination.bucket).file(destination.name + '*');
        return this.exportTable_(multiStorageFile);
      }
      console.error('Error in extract BQ to GCS:', error);
      throw (error);
    });
  }

  /**
   * Exports BiqQuery table to Storage File object.
   * @param {!File} storageFile Export destination Cloud Storage File.
   * @return {!Promise<string>}
   * @private
   */
  exportTable_(storageFile) {
    return this.bigqueryTable.extract(storageFile, this.config.options).then(
        ([job]) => {
          const errors = job.status.errors;
          if (errors && errors.length > 0) throw errors;
          // console.log(job);
          const fileCounts = job.statistics.extract.destinationUriFileCounts[0];
          const jobId = job.jobReference.jobId;
          console.log(`Job ${jobId} status ${job.status.state} with ${
              fileCounts} files.`);
          return jobId;
        });
  }
}

exports.ExportTask = ExportTask;