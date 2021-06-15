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
 * @fileoverview Export task class.
 */

'use strict';

const {Table} = require('@google-cloud/bigquery');
const {File} = require('@google-cloud/storage');
const {BigQueryAbstractTask} = require('./bigquery_abstract_task.js');
const {
  TaskType,
  BigQueryTableConfig,
  StorageFileConfig,
} = require('../../task_config/task_config_dao.js');

/**
 * Options for extracts BigQuery Table to Cloud Storage file(s).
 * @typedef {{
 *   destinationFormat:string,
 *   printHeader:boolean,
 * }}
 */
let ExtractOptions;

/**
 * @typedef {{
 *   type:TaskType.EXPORT,
 *   source:!BigQueryTableConfig,
 *   destination:!StorageFileConfig,
 *   options:!ExtractOptions,
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let ExportTaskConfig;

/** BigQuery export table to Cloud Storage task class. */
class ExportTask extends BigQueryAbstractTask {

  /** @override */
  getBigQueryForTask() {
    /** @const {BigQueryTableConfig} */
    const sourceTable = this.config.source;
    return this.getBigQuery(sourceTable);
  }

  /**
   * Exports a BigQuery Table into Cloud Storage file.
   * If it failed due the data is to big for a single file, it will try to put
   * an asterisk in the exported file name and try again.
   * @override
   */
  doTask() {
    /** @const {BigQueryTableConfig} */ const source = this.config.source;
    /** @const {StorageFileConfig} */
    const destination = this.config.destination;
    const sourceTable = this.getBigQueryForTask()
        .dataset(source.datasetId)
        .table(source.tableId);
    /** @const {File} */ const storageFile = this.getStorage(destination)
        .bucket(destination.bucket)
        .file(destination.name);
    return this.exportTable_(sourceTable, storageFile).catch((error) => {
      if (!destination.name.includes('*') && error.message.includes(
          'too large to be exported to a single file')) {
        this.logger.info(
            'Source table is too large to be exported in one file. Trying to '
            + 'add * to the output file and export again now...');
        const multiStorageFile = this.getStorage(destination).bucket(
            destination.bucket).file(destination.name + '*');
        return this.exportTable_(sourceTable, multiStorageFile);
      }
      this.logger.error('Error in extract BQ to GCS:', error);
      throw (error);
    });
  }

  /**
   * Exports BigQuery table to Storage File object.
   * @param {!Table} sourceTable BigQuery table as source.
   * @param {!File} storageFile Export destination Cloud Storage File.
   * @return {!Promise<{jobId:string}>}
   * @private
   */
  async exportTable_(sourceTable, storageFile) {
    const [job] = await sourceTable.extract(storageFile, this.config.options);
    const errors = job.status.errors;
    if (errors && errors.length > 0) throw errors;
    const fileCounts = job.statistics.extract.destinationUriFileCounts[0];
    const status = job.status.state;
    this.jobReference = job.jobReference;
    const {jobId} = job.jobReference;
    this.logger.info(
        `Job[${jobId}] status ${status} with ${fileCounts} files.`);
    return {jobId};
  }
}

module.exports = {
  ExtractOptions,
  ExportTaskConfig,
  ExportTask,
};
