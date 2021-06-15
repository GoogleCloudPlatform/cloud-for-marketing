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
 * @typedef {{
 *   type:TaskType.EXPORT_SCHEMA,
 *   source:!BigQueryTableConfig,
 *   destination:!StorageFileConfig,
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let ExportSchemaTaskConfig;

/** BigQuery get table schema to Cloud Storage task class. */
class ExportSchemaTask extends BigQueryAbstractTask {

  /** @override */
  getBigQueryForTask() {
    /** @const {BigQueryTableConfig} */
    const sourceTable = this.config.source;
    return this.getBigQuery(sourceTable);
  }

  /**
   * Exports a BigQuery Table schema into Cloud Storage file.
   * @override
   */
  async doTask() {
    /** @const {BigQueryTableConfig} */ const source = this.config.source;
    /** @const {StorageFileConfig} */
    const destination = this.config.destination;
    /** @const {Table} */
    const sourceTable = this.getBigQueryForTask()
        .dataset(source.datasetId)
        .table(source.tableId);
    /** @const {File} */
    const file = this.getStorage(destination)
        .bucket(destination.bucket)
        .file(destination.name);
    const [{metadata: {schema}}] = await sourceTable.get();
    this.logger.debug('Get schema of ', source);
    this.logger.debug(schema);
    await file.save(JSON.stringify(schema));
    return {
      parameters: this.appendParameter({schemaFile: destination}),
    };
  }

  /** @override */
  async isDone() {
    return true;
  }
}

module.exports = {
  ExportSchemaTaskConfig,
  ExportSchemaTask,
};
