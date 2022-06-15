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
 * @fileoverview Load task class.
 */

'use strict';

const {Table, TableSchema: BqTableSchema} = require('@google-cloud/bigquery');
const {
  storage: { StorageFile },
  utils: { wait },
} = require('@google-cloud/nodejs-common');
const {BigQueryAbstractTask} = require('./bigquery_abstract_task.js');
const {ReportTask} = require('../report_task.js');
const {
  TaskType,
  StorageFileConfig,
  BigQueryTableConfig,
  WriteDisposition,
} = require('../../task_config/task_config_dao.js');

/**
 * @typedef {{
 *     schema:BqTableSchema,
 *     timePartitioning:({
 *       type:string,
 *       requirePartitionFilter:boolean,
 *     }|undefined),
 * }}
 */
let TableSchema;

/**
 * Options for loading Cloud Storage file into BigQuery.
 * See: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload
 * @typedef {{
 *     sourceFormat:string,
 *     writeDisposition:!WriteDisposition,
 *     skipLeadingRows:number,
 *     autodetect:boolean,
 *     compression:(boolean|undefined),
 * }}
 */
let LoadOptions;

/**
 * `schemaSource` is a report task Id which generates report files and trigger
 * this load task to load data to BigQuery.
 * Sometimes BigQuery's autodetect can't figure out the schema correctly due to
 * the complexity of report. In that case, an explict schema is required to
 * complete the load job.
 * Some report systems, e.g. Google Ads report, through API offers a service to
 * indicate the data type and structure of reports. This makes it possible to
 * generate schema based on the report settings. So a load job will use `Report`
 * object to generate the schema through this `schemaSource` setting.
 * Another case is moving tables between locations through Cloud Storage, the
 * schema can be exported from the source table to a GCS file and be reused here
 * as `schemaFile`.
 *
 * @typedef {{
 *    table:!BigQueryTableConfig,
 *    tableSchema:!TableSchema,
 *    schemaSource:string|undefined,
 *    schemaFile:!StorageFileConfig|undefined,
 *  }}
 */
let LoadTaskDestination;

/**
 * @typedef {{
 *   type:TaskType.LOAD,
 *   source:{
 *     file:!StorageFileConfig,
 *     fileNamePattern:string,
 *   },
 *   destination:!LoadTaskDestination,
 *   options:!LoadOptions,
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let LoadTaskConfig;

/**
 * @const{number}
 * Rerty times if failed to start task.
 * When the schema is from external source, e.g. a Google Ads report, there is
 * a chance that it failed to get the schema. A retry should solve this problem.
 */
const DEFAULT_RETRY_TIMES = 3;

/** BigQuery load Cloud Storage file to table task class. */
class LoadTask extends BigQueryAbstractTask {

  /** @constructor */
  constructor(config, parameters, defaultProject = undefined) {
    super(config, parameters, defaultProject);
    /** @type {boolean} If the target file is empty, skip loading. */
    this.isEmptyFile = false;
  }

  /** @override */
  getBigQueryForTask() {
    /** @const {BigQueryTableConfig} */
    const destinationTable = this.config.destination.table;
    return this.getBigQuery(destinationTable);
  }

  /** @override */
  async isDone() {
    if (this.isEmptyFile) return true;
    return super.isDone();
  }

  /**
   * Loads a Cloud Storage file into BigQuery Table. If the table doesn't exist,
   * it will use the given table schema to create the table first.
   * @override
   */
  async doTask() {
    /** @const {StorageFileConfig} */
    const sourceFile = this.config.source.file;
    /** @const {LoadOptions} */
    const metadata = Object.assign({}, this.config.options);
    /** @const {LoadTaskDestination} */
    const destination = this.config.destination;
    destination.tableSchema = destination.tableSchema || {};
    // Needs to check the file is not empty for a single file and autodetect
    // schema mode. Note: even with wildcard, there is still a chance that no
    // files match and BigQuery fails to autodetect schema.
    if (sourceFile.name.indexOf('*') === -1 && metadata.autodetect) {
      /** @const {StorageFile} */
      const storageFile = StorageFile.getInstance(
          sourceFile.bucket, sourceFile.name,
          {projectId: this.getCloudProject(sourceFile)});
      const size = await storageFile.getFileSize();
      if (size === 0) {
        this.isEmptyFile = true;
        this.logger.info('Skip for empty file:', sourceFile);
        return {
          warning: `Skip for empty file: ${sourceFile.name}`,
          parameters: this.appendParameter(
              {destinationTable: destination.table}),
        };
      }
    }

    metadata.sourceUris = [`gs://${sourceFile.bucket}/${sourceFile.name}`];
    metadata.compression = this.isCompressed_(sourceFile.name);
    this.logger.debug('Load task metadata: ', metadata);
    // @see the typedef of LoadTaskDestination
    if (!metadata.autodetect && !destination.tableSchema.schema) {
      destination.tableSchema.schema = await this.getSchemaForExternal_(
          destination);
      this.logger.debug('External schema: ', destination.tableSchema.schema);
    }
    const storeTable = await this.getTableForLoading_(destination.table,
        destination.tableSchema);
    const [job] = await storeTable.load(undefined, metadata);
    const errors = job.status.errors;
    if (errors && errors.length > 0) throw errors;
    this.jobReference = job.jobReference;
    const {jobId} = job.jobReference;
    this.logger.debug(`Job ${jobId} status ${job.status.state}.`);
    return {
      jobId,
      parameters: this.appendParameter(
          {destinationTable: destination.table}),
    };
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
  async getOrCreateTable_(tableOptions, tableSchema) {
    const tableIdWithoutPartition = tableOptions.tableId.split('$')[0];
    const [dataset] = await this.getBigQueryForTask()
        .dataset(tableOptions.datasetId)
        .get({autoCreate: true});
    const table = dataset.table(tableIdWithoutPartition);
    const [tableExists] = await table.exists();
    let result;
    if (tableExists) {
      this.logger.debug(`Load to existing table: `, tableOptions);
      [result] = await table.get();
    } else {
      this.logger.info('Create table: ', tableOptions);
      this.logger.debug(JSON.stringify(tableSchema));
      [result] = await table.create(tableSchema);
    }
    return result;
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
  async getTableForLoading_(tableOptions, schema) {
    const table = await this.getOrCreateTable_(tableOptions, schema);
    if (!schema.timePartitioning) return table;
    this.logger.debug('Get partition table: ', tableOptions.tableId);
    const [result] = await this.getBigQueryForTask()
        .dataset(tableOptions.datasetId)
        .table(tableOptions.tableId)
        .get();
    return result;
  }

  /**
   * Get the schema definition from external tasks or resources.
   * @param destination
   * @return {!Promise<!BqTableSchema>}
   * @private
   */
  async getSchemaForExternal_(destination) {
    if (destination.schemaSource) {
      this.logger.debug('External schema from report: ',
          destination.schemaSource);
      const taskConfig = await this.options.taskConfigDao.load(
          destination.schemaSource);
      const task = new ReportTask(taskConfig, this.parameters);
      task.injectContext(this.options);
      return this.getSchemaFromReportTask_(task);
    }
    if (destination.schemaFile) {
      this.logger.debug('External schema from file: ', destination.schemaFile);
      const schemaFile = StorageFile.getInstance(
          destination.schemaFile.bucket, destination.schemaFile.name,
          {projectId: this.getCloudProject(destination.schemaFile)});
      return JSON.parse(await schemaFile.loadContent());
    }
    throw new Error('Not schema defined in config or externally');
  }

  /**
   * Gets the schema from a ReportTask.
   * It is only used for Google Ads Report currently. Google Ads API offers a
   * service to return the report type definition can be used to generate
   * BigQuery schema.
   *
   * @param {!ReportTask} task
   * @param {number=} retriedTimes
   */
  async getSchemaFromReportTask_(task, retriedTimes = 0) {
    try {
      const schema = await task.getReport().generateSchema();
      return schema;
    } catch (error) {
      if (task.getReport().isFatalError(error.toString())) {
        this.logger.error(
          'Fail immediately without retry for generateSchema error: ',
          error.toString());
        throw error;
      } else if (retriedTimes > DEFAULT_RETRY_TIMES) {
        this.logger.error(`Retried over ${DEFAULT_RETRY_TIMES} times.`,
          error.toString());
        throw error;
      } else {
        retriedTimes++;
        await wait(retriedTimes * 1000);
        this.logger.info('Retry for generateSchema error: ', error.toString());
        return this.getSchemaFromReportTask_(task, retriedTimes);
      }
    }
  }

  /**
   * Returns whether the file is compressed.
   * @param {string} name File name.
   * @return {boolean}
   * @private
   */
  isCompressed_(name) {
    const lowerCase = name.toLowerCase();
    return lowerCase.endsWith('.gz') || lowerCase.endsWith('.zip');
  }
}

module.exports = {
  TableSchema,
  LoadOptions,
  LoadTaskConfig,
  LoadTask,
};
