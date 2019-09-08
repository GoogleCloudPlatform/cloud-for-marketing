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
 * @fileoverview Utility class for data connector between Cloud Storage and
 * BigQuery.
 */

'use strict';

const {BigQuery, Table, Job} = require('@google-cloud/bigquery');
const {Storage} = require('@google-cloud/storage');
const {wait} = require('./utils.js');

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
 * @typedef {{
 *   projectId:(string|undefined),
 *   datasetId:(string|undefined),
 *   tableId:(string|undefined),
 *   keyFilename:(string|undefined),
 * }}
 */
let BigQueryTableConfig;

exports.BigQueryTableConfig = BigQueryTableConfig;

/**
 * @typedef {{
 *   projectId:(string|undefined),
 *   bucket:(string|undefined),
 *   name:(string|undefined),
 *   keyFilename:(string|undefined),
 * }}
 */
let StorageFileConfig;

exports.StorageFileConfig = StorageFileConfig;

/**
 * Options for loading Cloud Storage file into BigQuery.
 * @typedef{{
 *     sourceFormat:string,
 *     writeDisposition:string,
 *     skipLeadingRows:number,
 *     autodetect:boolean,
 *     compression:(boolean|undefined),
 * }}
 */
let LoadOptions;

exports.LoadOptions = LoadOptions;

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
 * Behaviors for how to export the data when the target (output) table exists
 * in BigQuery.
 * See
 * https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.job.WriteDisposition.html
 * @enum {string}
 */
const WriteDisposition = {
  WRITE_APPEND: 'WRITE_APPEND',      // Append to existent.
  WRITE_EMPTY: 'WRITE_EMPTY',        // Only exports data when there is no data.
  WRITE_TRUNCATE: 'WRITE_TRUNCATE',  // Overwrite existent.
};

exports.WriteDisposition = WriteDisposition;

/**
 * BigQuery(BQ) and Google Cloud Storage(GCS) connector class.
 * It offers functions to load data from GCS to BQ and export query results
 * from BQ to GCS as well. Main functions are:
 * 1. Gets BigQuery table. If the table doesn't exist, create it first.
 * 2. Load a GCS file to a given BQ Table.
 * 3. Executes query and export data to a BQ Table. For results in a huge
 * amount, exporting to BQ Table is the only way. (Can't go to GCS directly.)
 * 4. Extracts a BQ Table to GCS files.
 *
 * A complete data pipeline in this connector is:
 * GCS(source) --> BQ(store) --[query]--> BQ(output) --> GCS(output)
 */
class BigQueryStorageConnector {
  /**
   * Initialize the instance.
   * For output BigQuery, if datasetId is omitted, it will use store BiqQuery's
   * datasetId. Same as the bucket for output Storage.
   * 'projectId' is necessary for operations of BigQuery.s
   * @param {!StorageFileConfig=} sourceStorageOptions
   * @param {!BigQueryTableConfig=} storeBigqueryOptions
   * @param {!BigQueryTableConfig=} outputBigqueryOptions
   * @param {!StorageFileConfig=} outputStorageOptions
   * @param {{
   *   projectId:(string|undefined),
   *   datasetId:(string|undefined),
   *   bucket:(string|undefined),
   * }=} defaultOptions The default options for BigQuery and Storage.
   */
  constructor(
      sourceStorageOptions = {}, storeBigqueryOptions = {},
      outputBigqueryOptions = {}, outputStorageOptions = {},
      defaultOptions = {}) {
    /** @const {!StorageFileConfig} */
    this.sourceStorageOptions = Object.assign(
        {
          projectId: defaultOptions.projectId,
          bucket: defaultOptions.bucket,
        },
        sourceStorageOptions);
    /** @const {!BigQueryTableConfig} */
    this.storeBigqueryOptions = Object.assign(
        {
          projectId: defaultOptions.projectId,
          datasetId: defaultOptions.datasetId,
        },
        storeBigqueryOptions);
    /** @const {!BigQueryTableConfig} */
    this.outputBigqueryOptions = Object.assign(
        {
          projectId: defaultOptions.projectId,
          datasetId: defaultOptions.datasetId,
        },
        {
          datasetId: this.storeBigqueryOptions.datasetId,
        },
        outputBigqueryOptions);
    /** @const {!StorageFileConfig} */
    this.outputStorageOptions = Object.assign(
        {
          projectId: defaultOptions.projectId,
          bucket: defaultOptions.bucket,
        },
        {
          bucket: this.sourceStorageOptions.bucket,
        },
        outputStorageOptions);
  };

  /**
   * Gets the instance of Cloud Storage.
   * @param {!StorageFileConfig} options StorageFileConfig instance.
   * @return {!Storage} Cloud Storage instance.
   * @private
   */
  getStorage_(options) {
    return new Storage(
        {projectId: options.projectId, keyFilename: options.keyFilename});
  }

  /**
   * Gets the instance of BigQuery.
   * @param {!BigQueryTableConfig} options BigQueryTableConfig instance.
   * @return {!BigQuery} BigQuery instance.
   * @private
   */
  getBigQuery_(options) {
    return new BigQuery(
        {projectId: options.projectId, keyFilename: options.keyFilename});
  }

  /**
   * Returns the BigQueryTableConfig based on the default configuration and the
   * given Table information.
   * @param {string|!BigQueryTableConfig} tableOptions The table ID or
   *     BigQueryTableConfig of the target table.
   * @param {!BigQueryTableConfig} defaultOptions Default BigQuery
   *     configuration
   *     options.
   * @return {!BigQueryTableConfig} The complete BigQueryTableConfig.
   * @private
   */
  getBigQueryTableConfig_(tableOptions, defaultOptions) {
    return Object.assign(
        {}, defaultOptions,
        (typeof tableOptions === 'string') ? {tableId: tableOptions} :
            tableOptions);
  }

  /**
   * Returns the StorageConfig based on the default configuration and the give
   * file information.
   * @param {string|!StorageFileConfig} fileOptions
   * @param {!StorageFileConfig} defaultOptions Default Storage configuration
   *     options.
   * @return {!StorageFileConfig} The complete StorageConfig.
   * @private
   */
  getStorageFileConfig_(fileOptions, defaultOptions) {
    return Object.assign(
        {}, defaultOptions,
        (typeof fileOptions === 'string') ? {name: fileOptions} : fileOptions);
  }

  /**
   * Gets the Table object in BigQuery for import. If the table doesn't exist,
   * it creates the table based on the given schema then return the Table
   * object. If it is a partition table, it returns the partitioned table
   * object for import.
   * @param {string|!BigQueryTableConfig=} storeTableOptions The Table ID or
   *     BigQueryTableConfig of the BQ table to store incoming data.
   * @param {!TableSchema} schema Schema to create the table if it doesn't
   *     exist.
   * @param {string|undefined=} partitionDay If the target table is
   *     partitioned, this is partition day of the target table in 'YYYYMMDD'
   *     format.
   * @return {!Promise<!Table>}
   */
  getTableForLoading(storeTableOptions = {}, schema, partitionDay = undefined) {
    const tableOptions = this.getBigQueryTableConfig_(
        storeTableOptions, this.storeBigqueryOptions);
    return this.getOrCreateTable(tableOptions, schema).then((table) => {
      if (!schema.timePartitioning || !partitionDay) {
        console.log(`It's ${
            schema.timePartitioning ?
                '' :
                'not '} partitioned with given partition day: ${
            partitionDay}.`);
        return table;
      }
      const partitionedTable = `${tableOptions.tableId}\$${partitionDay}`;
      console.log(`Get partition table: [${partitionedTable}]`);
      return this.getBigQuery_(tableOptions)
          .dataset(tableOptions.datasetId)
          .table(partitionedTable)
          .get()
          .then(([table]) => table);
    });
  }

  /**
   * Returns the Table object in BigQuery. If it doesn't exist, creates the
   * Table and returns it.
   * @param {!BigQueryTableConfig} tableOptions BigQuery Table configuration.
   * @param {!TableSchema} tableSchema Schema to create the table if it doesn't
   *     exist.
   * @return {!Promise<!Table>} The Table.
   */
  getOrCreateTable(tableOptions, tableSchema) {
    return this.getBigQuery_(tableOptions)
        .dataset(tableOptions.datasetId)
        .get({autoCreate: true})
        .then(([dataset]) => {
          const table = dataset.table(tableOptions.tableId);
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
   * Loads Google Cloud Storage file into BigQuery.
   * @param {!StorageFileConfig|string} sourceFileOptions
   * @param {!Table} storeTable A promised BigQuery Table.
   * @param {!LoadOptions} metadata Settings let BigQuery understand the loaded
   *     files.
   * @return {!Promise<void>}
   */
  loadStorageToBigQuery(sourceFileOptions, storeTable, metadata) {
    console.log(`Start to load GCS '${sourceFileOptions.name}' from [${
        sourceFileOptions.bucket}]`);
    const fileOptions = this.getStorageFileConfig_(
        sourceFileOptions, this.sourceStorageOptions);
    const fileObj = this.getStorage_(fileOptions)
        .bucket(fileOptions.bucket)
        .file(fileOptions.name);
    metadata.compression =
        fileOptions.name.endsWith('.gz') || fileOptions.name.endsWith('.zip');
    console.log(`Import metadata: ${JSON.stringify(metadata)}.`);
    return storeTable.load(fileObj, metadata).catch((error) => {
      console.error(`Job failed for ${fileOptions.name}: `, error);
      throw error;
    });
  }

  /**
   * Runs a query on BigQuery and exports the result to the given table.
   * @param {string} query The query SQL.
   * @param {!Object<string,*>=} params Named parameter objects of the query.
   * @param {string|!BigQueryTableConfig=} outputTableOptions The Table ID or
   *     BigQueryTableConfig of the BQ table to store result of the query.
   *     Partition tableId is supported.
   * @param {!WriteDisposition=} writeDisposition Specifies the action that
   *     occurs if destination table already exists. Default value is
   *     'WRITE_TRUNCATE'.
   * @return {!Promise<!Job>} BigQuery Job.
   */
  queryToBigQuery(
      query, params = {}, outputTableOptions = {},
      writeDisposition = WriteDisposition.WRITE_TRUNCATE) {
    const tableOptions = this.getBigQueryTableConfig_(
        outputTableOptions, this.outputBigqueryOptions);
    const options = {
      query: query,
      params: params,
      destinationTable: tableOptions,
      writeDisposition: writeDisposition,
    };
    if (tableOptions.tableId.indexOf('$') > -1) {
      options.timePartitioning = {type: 'DAY', requirePartitionFilter: false};
    }
    console.log(
        `Export to: ${JSON.stringify(tableOptions)} with query: `, query);
    return this.getBigQuery_(tableOptions)
        .createQueryJob(options)
        .then(([job]) => {
          console.log(`Create job[${job.id}] on query [${
              options.query.split('\n')[0]}]`);
          return this.waitForFinished_(job);
        })
        .catch((error) => {
          console.error(`Error in query to BigQuery:`, error);
          throw (error);
        });
  }

  /**
   * Extracts a BigQuery Table to GCS file(s).
   * https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.extract
   *
   * @param {!ExtractOptions} extractOptions Options for extraction.
   * @param {string|!StorageFileConfig=} outputFileOptions File name or
   *     configuration for the output Storage file. The file name should be the
   *     full path file name under GCS bucket. In order to support massive
   *     data, an astral is expected in the file name.
   * @param {string|!BigQueryTableConfig=} outputTableOptions The Table ID or
   *     BigQueryTableConfig of the BQ table to be exported. Partition tableId
   *     is supported in '$YYYYMMDD' suffix format.
   * @return {!Promise<number>} How many files exported to GCS.
   */
  extractBigQueryToStorage(
      extractOptions, outputFileOptions = {}, outputTableOptions = {}) {
    const tableOptions = this.getBigQueryTableConfig_(
        outputTableOptions, this.outputBigqueryOptions);
    const fileOptions = this.getStorageFileConfig_(
        outputFileOptions, this.outputStorageOptions);
    const fileObject = this.getStorage_(fileOptions)
        .bucket(fileOptions.bucket)
        .file(fileOptions.name);
    return this.getBigQuery_(tableOptions)
        .dataset(tableOptions.datasetId)
        .table(tableOptions.tableId)
        .extract(fileObject, extractOptions)
        .then(([job]) => {
          const errors = job.status.errors;
          if (errors && errors.length > 0) {
            throw errors;
          }
          const fileCounts = job.statistics.extract.destinationUriFileCounts[0];
          console.log(`Job ${job.id} status ${job.status.state} with ${
              fileCounts} files.`);
          return parseInt(fileCounts);
        })
        .catch((error) => {
          console.error('Error in extract BQ to GCS:', error);
          throw (error);
        });
  }

  /**
   * Waits for the query job finished and returns it.
   * @param {!Job} job BigQuery Job object.
   * @param {number=} count Counter of Job check times. It waits for 5 seconds
   *     between checks.
   * @return {!Promise<!Job>}
   * @private
   */
  waitForFinished_(job, count = 0) {
    return job.get().then(([job, response]) => {
      if (response.status.state !== 'DONE') {
        console.log(`Job[${job.id}] is not finished [${count}]. Wait 5 sec.`);
        return wait(5000, count++).then((count) => {
          return this.waitForFinished_(job, count);
        });
      } else {
        console.log(`Job[${job.id}] done [${count}].`);
        return job;
      }
    });
  }

  /**
   * Executes query, exports the result into a BigQuery table and extracts it
   * to Storage file(s). For huge amount query results, BQ doesn't support to
   * export to GCS directly.
   * @param {string} query The query SQL.
   * @param {!Object<string,*>} params Named parameter objects of the query.
   * @param {string|!BigQueryTableConfig} outputTableOptions The Table ID or
   *     BigQueryTableConfig of the BQ table to store result of the query.
   *     Partition tableId is supported.
   * @param {string|!StorageFileConfig} outputFileOptions File name or
   *     configuration for the output Storage file. The file name should be the
   *     full path file name under GCS bucket. In order to support massive
   *     data, an astral is expected in the file name.
   * @param {!ExtractOptions} extractOptions Options for extraction.
   * @return {!Promise<number>} How many files exported to GCS.
   */
  queryToStorageThroughBigQuery(
      query, params, outputTableOptions, outputFileOptions, extractOptions) {
    return this.queryToBigQuery(query, outputTableOptions, params).then(() => {
      return this.extractBigQueryToStorage(
          outputTableOptions, outputFileOptions, extractOptions);
    });
  }
}

exports.BigQueryStorageConnector = BigQueryStorageConnector;
