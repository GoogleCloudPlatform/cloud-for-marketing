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

const { TableSchema: BqTableSchema } = require('@google-cloud/bigquery');
const { api: { spreadsheets: { Spreadsheets } } } =
  require('@google-cloud/nodejs-common');
const { TaskType, BigQueryTableConfig, }
  = require('../../task_config/task_config_dao.js');
const { BigQueryAbstractTask } = require('./bigquery_abstract_task.js');
const { LoadTaskDestination } = require('./load_task.js');

/**
 * @typedef {{
 *   type:TaskType.CREATE_EXTERNAL,
 *   source:{
 *     sheet:{
 *       url: string,
 *       sheetName: string,
 *       skipLeadingRows: number|undefined,
 *       safeColumnNames: boolean|undefined,
 *     }
 *   },
 *   destination:!LoadTaskDestination,
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let CreateExternalTableTaskConfig;

/** Task to create a BigQuery external table on a Google Sheet. */
class CreateExternalTableTask extends BigQueryAbstractTask {

  /** @override */
  getBigQueryForTask() {
    /** @const {BigQueryTableConfig} */
    const destinationTable = this.config.destination.table;
    return this.getBigQuery(destinationTable, true);
  }

  /**
   * Create a BigQuery external table based on the given Google Sheet. If the
   * table exists, it will be removed first.
   * @override
   */
  async doTask() {
    const { source, destination } = this.config;
    await this.precheckTargetTable_(destination.table);
    const { sheet } = source;
    const { schema } = destination.tableSchema || {};
    const options = {
      externalDataConfiguration:
        await this.getSheetConfiguration_(sheet, schema),
    };
    const [table] = await this.getBigQueryForTask()
      .dataset(destination.table.datasetId)
      .createTable(destination.table.tableId, options);
    if (!table) {
      throw new Error(`Fail to create ${destination.table.tableId}`);
    }
    this.logger.info(`Created external table ${destination.table.tableId}`);
    return {
      parameters: this.appendParameter({ destinationTable: destination.table }),
    };
  }

  /** @override */
  async isDone() {
    return true;
  }

  /** @override */
  async completeTask() { }

  /**
   * Pre-checks for the target table. If it's an existing external table, will
   * delete it; if the existing table is not an external one, will throw an
   * error.
   * @param {!BigQueryTableConfig} tableOptions BigQuery Table configuration.
   * @private
   */
  async precheckTargetTable_(tableOptions) {
    const { tableId } = tableOptions;
    if (!tableId) {
      throw new Error(`Missing destination external table Id.`);
    }
    if (tableId.indexOf('$') > -1) {
      throw new Error(`External table [${tableId}] can't be a partition one.`);
    }
    const [dataset] = await this.getBigQueryForTask()
      .dataset(tableOptions.datasetId)
      .get({ autoCreate: true });
    const table = dataset.table(tableId);
    const [tableExists] = await table.exists();
    if (!tableExists) {
      this.logger.debug(`Destination table doesn't exist: `, tableOptions);
      return;
    }
    const { type } = (await table.get())[0].metadata;
    if (type !== 'EXTERNAL') {
      throw new Error(`Target table [${tableId}] is not an external table.`);
    }
    this.logger.debug(`Delete the existing table: `, tableOptions);
    let result = await table.delete();
    this.logger.debug(`... result: `, result);
  }

  /**
   * Gets the `externalDataConfiguration` for Google Sheet.
   * @see https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externaldataconfiguration
   * @see https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#GoogleSheetsOptions
   * @param {!Object} sheet
   * @param {!BqTableSchema|undefined} schema
   * @return {Object}
   */
  async getSheetConfiguration_(sheet, schema) {
    const skipLeadingRows = sheet.skipLeadingRows || 1;
    const config = {
      sourceFormat: 'GOOGLE_SHEETS',
      sourceUris: [sheet.url],
      googleSheetsOptions: {
        skipLeadingRows,
        range: sheet.sheetName,
      },
    };
    if (schema) {
      config.schema = schema;
    } else if (sheet.safeColumnNames) {
      const spreadsheet = new Spreadsheets(sheet.url);
      const headers =
        await spreadsheet.getHeadline(sheet.sheetName, skipLeadingRows);
      config.schema = this.getBigQuerySafeSchema(headers);
    } else {
      config.autodetect = true;
    }
    this.logger.debug(`Get sheet options: `, config);
    return config;
  }
}

module.exports = {
  CreateExternalTableTaskConfig,
  CreateExternalTableTask,
};
