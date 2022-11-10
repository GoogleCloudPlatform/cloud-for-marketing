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
 * @fileoverview Google Spreadsheets API Client Library.
 */

'use strict';

const {google} = require('googleapis');
const {Params$Resource$Spreadsheets$Get} = google.sheets;
const AuthClient = require('./auth_client.js');
const {getLogger, BatchResult} = require('../components/utils.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/spreadsheets',
]);
const API_VERSION = 'v4';

/**
 * Definition of PasteDataRequest for Sheets API batchUpdate operation.
 * see:
 * https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request#pastedatarequest
 * @typedef {{
 *   coordinate:{
 *     sheetId:(number|undefined),
 *     rowIndex:(number|undefined),
 *     columnIndex:(number|undefined),
 *   },
 *   data:(string|undefined),
 *   type:(string|undefined),
 *   delimiter:(string|undefined),
 * }}
 */
let ParseDataRequest;

/**
 *
 * Definition of the object 'range' that is used to append or delete dimensions
 * requests. see:
 * https://developers.google.com/sheets/api/samples/rowcolumn#delete_rows_or_columns
 * https://developers.google.com/sheets/api/samples/rowcolumn#insert_an_empty_row_or_column
 * @typedef {{
 *     sheetId:number,
 *     dimension:string,
 *     startIndex:number,
 *     endIndex:(number|undefined),
 * }}
 */
let DimensionRange;

/**
 * Google Spreadsheets API v4 stub.
 */
class Spreadsheets {
  /**
   * Init Spreadsheets API client.
   * @param {string} spreadsheetId
   * @param {!Object<string,string>=} env The environment object to hold env
   *     variables.
   */
  constructor(spreadsheetId, env = process.env) {
    /** @const {string} */
    this.spreadsheetId = spreadsheetId;
    this.authClient = new AuthClient(API_SCOPES, env);
    /**
     * Logger object from 'log4js' package where this type is not exported.
     */
    this.logger = getLogger('API.GS');
  }

  /**
   * Prepares the Google Sheets instance.
   * @return {!google.sheets}
   * @private
   */
  async getApiClient_() {
    if (this.sheets) return this.sheets;
    await this.authClient.prepareCredentials();
    this.logger.debug(`Initialized ${this.constructor.name} instance.`);
    this.sheets = google.sheets({
      version: API_VERSION,
      auth: this.authClient.getDefaultAuth(),
    });
    return this.sheets;
  }

  /**
   * Gets the Sheet Id of the given Spreadsheet and possible Sheet name. If the
   * Sheet name is missing, it will return the first Sheet's Id.
   * @param {string} sheetName Name of the Sheet.
   * @return {!Promise<number>} The ID for the sheet that is unique to the
   *   spreadsheet.
   */
  async getSheetId(sheetName) {
    const request = /** @type{Params$Resource$Spreadsheets$Get} */ {
      spreadsheetId: this.spreadsheetId,
      ranges: sheetName,
    };
    const sheets = await this.getApiClient_();
    const response = await sheets.spreadsheets.get(request);
    const sheet = response.data.sheets[0];
    this.logger.debug(`Get sheet[${sheetName}]: `, sheet);
    return sheet.properties.sheetId;
  }

  /**
   * Clears the content of the Sheet.
   * see:
   * https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/clear
   * @param {string} sheetName Name of the Sheet.
   */
  async clearSheet(sheetName) {
    const request = {
      spreadsheetId: this.spreadsheetId,
      range: sheetName,
    };
    try {
      const sheets = await this.getApiClient_();
      const response = await sheets.spreadsheets.values.clear(request);
      const data = response.data;
      this.logger.debug(`Clear sheet[${sheetName}}]: `, data);
    } catch (error) {
      this.logger.error(error.toString());
      throw error;
    }
  }

  /**
   * Gets the request to change the dimensions through Sheets batchUpdate.
   * See:
   * https://developers.google.com/sheets/api/samples/rowcolumn#delete_rows_or_columns
   * https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request#AppendDimensionRequest
   * @param {number} sheetId Sheet Id.
   * @param {string} dimension 'ROWS' or 'COLUMNS'.
   * @param {number} current Current number of the dimension.
   * @param {number} target The target number of the dimension.
   * @return {{
        appendDimension: {
          sheetId:number,
          dimension:string,
          length:number,
        }
      }|{deleteDimension:{range:!DimensionRange}}|undefined}
   * @private
   */
  getChangeDimensionRequest_(sheetId, dimension, current, target) {
    if (current === target) return;
    if (current < target) {  // Appends dimension.
      return {appendDimension: {sheetId, dimension, length: target - current}};
    } else {  // Deletes dimension.
      return {
        deleteDimension: {
          range: {sheetId, dimension, startIndex: target, endIndex: current},
        }
      };
    }
  };

  /**
   * Reshapes the grids before loading data.
   * @param {string} sheetName Name of the Sheet.
   * @param {number} targetRows Loaded data rows number.
   * @param {number} targetColumns Loaded data columns number.
   */
  async reshape(sheetName, targetRows, targetColumns) {
    const request =  /** @type{Params$Resource$Spreadsheets$Get} */ {
      spreadsheetId: this.spreadsheetId,
      ranges: sheetName,
    };
    try {
      const sheets = await this.getApiClient_();
      const response = await sheets.spreadsheets.get(request);
      const sheet = response.data.sheets[0];
      const sheetId = sheet.properties.sheetId;
      const rowCount = sheet.properties.gridProperties.rowCount;
      const columnCount = sheet.properties.gridProperties.columnCount;
      this.logger.debug(`Get sheet[${sheetName}]: `, sheet);
      const requests = {
        spreadsheetId: this.spreadsheetId,
        resource: {requests: []},
      };
      if (rowCount !== targetRows) {
        requests.resource.requests.push(this.getChangeDimensionRequest_(
            sheetId, 'ROWS', rowCount, targetRows));
      }
      if (columnCount !== targetColumns) {
        requests.resource.requests.push(this.getChangeDimensionRequest_(
            sheetId, 'COLUMNS', columnCount, targetColumns));
      }
      this.logger.debug(`Reshape Sheet from [${rowCount}, ${
              columnCount}] to [${targetRows}, ${targetColumns}]`,
          JSON.stringify(requests.resource.requests));
      if (requests.resource.requests.length > 0) {
        const { data } = await sheets.spreadsheets.batchUpdate(requests);
        this.logger.debug(`Reshape Sheet [${sheetName}]: `, data);
      } else {
        this.logger.debug('No need to reshape.');
      }
    } catch (error) {
      this.logger.error(error.toString());
      throw error;
    }
  }

  /**
   * Loads the data into the Sheet through 'pasteData' request.
   * @param {string} data Data to be loaded to Sheet.
   * @param {!ParseDataRequest} config A ParseDataRequest object template. The
   * data will be put in before it is send out through Sheets batchUpdate.
   * @param {string=} batchId The tag for log.
   * @return {!BatchResult}
   */
  async loadData(data, config, batchId = 'unnamed') {
    const pasteData = Object.assign({}, config, {data});
    const request = {
      spreadsheetId: this.spreadsheetId,
      resource: {requests: [{pasteData}]},
    };
    /** @type {BatchResult} */ const batchResult = {
      numberOfLines: data.trim().split('\n').length,
    };
    try {
      const sheets = await this.getApiClient_();
      const response = await sheets.spreadsheets.batchUpdate(request);
      const data = response.data;
      this.logger.debug(`Batch[${batchId}] uploaded: `, data);
      batchResult.result = true;
    } catch (error) {
      this.logger.error(error);
      batchResult.result = false;
      batchResult.errors = [error.toString()];
    }
    return batchResult;
  }
}

module.exports = {
  Spreadsheets,
  ParseDataRequest,
  API_VERSION,
  API_SCOPES,
};
