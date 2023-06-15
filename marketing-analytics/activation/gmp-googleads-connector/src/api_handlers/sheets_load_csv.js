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
 * @fileoverview Tentacles API handler to load CSV to Google Sheet through
 *     Sheets API.
 */

'use strict';

const {
  api: {spreadsheets: {Spreadsheets, ParseDataRequest}},
  utils: {
    getProperValue,
    splitArray,
    BatchResult,
    mergeBatchResults,
  },
  storage: {StorageFile},
} = require('@google-cloud/nodejs-common');
const { ApiHandler } = require('./api_handler.js');

const MAXIMUM_REQUESTS_LENGTH = 10000000;  // Maximum requests size of this API.
const NUMBER_OF_THREADS = 9;  // Maximum number of concurrent requests.
const DEFAULT_DELIMITER = ',';
/**
 * Default kind of data that will be pasted.
 * see:
 * https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request#PasteType
 */
const DEFAULT_PASTE_TYPE = 'PASTE_NORMAL';

/**
 * 'sheetHeader' is the fixed head row(s) in the Sheet.
 * 'requestLength' is the byte size of each request.
 * 'pasteData' is for Sheets API batchUpdate. see
 *     https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request#pastedatarequest
 *
 * @typedef {{
*   spreadsheetId:string,
*   sheetName:string,
*   sheetHeader:(string|undefined),
*   requestLength:(number|undefined),
*   pasteData:!ParseDataRequest,
*   numberOfThreads:(number|undefined),
*   secretName:(string|undefined),
* }}
*/
let SheetsLoadConfig;

/**
 * Load a CSV to Google Sheet.
 */
class GoogleSheetLoadCsv extends ApiHandler {

  /**
   * Sends the data from a CSV file or a Pub/sub message to Google Sheet.
   * @param {string} message Message data from Pubsub. It could be the
   *     information of the file to be sent out, or a piece of data that need to
   *     be send out (used for test).
   * @param {string} messageId Pub/sub message ID for log.
   * @param {!SheetsLoadConfig} config SheetsLoadConfig Json object.
   * @return {!BatchResult}
   * @override
   */
  sendData(message, messageId, config) {
    const spreadsheets = new Spreadsheets(
      config.spreadsheetId, this.getOption(config));
    return this.sendDataInternal(spreadsheets, message, messageId, config);
  }

  /**
   * Internal sendData function for test.
   * @param {!Spreadsheets} spreadsheets Injected Spreadsheets instance.
   * @param {string} message Message data from Pubsub. It could be the
   *     information of the file to be sent out, or a piece of data that need to
   *     be send out (used for test).
   * @param {string} messageId Pub/sub message ID for log.
   * @param {!SheetsLoadConfig} config SheetsLoadConfig Json object.
   * @return {!BatchResult}
   */
  async sendDataInternal(spreadsheets, message, messageId, config) {
    let data;
    try {
      data = JSON.parse(message);
    } catch (error) {
      this.logger.info(`This is not a JSON string. Data is not from GCS.`);
      data = message;
    }
    const sheetName = config.sheetName;
    const pasteDataRequest = Object.assign(
      {
        coordinate: {},
        type: DEFAULT_PASTE_TYPE,
        delimiter: DEFAULT_DELIMITER,
      },
      config.pasteData);
    const coordinate = pasteDataRequest.coordinate;
    try {
      await spreadsheets.clearSheet(sheetName);
      coordinate.sheetId = await spreadsheets.getSheetId(sheetName);
      coordinate.rowIndex = coordinate.rowIndex || 0;
      const sheetHeader = config.sheetHeader;
      if (sheetHeader) {
        await spreadsheets.loadData(sheetHeader, pasteDataRequest, 'header');
        coordinate.rowIndex += sheetHeader.trim().split('\n').length;
      }
      if (data.bucket) {  // Data is a GCS file.
        return this.loadCsvToSheet_(
          spreadsheets, config, pasteDataRequest, data, messageId);
      } else {  // Data comes from the message data.
        await this.setDimensions_(
          spreadsheets, sheetName, data, pasteDataRequest);
        return spreadsheets.loadData(data, pasteDataRequest);
      }
    } catch (error) {
      return {
        result: false,
        errors: [error.toString()],
      }
    }
  }

  /**
   * Set the rows and columns for the target Sheet based on given data.
   * @param {!Spreadsheets} spreadsheets Sheets API invoker.
   * @param {string} sheetName Name of the sheet will be loaded data.
   * @param {string} data CSV data.
   * @param {!ParseDataRequest} pasteDataRequest Definition of PasteDataRequest
   *     for Sheets API batchUpdate operation.
   * @private
   */
  setDimensions_(spreadsheets, sheetName, data, pasteDataRequest) {
    const rows = data.split('\n');
    const targetRows = pasteDataRequest.coordinate.rowIndex + rows.length;
    const targetColumns = rows[0].split(pasteDataRequest.delimiter).length;
    return spreadsheets.reshape(sheetName, targetRows, targetColumns);
  }

  /**
   * Loads the data from CSV to the given Sheet. Google Sheets API has a limit for
   * request size (10MB) and number of concurrent requests.
   * To achieve the best performance, the whole CSV data will be divided into
   * rounds and each round contain several batches. Each batch is a request with a
   * piece of data to be loaded in to the target Sheet. Any round will wait until
   * its batches are all fulfilled before it goes for the next round.
   * @param {!Spreadsheets} spreadsheets Sheets API v4 stub class.
   * @param {!SheetsLoadConfig} config SheetsLoadConfig object.
   * @param {!ParseDataRequest} pasteDataRequest PasteDataRequest for Sheets API
   *     batchUpdate operation.
   * @param {{
   *       bucket:string,
   *       name:string,
   *     }} data CSV file.
   * @param {string} taskId The tag for log.
   * @return {!BatchResult}
   * @private
   */
  async loadCsvToSheet_(spreadsheets, config, pasteDataRequest, data, taskId) {
    const { sheetName, requestLength, numberOfThreads } = config;
    const messageMaxSize = getProperValue(requestLength, MAXIMUM_REQUESTS_LENGTH);
    const roundSize = getProperValue(numberOfThreads, NUMBER_OF_THREADS);
    const storageFile = new StorageFile(data.bucket, data.file);
    const allData = await storageFile.loadContent(0);
    let rowIndex = pasteDataRequest.coordinate.rowIndex;
    await this.setDimensions_(spreadsheets, sheetName, allData, pasteDataRequest);
    const fileSize = await storageFile.getFileSize();
    const splitRanges = await storageFile.getSplitRanges(fileSize,
      messageMaxSize);
    const roundedSplitRanges = splitArray(splitRanges, roundSize);
    const reduceFn = async (previous, singleRound, roundNo) => {
      const roundTag = `${taskId}-${roundNo}`;
      const results = await previous;
      const batchInserts = singleRound.map(([start, end], batchNo) => {
        const batchData = allData.substring(start, end + 1);
        const request = Object.assign({}, pasteDataRequest,
          { coordinate: Object.assign({}, pasteDataRequest.coordinate) }
        );
        request.coordinate.rowIndex = rowIndex;
        rowIndex += batchData.trim().split('\n').length;
        return spreadsheets.loadData(batchData, request,
          `${roundTag}-${batchNo}`);
      });
      const batchResults = await Promise.all(batchInserts);
      const currentResult = mergeBatchResults(batchResults, roundTag);
      return results.concat(currentResult);
    }
    /** @const {!Array<!BatchResult>} */
    const taskResult = await roundedSplitRanges.reduce(reduceFn, []);
    return mergeBatchResults(taskResult, taskId);
  }

}

/** API name in the incoming file name. */
GoogleSheetLoadCsv.code = 'GS';

/** Data for this API will be transferred through GCS by default. */
GoogleSheetLoadCsv.defaultOnGcs = true;

module.exports = {
  SheetsLoadConfig,
  GoogleSheetLoadCsv,
};
