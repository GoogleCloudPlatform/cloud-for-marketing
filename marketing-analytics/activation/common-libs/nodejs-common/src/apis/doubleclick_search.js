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
 * @fileoverview Google DoubleClick Search Ads Conversions uploading on Google
 * API Client Library.
 */

'use strict';

const { GoogleApiClient } = require('./base/google_api_client.js');
const {
  getLogger,
  SendSingleBatch,
  BatchResult,
} = require('../components/utils.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/doubleclicksearch',
]);
const API_VERSION = 'v2';

/**
 * Configuration for inserting conversions for Search Ads, includes:
 * segmentationType, segmentationId, segmentationName, currencyCode, type, etc.
 * For more details, see
 * https://developers.google.com/search-ads/v2/reference/conversion/insert
 * @typedef {{
 *   segmentationType:string,
 *   segmentationId:(string|undefined),
 *   segmentationName:(string|undefined),
 *   currencyCode:(string|undefined),
 *   type:(string|undefined),
 *   state:(string|undefined),
 * }}
 */
let InsertConversionsConfig;

/**
 * Configuration for updating availabilities of Search Ads, includes: agencyId,
 * advertiserId, segmentationType, segmentationId, etc.
 * When there is no conversion, this request can let Search Ads be aware that
 * the Floodlight tag is working and there is just no conversions.
 * For details, see
 * https://developers.google.com/search-ads/v2/reference/conversion/updateAvailability
 * @typedef {{
 *   agencyId:string,
 *   advertiserId:string,
 *   segmentationType:string,
 *   segmentationId:string,
 *   segmentationName:(string|undefined),
 *   availabilityTimestamp:(number|undefined),
 * }}
 */
let AvailabilityConfig;

/**
 * DoubleClick Search (DS) Ads 360 API v2 stub.
 * See: https://developers.google.com/search-ads/v2/reference/
 * Conversion type definition, see:
 * https://developers.google.com/search-ads/v2/reference/conversion#resource
 * About uploading offline conversions, see:
 * https://support.google.com/searchads/answer/3465926?hl=en
 * Quota limits, see:
 * https://support.google.com/adsihc/answer/6346075?hl=en
 */
class DoubleClickSearch extends GoogleApiClient {

  /**
   * @constructor
   * @param {!Object<string,string>=} env The environment object to hold env
   *     variables.
   */
  constructor(env = process.env) {
    super(env);
    this.googleApi = 'doubleclicksearch';
    this.logger = getLogger('API.DS');
  }

  /** @override */
  getScope() {
    return API_SCOPES;
  }

  /** @override */
  getVersion() {
    return API_VERSION;
  }

  /**
   * Updates the availabilities of a batch of floodlight activities in
   * DoubleClick Search.
   * See
   * https://developers.google.com/search-ads/v2/reference/conversion/updateAvailability
   * @param {!Array<!AvailabilityConfig>} availabilities Floodlight
   *     availabilities array.
   * @return {!Promise<boolean>} Update result.
   */
  async updateAvailability(availabilities) {
    const availabilityTimestamp = Date.now();
    availabilities.forEach((availability) => {
      availability.availabilityTimestamp = availabilityTimestamp;
    });
    this.logger.debug('Sending out availabilities', availabilities);
    try {
      const doubleclicksearch = await this.getApiClient();
      const response = await doubleclicksearch.conversion.updateAvailability(
          {requestBody: {availabilities}});
      this.logger.debug('Get response: ', response);
      return response.status === 200;
    } catch (e) {
      this.logger.error(e);
      return false;
    }
  }

  /**
   * Returns the function to sends out a request to Search Ads 360 with a batch
   * of conversions.
   * @see https://developers.google.com/search-ads/v2/reference/conversion/insert
   * @param {!InsertConversionsConfig} config Campaign Manager configuration.
   * @return {!SendSingleBatch} Function which can send a batch of hits to
   *     Search Ads 360.
   */
  getInsertConversionFn(config) {
    /**
     * Sends a batch of hits to Search Ads 360.
     * @param {!Array<string>} lines Data for single request. It should be
     *     guaranteed that it doesn't exceed quota limitation.
     * @param {string} batchId The tag for log.
     * @return {!Promise<BatchResult>}
     */
    return async (lines, batchId) => {
      const conversionTimestamp = new Date().getTime();
      const conversions = lines.map((line, index) => {
        const record = JSON.parse(line);
        return Object.assign(
            {
              // Default value, can be overwritten by the exported data.
              conversionTimestamp,
              // Default conversion Id should be unique in a single request.
              // See error code 0x0000011F at here:
              // https://developers.google.com/search-ads/v2/troubleshooting#conversion-upload-errors
              conversionId: conversionTimestamp + index,
            },
            config, record);
      });
      this.logger.debug('Configuration: ', config);
      /** @const {BatchResult} */
      const batchResult = {
        result: true,
        numberOfLines: lines.length,
      };
      try {
        const doubleclicksearch = await this.getApiClient();
        const response = await doubleclicksearch.conversion.insert(
            {requestBody: {conversion: conversions}}
        );
        this.logger.debug('Response: ', response);
        const insertedConversions = response.data.conversion.length;
        if (lines.length !== insertedConversions) {
          const errorMessage =
              `Conversions input/inserted: ${lines.length}/${insertedConversions}`;
          this.logger.warn(errorMessage);
          batchResult.result = false;
          batchResult.numberOfLines = insertedConversions;
          batchResult.errors = [errorMessage];
        }
        this.logger.info(
            `SA[${batchId}] Insert ${insertedConversions} conversions.`);
        return batchResult;
      } catch (error) {
        this.updateBatchResultWithError_(batchResult, error, lines);
        return batchResult;
      }
    };
  }

  /**
   * Updates the BatchResult based on errors.
   * There are 3 types of errors here:
   * The first two errors are from 'Standard Error Responses';
   * The last one is normal Javascript Error object.
   *
   * For more details of 'Standard Error Responses', see:
   * https://developers.google.com/search-ads/v2/standard-error-responses
   *
   * Error 1. error code is not 400, e.g. 403 for no access to SA360 API. This
   *     error fail the whole process and no need to extract detailed lines.
   * Error 2. error code 400 and 'errors' has one or more lines. Each line might
   *     have different failure reason. Failed lines can be extracted to give
   *     users more information.
   *     Note, some failure reason may fail every line, e.g. wrong
   *     'segmentationName' in the config (Error code '0x0000010E', see:
   *     https://developers.google.com/search-ads/v2/troubleshooting#conversion-upload-errors )
   * Error 3. normal JavaScript Error object. No property 'errors'.
   *
   * @param {!BatchResult} batchResult
   * @param {(!GoogleAdsFailure|!Error)} error
   * @param {!Array<string>} lines The original input data.
   * @private
   */
  updateBatchResultWithError_(batchResult, error, lines) {
    batchResult.result = false;
    // Error 3.
    if (!error.errors) {
      batchResult.errors = [error.message || error.toString()];
      return;
    }
    const errorMessages = error.errors.map(({message}) => message);
    // Error 1.
    if (error.code !== 400) {
      batchResult.errors = errorMessages;
      return;
    }
    // Error 2.
    batchResult.failedLines = [];
    batchResult.groupedFailed = {};
    const errors = new Set();
    const messageReg = /.*Details: \[(.*) index=\d+ conversionId=.*/;
    const indexReg = /.*index=(\d*) .*/;
    errorMessages.forEach((message) => {
      const errorMessage = messageReg.exec(message);
      if (errorMessage) {
        const index = indexReg.exec(message);
        const failedLine = lines[index[1]];
        batchResult.failedLines.push(failedLine);
        // error messages have detailed IDs. Need to generalize them.
        const generalMessage =
            errorMessage[1].replace(/ \'[^\']*\'/, '');
        errors.add(generalMessage);
        const groupedFailed = batchResult.groupedFailed[generalMessage] || [];
        groupedFailed.push(failedLine);
        if (groupedFailed.length === 1) {
          batchResult.groupedFailed[generalMessage] = groupedFailed;
        }
      } else {
        errors.add(message);
      }
    });
    batchResult.errors = Array.from(errors);
  }
}

module.exports = {
  DoubleClickSearch,
  InsertConversionsConfig,
  AvailabilityConfig,
  API_VERSION,
  API_SCOPES,
};
