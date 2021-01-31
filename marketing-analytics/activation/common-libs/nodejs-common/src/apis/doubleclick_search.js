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

const {google} = require('googleapis');
const {request} = require('gaxios');
const AuthClient = require('./auth_client.js');
const {getLogger, SendSingleBatch} = require('../components/utils.js');

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
 * Nodejs Google API client library doesn't export this type, so here is a
 * partial typedef of Report Request which only contains essential properties.
 * For complete definition, see:
 * https://developers.google.com/search-ads/v2/reference/reports/request
 * @typedef {{
 *    reportScope: {
 *      agencyId: string,
 *      advertiserId: string,
 *    },
 *    reportType: string,
 *    columns: Array<{
 *      columnName: string,
 *      headerText: string,
 *      startDate: string,
 *      endDate: string
 *    }>,
 *    filters: Array<{
 *      columnName: string,
 *      headerText: string,
 *      startDate: string,
 *      endDate: string
 *    }>|undefined,
 *    timeRange: {
 *      startDate: string,
 *      endDate: string,
 *      changedMetricsSinceTimestamp: datetime|undefined,
 *      changedAttributesSinceTimestamp: datetime|undefined,
 *    },
 *    downloadFormat: 'CSV'|'TSV',
 *    statisticsCurrency: 'usd'|'agency'|'advertiser'|'account',
 *    maxRowsPerFile: integer,
 *    includeDeletedEntities: boolean|undefined,
 *    includeRemovedEntities: boolean|undefined,
 *    verifySingleTimeZone: boolean|undefined,
 *    startRow: integer|undefined,
 *    rowCount: integer|undefined,
 * }}
 */
let ReportRequest;

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
class DoubleClickSearch {
  constructor() {
    const authClient = new AuthClient(API_SCOPES);
    this.auth = authClient.getDefaultAuth();
    /** @const {!google.doubleclicksearch} */
    this.instance = google.doubleclicksearch({
      version: API_VERSION,
      auth: this.auth,
    });
    /**
     * Logger object from 'log4js' package where this type is not exported.
     */
    this.logger = getLogger('API.DS');
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
  updateAvailability(availabilities) {
    const availabilityTimestamp = Date.now();
    for (const availability of availabilities) {
      availability.availabilityTimestamp = availabilityTimestamp;
    }
    this.logger.debug('Sending out availabilities', availabilities);
    return this.instance.conversion
        .updateAvailability({requestBody: {availabilities}})
        .then((response) => {
          this.logger.debug('Get response: ', response);
          return response.status === 200;
        })
        .catch((error) => {
          console.error(error);
          return false;
        });
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
     * @return {!Promise<boolean>}
     */
    return (lines, batchId) => {
      const conversionTimestamp = new Date().getTime();
      const conversions = lines.map((line, index) => {
        const record = JSON.parse(line);
        return Object.assign(
            {
              // Default value, can be overwritten by the exported data.
              conversionTimestamp: conversionTimestamp,
              // Default conversion Id should be unique in a single request.
              // See error code 0x0000011F at here:
              // https://developers.google.com/search-ads/v2/troubleshooting#conversion-upload-errors
              conversionId: conversionTimestamp + index,
            },
            config, record);
      });
      this.logger.debug('Configuration: ', config);
      return this.instance.conversion
          .insert({requestBody: {conversion: conversions}})
          .then((response) => {
            this.logger.debug('Response: ', response);
            console.log(`SA[${batchId}] Insert ${
                response.data.conversion.length} conversions`);
            return true;
          })
          .catch((error) => {
            if (error.code === 400 && error.errors) {//requestValidation error
              const messages = error.errors.map((singleError) => {
                return singleError.message.replace(
                    'The request was not valid. Details: ', '');
              });
              console.log(`SA[${batchId}] partially failed.\n`,
                  messages.join(',\n'));
              return false;
            }
            console.error(
                `SA insert conversions [${batchId}] failed.`, error.message);
            this.logger.debug(
                'Errors in response:', error.response.data.error);
            return false;
          });
    };
  }

  /**
   * There are three steps to get asynchronous reports in SA360:
   * 1. Call Reports.request() to specify the type of data for the report.
   * 2. Call Reports.get() with the report id to check whether it's ready.
   * 3. Call Reports.getFile() to the download the report files.
   * @see https://developers.google.com/search-ads/v2/how-tos/reporting/asynchronous-requests
   *
   * This is the first step.
   * @param {!ReportRequest} requestBody
   * @return {!Promise<string>}
   */
  requestReports(requestBody) {
    return this.instance.reports.request({requestBody})
        .then(({status, data}) => {
          if (status >= 200 && status < 300) return data.id;
          const errorMsg =
              `Fail to request reports: ${JSON.stringify(requestBody)}`;
          console.error(errorMsg, data);
          throw new Error(errorMsg);
        });
  }

  /**
   * Returns the report file links if the report is ready or undefined if not.
   * @param {string} reportId
   * @return {!Promise<undefined|Array<{
   *   url:string,
   *   byteCount:string,
   * }>>}
   */
  getReportUrls(reportId) {
    return this.instance.reports.get({reportId})
        .then(({status, data}) => {
          switch (status) {
            case 200:
              console.log(
                  `Report[${reportId}] has ${data.rowCount} rows and ${data.files.length} files.`);
              return data.files;
            case 202:
              console.log(`Report[${reportId}] is not ready.`);
              break;
            default:
              const errorMsg =
                  `Error in get reports: ${reportId} with status code: ${status}`;
              console.error(errorMsg, data);
              throw new Error(errorMsg);
          }
        });
  }

  /**
   * Get the given part of a report.
   * @param {string} reportId
   * @param {number} reportFragment The index (based 0) of report files.
   * @return {!Promise<string>}
   */
  getReportFile(reportId, reportFragment) {
    return this.instance.reports.getFile({reportId, reportFragment})
        .then((response) => {
          if (response.status === 200) return response.data;
          const errorMsg =
              `Error in get file from reports: ${reportFragment}@${reportId}`;
          console.error(errorMsg, response);
          throw new Error(errorMsg);
        });
  }

  /**
   * Returns a readable stream of the report.
   * In case of the report is large, use stream directly to write out to fit in
   * the resource limited environment, e.g. Cloud Functions.
   * @param {string} url
   * @return {!Promise<ReadableStream>}
   */
  getReportFileStream(url) {
    return this.auth.getRequestHeaders()
        .then((headers) => {
          return request({
            method: 'GET',
            headers,
            url,
            responseType: 'stream',
          });
        }).then((response) => response.data);
  }

}

module.exports = {
  DoubleClickSearch,
  InsertConversionsConfig,
  AvailabilityConfig,
  ReportRequest,
  API_VERSION,
  API_SCOPES,
};
