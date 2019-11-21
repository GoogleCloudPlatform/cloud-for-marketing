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
 *     API Client Library.
 */

'use strict';

const {google} = require('googleapis');
const {doubleclick_v2} = require('googleapis');
const AuthClient = require('./auth_client.js');
const {getLogger} = require('../components/utils.js');

const SEARCH_ADS_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/doubleclicksearch',
]);

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
exports.InsertConversionsConfig = InsertConversionsConfig;

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
exports.AvailabilityConfig = AvailabilityConfig;

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
    const authClient = new AuthClient(SEARCH_ADS_SCOPES);

    /** @const {!Promise<!doubleclick_v2.Doubleclicksearch>} */
    this.instance = authClient.getDefaultAuth().then(
        (auth) => google.doubleclicksearch({version: 'v2', auth}));
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
    const availabilityTimestamp = new Date().getTime();
    for (const availability of availabilities) {
      availability.availabilityTimestamp = availabilityTimestamp;
    }
    return this.instance
        .then((searchAds) => {
          this.logger.debug('Sending out availabilities', availabilities);
          return searchAds.conversion.updateAvailability(
              {requestBody: {availabilities}});
        })
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
   * @return {function(!Array<string>, string): !Promise<boolean>} Function
   *     which can send a batch of hits to Search Ads 360.
   */
  getInsertConversionFn(config) {
    /**
     * Sends a batch of hits to Search Ads 360.
     * @param {!Array<string>} lines Data for single request. It should be
     *     guaranteed that it doesn't exceed quota limitation.
     * @param {string} batchId The tag for log.
     * @return {!Promise<boolean>}
     */
    const sendRequest = (lines, batchId) => {
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
      return this.instance.then((searchAds) => {
        return searchAds.conversion
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
      });
    };
    return sendRequest;
  }
}

exports.DoubleClickSearch = DoubleClickSearch;
