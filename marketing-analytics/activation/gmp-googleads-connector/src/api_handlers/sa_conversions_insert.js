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
 * @fileoverview Tentacles API handler for DoubleClick Search Ads Conversions
 * uploading.
 */

'use strict';

const {
  api: {
    doubleclicksearch:
    { DoubleClickSearch, InsertConversionsConfig, AvailabilityConfig }
  },
  utils: { getProperValue, BatchResult },
} = require('@google-cloud/nodejs-common');
const { ApiHandler } = require('./api_handler.js');

/**
 * Conversions per request.
 */
const RECORDS_PER_REQUEST = 200;
/**
 * Queries per second. DoubleClick Search Ads has a limit as 20.
 * see https://developers.google.com/search-ads/pricing#quotas
 */
const QUERIES_PER_SECOND = 20;
const NUMBER_OF_THREADS = 10;

/**
 * Configuration for a Search Ads 360(SA) conversions upload.
 * For SA conversions uploading,
 *
 * @typedef {{
 *   recordsPerRequest:(number|undefined),
 *   qps:(number|undefined),
 *   numberOfThreads:(number|undefined),
 *   saConfig:!InsertConversionsConfig,
 *   availabilities:(!Array<!AvailabilityConfig>|undefined),
 *   secretName:(string|undefined),
 * }}
 */
let SearchAdsConfig;

/**
 * Conversion upload for Search Ads.
 */
class SearchAdsConversionUpload extends ApiHandler {

  /** @override */
  getSpeedOptions(config) {
    const recordsPerRequest =
      getProperValue(config.recordsPerRequest, RECORDS_PER_REQUEST, false);
    const numberOfThreads =
      getProperValue(config.numberOfThreads, NUMBER_OF_THREADS, false);
    const qps = getProperValue(config.qps, QUERIES_PER_SECOND);
    return { recordsPerRequest, numberOfThreads, qps };
  }
  /**
   * Sends out the data as conversions to Search Ads (SA).
   * @param {string} records Data to send out as conversions. Expected JSON
   *     string in each line.
   * @param {string} messageId Pub/sub message ID for log.
   * @param {!SearchAdsConfig} config
   * @return {!Promise<BatchResult>}
   */
  sendData(records, messageId, config) {
    const doubleClickSearch = new DoubleClickSearch(this.getOption(config));
    return this.sendDataInternal(doubleClickSearch, records, messageId, config);
  }

  /**
   * Sends out the data as conversions to Search Ads (SA).
   * This function exposes a DoubleClickSearch parameter for test.
   * @param {DoubleClickSearch} doubleClickSearch DoubleClickSearch instance.
   * @param {string} records Data to send out as conversions. Expected JSON
   *     string in each line.
   * @param {string} messageId Pub/sub message ID for log.
   * @param {!SearchAdsConfig} config
   * @return {!Promise<BatchResult>}
   */
  async sendDataInternal(doubleClickSearch, records, messageId, config) {
    if (records.trim() === '') {
      /** @type {!BatchResult} */
      const batchResult = {
        numberOfLines: 0,
      };
      if (config.availabilities) {
        batchResult.result = await doubleClickSearch.updateAvailability(
          config.availabilities);
      } else {
        const error = 'Empty file with no availabilities settings. Quit.';
        this.logger.error(error);
        batchResult.result = false;
        batchResult.errors = [error];
      }
      return batchResult;
    }
    const managedSend = this.getManagedSendFn(config);
    const configedUpload =
      doubleClickSearch.getInsertConversionFn(config.saConfig);
    return managedSend(configedUpload, records, messageId);
  }

}

/** @const {string} API name in the incoming file name. */
SearchAdsConversionUpload.code = 'SA';

module.exports = {
  SearchAdsConfig,
  SearchAdsConversionUpload,
};
