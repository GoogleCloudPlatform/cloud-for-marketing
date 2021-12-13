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
    doubleclicksearch: {
      DoubleClickSearch,
      InsertConversionsConfig,
      AvailabilityConfig,
    }
  },
  utils: {apiSpeedControl, getProperValue, BatchResult, getLogger},
} = require('@google-cloud/nodejs-common');

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

/** @const {string} API name in the incoming file name. */
exports.name = 'SA';

/**
 * @const {boolean} Data for this API will be transferred through GCS by
 * default.
 */
exports.defaultOnGcs = false;

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
 * }}
 */
let SearchAdsConfig;

exports.SearchAdsConfig = SearchAdsConfig;

/**
 * Sends out the data as conversions to DoubleClick Search Ads (DS).
 * @param {string} records Data to send out as conversions. Expected JSON
 *     string in each line.
 * @param {string} messageId Pub/sub message ID for log.
 * @param {!SearchAdsConfig} config
 * @return {!Promise<BatchResult>}
 */
const sendData = async (records, messageId, config) => {
  const doubleClickSearch = new DoubleClickSearch();
  const logger = getLogger('API.SA');
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
      logger.error(error);
      batchResult.result = false;
      batchResult.errors = [error];
    }
    return batchResult;
  }
  const recordsPerRequest =
      getProperValue(config.recordsPerRequest, RECORDS_PER_REQUEST, false);
  const numberOfThreads =
      getProperValue(config.numberOfThreads, NUMBER_OF_THREADS, false);
  const qps = getProperValue(config.qps, QUERIES_PER_SECOND);
  const managedSend = apiSpeedControl(recordsPerRequest, numberOfThreads, qps);
  const configedUpload =
      doubleClickSearch.getInsertConversionFn(config.saConfig);
  return managedSend(configedUpload, records, messageId);
};

exports.sendData = sendData;
