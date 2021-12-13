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
 * @fileoverview Tentacles API handler for Google Ads Click Conversions
 * uploading (Google Ads API (unofficial) Wrapper).
 */

'use strict';

const {
  api: {googleads: {GoogleAds, ClickConversionConfig}},
  utils: {apiSpeedControl, getLogger, getProperValue, BatchResult},
} = require('@google-cloud/nodejs-common');

/**
 * Conversions per request. Google Ads has a limit as 2000.
 * @see https://developers.google.com/google-ads/api/docs/best-practices/quotas
 */
const RECORDS_PER_REQUEST = 2000;
/**
 * Queries per second. Google Ads has no limits on queries per second, however
 * it has limits on the gRPC size (4MB), so large requests may fail.
 */
const QUERIES_PER_SECOND = 10;

/** API name in the incoming file name. */
exports.name = 'ACLC';

/** Data for this API will be transferred through GCS by default. */
exports.defaultOnGcs = false;

/**
 * Configuration for a Google Ads click conversions upload.
 * @typedef {{
 *   developerToken:string,
 *   customerId:string,
 *   loginCustomerId:(string|undefined),
 *   adsConfig:!ClickConversionConfig,
 *   recordsPerRequest:(number|undefined),
 *   qps:(number|undefined),
 *   debug:(boolean|undefined),
 * }}
 */
let GoogleAdsClickConversionConfig;

exports.GoogleAdsClickConversionConfig = GoogleAdsClickConversionConfig;

/**
 * Sends out the data as click conversions to Google Ads API.
 * This function exposes a googleAds parameter for test
 * @param {GoogleAds} googleAds Injected Google Ads instance.
 * @param {string} records Data to send out as click conversions. Expected JSON
 *     string in each line.
 * @param {string} messageId Pub/sub message ID for log.
 * @param {!GoogleAdsClickConversionConfig} config
 * @return {!Promise<BatchResult>}
 */
const sendDataInternal = async (googleAds, records, messageId, config) => {
  const logger = getLogger('API.ACLC');
  const recordsPerRequest =
      getProperValue(config.recordsPerRequest, RECORDS_PER_REQUEST);
  const qps = getProperValue(config.qps, QUERIES_PER_SECOND, false);
  const managedSend = apiSpeedControl(recordsPerRequest, 1, qps);
  const {customerId, loginCustomerId, adsConfig} = config;
  if (adsConfig.custom_variable_tags) {
    try {
      adsConfig.customVariables = await Promise.all(
          adsConfig.custom_variable_tags.map(async (tag) => {
            const id = await googleAds.getConversionCustomVariableId(tag,
                customerId, loginCustomerId);
            if (!id) throw new Error(`Couldn't find the tag named ${tag}.`);
            return {[tag]: id};
          }));
      logger.debug('Updated adsConfig', adsConfig);
    } catch (error) {
      /** @type {BatchResult} */ const batchResult = {
        result: false,
        errors: [error.message],
      };
      logger.error('Get error', error);
      return batchResult;
    }
  }
  const configuredUpload = googleAds.getUploadConversionFn(customerId,
      loginCustomerId, adsConfig);
  return managedSend(configuredUpload, records, messageId);
};

exports.sendDataInternal = sendDataInternal;

/**
 * Sends out the data as click conversions to Google Ads API.
 * @param {string} records Data to send out as click conversions. Expected JSON
 *     string in each line.
 * @param {string} messageId Pub/sub message ID for log.
 * @param {!GoogleAdsClickConversionConfig} config
 * @return {!Promise<BatchResult>}
 */
const sendData = (records, messageId, config) => {
  const debug = !!config.debug;
  const googleAds = new GoogleAds(config.developerToken, debug);
  return sendDataInternal(googleAds, records, messageId, config);
};

exports.sendData = sendData;
