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
  api: {googleads: {GoogleAds, ConversionConfig}},
  utils: { getProperValue, BatchResult },
} = require('@google-cloud/nodejs-common');
const { ApiHandler } = require('./api_handler.js');

/**
 * Conversions per request. Google Ads has a limit as 2000.
 * @see https://developers.google.com/google-ads/api/docs/best-practices/quotas#conversion_upload_service
 * Conversion adjustments per request. Google Ads has a limit as 2000.
 * @see https://developers.google.com/google-ads/api/docs/best-practices/quotas#conversion_adjustment_upload_service
 */
const RECORDS_PER_REQUEST = 2000;
/**
 * Queries per second. Google Ads has no limits on queries per second, however
 * it has limits on the gRPC size (4MB), so large requests may fail.
 */
const QUERIES_PER_SECOND = 1;

/**
 * Configuration for a Google Ads click conversions upload.
 * @typedef {{
 *   developerToken:string,
 *   customerId:string,
 *   loginCustomerId:(string|undefined),
 *   adsConfig:!ConversionConfig,
 *   recordsPerRequest:(number|undefined),
 *   qps:(number|undefined),
 *   debug:(boolean|undefined),
 *   secretName:(string|undefined),
 * }}
 */
let GoogleAdsConversionConfig;

/**
 * Offline (click) conversions upload for Google Ads.
 */
class GoogleAdsClickConversionUpload extends ApiHandler {

  /** @override */
  getSpeedOptions(config) {
    const recordsPerRequest =
      getProperValue(config.recordsPerRequest, RECORDS_PER_REQUEST);
    const numberOfThreads = 1;
    const qps = getProperValue(config.qps, QUERIES_PER_SECOND, false);
    return { recordsPerRequest, numberOfThreads, qps };
  }

  /**
   * Sends out the data as click conversions to Google Ads API.
   * @param {string} records Data to send out as click conversions. Expected JSON
   *     string in each line.
   * @param {string} messageId Pub/sub message ID for log.
   * @param {!GoogleAdsConversionConfig} config
   * @return {!Promise<BatchResult>}
   * @override
   */
  sendData(records, messageId, config) {
    const googleAds = this.getGoogleAds(config);
    return this.sendDataInternal(googleAds, records, messageId, config);
  }

  /**
   * Gets an instances of GoogleAds class. Google Ads API is used in multiple
   * connectors, including:
   * 1. Click Conversions upload
   * 2. Call Conversions upload
   * 3. Enhanced Conversions upload
   * 4. Customer match user data upload
   * 5. User data offline job service to upload customer match or store sales data
   *
   * @param {{
  *   developerToken:string,
  *   debug:(boolean|undefined),
  *   secretName:(string|undefined),
  * }} config The Google Ads API based connectors configuration.
  * @return {!GoogleAds}
  */
  getGoogleAds(config) {
    const { developerToken, debug } = config;
    return new GoogleAds(
      developerToken, this.getDebug(debug), this.getOption(config));
  }

  /**
   * Returns `BatchResult` based on the error. For `GoogleAdsFailure` error, it
   * reuses the method in GoogleAds class to extract the message.
   *
   * @param {!GoogleAds} googleAds
   * @param {Error} error
   * @param {Array<string>=} lines
   * @return {!BatchResult}
   */
  getResultFromError(googleAds, error, lines = []) {
  /** @const {BatchResult} */ const batchResult = {
      result: true,
      numberOfLines: lines.length,
  };
    googleAds.updateBatchResultWithError(batchResult, error, lines, 2);
    return batchResult;
  }

  /**
   * Sets customer variable tags for Google Ads conversions.
   * @param {GoogleAds} googleAds
   * @param {GoogleAdsConversionConfig} config
   * @return {!BatchResult|undefined}
   */
  async setCustomVariable(googleAds, config) {
    const { customerId, loginCustomerId, adsConfig } = config;
    if (adsConfig.custom_variable_tags) {
      try {
        adsConfig.customVariables = await Promise.all(
          adsConfig.custom_variable_tags.map(async (tag) => {
            const id = await googleAds.getConversionCustomVariableId(tag,
              customerId, loginCustomerId);
            if (!id) throw new Error(`Couldn't find the tag named ${tag}.`);
            return { [tag]: id };
          }));
        this.logger.debug('Updated adsConfig', adsConfig);
      } catch (error) {
        this.logger.error('Error in UploadClickConversion: ', error);
        return this.getResultFromError(googleAds, error);
      }
    }
  }

  /**
   * Sends out the data as click conversions to Google Ads API.
   * This function exposes a googleAds parameter for test.
   * @param {GoogleAds} googleAds Injected Google Ads instance.
   * @param {string} records Data to send out as click conversions. Expected JSON
   *     string in each line.
   * @param {string} messageId Pub/sub message ID for log.
   * @param {!GoogleAdsConversionConfig} config
   * @return {!Promise<BatchResult>}
   */
  async sendDataInternal(googleAds, records, messageId, config) {
    const result = await this.setCustomVariable(googleAds, config);
    if (result) return result;
    const managedSend = this.getManagedSendFn(config);
    const { customerId, loginCustomerId, adsConfig } = config;
    const configuredUpload = googleAds.getUploadClickConversionFn(customerId,
      loginCustomerId, adsConfig);
    return managedSend(configuredUpload, records, messageId);
  };
}

/** API name in the incoming file name. */
GoogleAdsClickConversionUpload.code = 'ACLC';

module.exports = {
  GoogleAdsConversionConfig,
  GoogleAdsClickConversionUpload,
};
