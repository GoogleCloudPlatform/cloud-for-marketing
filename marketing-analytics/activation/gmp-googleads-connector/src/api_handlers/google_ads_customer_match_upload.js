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
 * @fileoverview Tentacles API handler for Google Ads Customer Match
 * uploading (Google Ads API (unofficial) Wrapper).
 */

'use strict';

const {
  api: {googleads: {GoogleAds, CustomerMatchConfig}},
  utils: {apiSpeedControl, getProperValue},
} = require('@google-cloud/nodejs-common');

/**
 * Mutate requests. Google Ads has a limit as 5000.
 * @see https://developers.google.com/google-ads/api/docs/best-practices/quotas
 * However UserDataService has limit of 10 operations and 100 userIds per request
 * @see https://developers.google.com/google-ads/api/docs/migration/user-data-service#rate_limits
 */
const RECORDS_PER_REQUEST = 1000;

/**
 * Queries per second. Google Ads has no limits on queries per second, however
 * it has limits on the gRPC size (4MB), so large requests may fail.
 */
const QUERIES_PER_SECOND = 1;

/** API name in the incoming file name. */
exports.name = 'ACM';

/** Data for this API will be transferred through GCS by default. */
exports.defaultOnGcs = false;

/**
 * Configuration for a Google Ads customer match upload.
 * @typedef {{
 *   developerToken:string,
 *   customerMatchConfig: !CustomerMatchConfig,
 *   recordsPerRequest:(number|undefined),
 *   qps:(number|undefined),
 * }}
 */
let GoogleAdsCustomerMatchConfig;

exports.GoogleAdsCustomerMatchConfig = GoogleAdsCustomerMatchConfig;

/**
 * Sends out the data as user ids to Google Ads API.
 * This function exposes a googleAds parameter for test
 * @param {GoogleAds} googleAds Injected Google Ads instance.
 * @param {string} records Data to send out as user ids. Expected JSON
 *     string in each line.
 * @param {string} messageId Pub/sub message ID for log.
 * @param {!GoogleAdsCustomerMatchConfig} config
 * @return {!Promise<boolean>} Whether 'records' have been sent out without any
 *     errors.
 */
const sendDataInternal = (googleAds, records, messageId, config) => {
  const recordsPerRequest =
      getProperValue(config.recordsPerRequest, RECORDS_PER_REQUEST);
  const qps = getProperValue(config.qps, QUERIES_PER_SECOND, false);
  const managedSend = apiSpeedControl(recordsPerRequest, 1, qps);
  const configedUpload = googleAds.getUploadCustomerMatchFn(
      config.customerMatchConfig);
  return managedSend(configedUpload, records, messageId);
};

exports.sendDataInternal = sendDataInternal;

/**
 * Sends out the data as user ids to Google Ads API.
 * @param {string} records Data to send out as user id. Expected JSON
 *     string in each line.
 * @param {string} messageId Pub/sub message ID for log.
 * @param {!GoogleAdsCustomerMatchConfig} config
 * @return {!Promise<boolean>} Whether 'records' have been sent out without any
 *     errors.
 */
const sendData = (records, messageId, config) => {
  const googleAds = new GoogleAds(config.developerToken);
  return sendDataInternal(googleAds, records, messageId, config);
};

exports.sendData = sendData;
