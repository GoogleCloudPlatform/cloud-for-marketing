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
 * @fileoverview Tentacles API handler for Campaign Manager Conversions
 * uploading (DfaReport API).
 */

'use strict';

const {
  api: {dfareporting: {DfaReporting, InsertConversionsConfig}},
  utils: {apiSpeedControl, getProperValue},
} = require('nodejs-common');

/**
 * Conversions per request. Campaign Manager has a limit as 1000.
 * see https://developers.google.com/doubleclick-advertisers/quotas
 */
const RECORDS_PER_REQUEST = 1000;
/**
 * Queries per second. Campaign Manager has a limit as 1.
 * see https://developers.google.com/doubleclick-advertisers/quotas
 */
const QUERIES_PER_SECOND = 1;
const NUMBER_OF_THREADS = 10;

/** API name in the incoming file name. */
exports.name = 'CM';

/** Data for this API will be transferred through GCS by default. */
exports.defaultOnGcs = false;

/**
 * Configuration for a Campaign Manager(CM) conversions upload.
 * For CM conversions uploading, a 'profileId' is required as
 * 'InsertConversionsConfig' suggests. But here a property 'cmAccountId' (CM
 * account Id) exists instead. The reason is that different users(email based)
 * have different profiles for the same CM account. In order NOT to bind the
 * configuration to a specific user(email), the function uses CM
 * account Id plus current user(email) to get the current profile. After that,
 * put the profileId into the 'InsertConversionsConfig' and invoke the function
 * to upload conversions.
 *
 * @typedef {{
 *   cmAccountId:string,
 *   recordsPerRequest:(number|undefined),
 *   qps:(number|undefined),
 *   numberOfThreads:(number|undefined),
 *   cmConfig:!InsertConversionsConfig,
 * }}
 */
let CampaignManagerConfig;

exports.CampaignManagerConfig = CampaignManagerConfig;

/**
 * Sends out the data as conversions to Campaign Manager (CM).
 * Gets the CM user profile based on CM account Id and current user, then uses
 * the profile to send out data as CM conversions with speed control and data
 * volume adjustment.
 * This function exposes a DfaReporting parameter for test
 * @param {DfaReporting} dfaReporting Injected DfaReporting instance.
 * @param {string} records Data to send out as conversions. Expected JSON
 *     string in each line.
 * @param {string} messageId Pub/sub message ID for log.
 * @param {!CampaignManagerConfig} config
 * @return {!Promise<boolean>} Whether 'records' have been sent out without any
 *     errors.
 */
const sendDataInternal = (dfaReporting, records, messageId, config) => {
  return dfaReporting.getProfileId(config.cmAccountId).then((profileId) => {
    config.cmConfig.profileId = profileId;
    const recordsPerRequest =
        getProperValue(config.recordsPerRequest, RECORDS_PER_REQUEST);
    const numberOfThreads =
        getProperValue(config.numberOfThreads, NUMBER_OF_THREADS, false);
    const qps = getProperValue(config.qps, QUERIES_PER_SECOND);
    const managedSend =
        apiSpeedControl(recordsPerRequest, numberOfThreads, qps);
    const configedUpload = dfaReporting.getUploadConversionFn(config.cmConfig);
    return managedSend(configedUpload, records, messageId);
  });
};

exports.sendDataInternal = sendDataInternal;

/**
 * Sends out the data as conversions to Campaign Manager (CM).
 * Gets the CM user profile based on CM account Id and current user, then uses
 * the profile to send out data as CM conversions with speed control and data
 * volume adjustment.
 * @param {string} records Data to send out as conversions. Expected JSON
 *     string in each line.
 * @param {string} messageId Pub/sub message ID for log.
 * @param {!CampaignManagerConfig} config
 * @return {!Promise<boolean>} Whether 'records' have been sent out without any
 *     errors.
 */
exports.sendData = (records, messageId, config) => {
  const dfaReporting = new DfaReporting();
  return sendDataInternal(dfaReporting, records, messageId, config);
};

