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
 * @fileoverview Hosts all API handlers that Tentacles supports.
 */

'use strict';

const { ApiHandlerFunction, ApiHandler } = require('./api_handler.js');
const { GoogleAnalyticsConfig, GoogleAnalyticsDataImport }
  = require('./ga_data_import.js');
const { MeasurementProtocolConfig, GoogleAnalyticsMeasurementProtocol }
  = require('./ga_measurement_protocol.js');
const { MpGa4IntegrationConfig, MeasurementProtocolForGoogleAnalytics4 }
  = require('./ga4_measurement_protocol.js');
const { CampaignManagerConfig, CampaingManagerConversionUpload }
  = require('./cm_conversions_upload.js');
const { SearchAdsConfig, SearchAdsConversionUpload }
  = require('./sa_conversions_insert.js');
const { SftpConfig, SftpUpload } = require('./sftp_upload.js');
const { PubSubMessageConfig, PubSubMessageSend }
  = require('./pubsub_message_send.js');
const { SheetsLoadConfig, GoogleSheetLoadCsv } = require('./sheets_load_csv.js');
const { GoogleAdsConversionConfig, GoogleAdsClickConversionUpload }
  = require('./google_ads_click_conversions_upload.js');
const { GoogleAdsCallConversionUpload }
  = require('./google_ads_call_conversions_upload.js');
const { GoogleAdsConversionAdjustment } =
  require('./google_ads_conversion_adjustments_upload.js');
const { GoogleAdsCustomerMatchConfig, GoogleAdsCustomerMatch }
  = require('./google_ads_customer_match_upload.js');
const { GoogleAdsOfflineUserDataJobConfig, GoogleAdsOfflineUserDataJobUpload }
  = require('./google_ads_offline_userdata_job.js');
const { CloudFunctionInvokeConfig, CloudFunctionInvoke }
  = require('./cloud_function_invoke.js');

/**
 * API configuration types for all APIs that Tentacles supports.
 *
 * @typedef {(!GoogleAnalyticsConfig|!CampaignManagerConfig|
 * !MeasurementProtocolConfig|!SftpConfig|!SheetsLoadConfig|
 * !SearchAdsConfig|!PubSubMessageConfig|
 * !GoogleAdsConversionConfig|!GoogleAdsCustomerMatchConfig|
 * !GoogleAdsOfflineUserDataJobConfig|!MpGa4IntegrationConfig|
 * !CloudFunctionInvokeConfig)}
 */
let ApiConfigItem;

/**
 * All handlers for the APIs that Tentacles supports.
 * @type {!Array<!ApiHandler>}
 */
const API_HANDLERS = [
  CampaingManagerConversionUpload,
  GoogleAnalyticsDataImport,
  GoogleAnalyticsMeasurementProtocol,
  MeasurementProtocolForGoogleAnalytics4,
  GoogleAdsClickConversionUpload,
  GoogleAdsCallConversionUpload,
  GoogleSheetLoadCsv,
  SftpUpload,
  SearchAdsConversionUpload,
  PubSubMessageSend,
  GoogleAdsConversionAdjustment,
  GoogleAdsCustomerMatch,
  GoogleAdsOfflineUserDataJobUpload,
  CloudFunctionInvoke,
];

/**
 * Gets all supported API names.
 * Only used for bash script to generate Pub/Sub topics/subscriptons.
 * @return {!Array<string>}
 */
const getApiNameList = () => {
  return API_HANDLERS.map(({ code }) => code);
};

/**
 * Gets the names of APIs that use Cloud Storage as the default data transfer
 * infrastructure.
 * Some APIs are mainly a file uploading process, e.g. Google Analytics Data
 * Import or SFTP. These APIs suit for Cloud Storage rather than Pub/Sub which
 * has a limited size for a batch of data transferred.
 *
 * @return {!Array<string>}
 */
const getApiOnGcs = () => {
  return API_HANDLERS
    .filter(({ defaultOnGcs }) => defaultOnGcs).map(({ code }) => code);
};

/**
 * Gets the handler for the given API.
 *
 * @param {string} api The API name.
 * @return {(!ApiHandler|undefined)}
 */
const getApiHandler = (api) => {
  const handler = API_HANDLERS.filter(({ code }) => code === api)[0];
  return handler ? new handler() : undefined;
};

module.exports = {
  ApiConfigItem,
  ApiHandlerFunction,
  getApiNameList,
  getApiOnGcs,
  getApiHandler,
};
