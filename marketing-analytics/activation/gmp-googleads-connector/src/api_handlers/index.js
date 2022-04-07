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

const {utils: {BatchResult},} = require('@google-cloud/nodejs-common');

const gaDataImport = require('./ga_data_import.js');
const gaMeasurementProtocol = require('./ga_measurement_protocol.js');
const cmConversionsUpload = require('./cm_conversions_upload.js');
const sftpUpload = require('./sftp_upload.js');
const sheetsLoadCsv = require('./sheets_load_csv.js');
const saConversionsInsert = require('./sa_conversions_insert.js');
const pubsubMessageSend = require('./pubsub_message_send.js');
const googleAdsClickConversionUpload = require(
    './google_ads_click_conversions_upload.js');
const googleAdsCustomerMatchUpload = require(
  './google_ads_customer_match_upload.js');
const googleAdsConversionAdjustmentUpload = require(
  './google_ads_conversion_adjustments_upload.js');
const ga4MeasurementProtocol = require('./ga4_measurement_protocol.js');

const {GoogleAnalyticsConfig} = gaDataImport;
const {MeasurementProtocolConfig} = gaMeasurementProtocol;
const {CampaignManagerConfig} = cmConversionsUpload;
const {SftpConfig} = sftpUpload;
const {SheetsLoadConfig} = sheetsLoadCsv;
const {SearchAdsConfig} = saConversionsInsert;
const {PubSubMessageConfig} = pubsubMessageSend;
const {GoogleAdsConversionConfig} = googleAdsClickConversionUpload;
const {GoogleAdsCustomerMatchConfig} = googleAdsCustomerMatchUpload;
const {MpGa4IntegrationConfig} = ga4MeasurementProtocol;

/**
 * API configuration types for all APIs that Tentacles supports.
 *
 * @typedef {(!GoogleAnalyticsConfig|!CampaignManagerConfig|
 * !MeasurementProtocolConfig|!SftpConfig|!SheetsLoadConfig|
 * !SearchAdsConfig|!PubSubMessageConfig|
 * !GoogleAdsConversionConfig|!GoogleAdsCustomerMatchConfig|
 * !MpGa4IntegrationConfig)}
 */
let ApiConfigItem;

/**
 * Definition of API handler function. It takes three parameters:
 * {string} Data to send out.
 * {string} Pub/sub message ID for log.
 * {!ApiConfigItem} API configuration.
 * @typedef {function(string,string,!ApiConfigItem):!BatchResult}
 */
let ApiHandlerFunction;

/**
 * All handlers for the APIs that Tentacles supports.
 * @type {!Object<string,!ApiHandlerFunction>}
 */
const API_HANDLERS = Object.freeze({
  [gaDataImport.name]: gaDataImport,
  [gaMeasurementProtocol.name]: gaMeasurementProtocol,
  [cmConversionsUpload.name]: cmConversionsUpload,
  [sftpUpload.name]: sftpUpload,
  [sheetsLoadCsv.name]: sheetsLoadCsv,
  [saConversionsInsert.name]: saConversionsInsert,
  [pubsubMessageSend.name]: pubsubMessageSend,
  [googleAdsClickConversionUpload.name]: googleAdsClickConversionUpload,
  [googleAdsCustomerMatchUpload.name]: googleAdsCustomerMatchUpload,
  [googleAdsConversionAdjustmentUpload.name]: googleAdsConversionAdjustmentUpload,
  [ga4MeasurementProtocol.name]: ga4MeasurementProtocol,
});

/**
 * Gets all supported API names.
 *
 * @return {!Array<string>}
 */
const getApiNameList = () => Object.keys(API_HANDLERS);

/**
 * Gets the names of APIs that use Cloud Storage as the default data transfer
 * infrastructure.
 * Some APIs are mainly a file uploading process, e.g. Google Analytics Data
 * Import or SFTP. These APIs suit for Cloud Storage rather than Pub/Sub which
 * has a limited size for a batch of data transferred.
 *
 * @return {!Array<string>}
 */
const getApiOnGcs = () =>
    getApiNameList().filter((key) => API_HANDLERS[key].defaultOnGcs);

/**
 * Gets the handler for the given API.
 *
 * @param {string} api The API name.
 * @return {(!ApiHandlerFunction|undefined)}
 */
const getApiHandler = (api) =>
    API_HANDLERS[api] ? API_HANDLERS[api].sendData : undefined;

module.exports = {
  ApiConfigItem,
  ApiHandlerFunction,
  getApiNameList,
  getApiOnGcs,
  getApiHandler,
};
