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

const GoogleAnalyticsDataImport = require('./ga_data_import.js');
const MeasurementProtocol = require('./ga_measurement_protocol.js');
const CampaignManagerConversionsUpload = require('./cm_conversions_upload.js');
const SftpUpload = require('./sftp_uploader.js');
const SheetsLoadCsv = require('./sheets_load_csv.js');

const {GoogleAnalyticsConfig} = GoogleAnalyticsDataImport;
const {MeasurementProtocolConfig} = MeasurementProtocol;
const {CampaignManagerConfig} = CampaignManagerConversionsUpload;
const {SftpConfig} = SftpUpload;
const {SheetsLoadConfig} = SheetsLoadCsv;

/**
 * API configuration types for all APIs that Tentacles supports.
 *
 * @typedef {(!GoogleAnalyticsConfig|!CampaignManagerConfig|
 * !MeasurementProtocolConfig|!SftpConfig|!SheetsLoadConfig)}
 */
let ApiConfigItem;

exports.ApiConfigItem = ApiConfigItem;

/**
 * Definition of API handler function. It takes three parameters:
 * {string} Data to send out.
 * {string} Pub/sub message ID for log.
 * {!ApiConfigItem} API configuration.
 * @typedef {function(string,string,!ApiConfigItem):!Promise<boolean>}
 */
let ApiHandlerFunction;

exports.ApiHandlerFunction = ApiHandlerFunction;

/**
 * All handlers for the APIs that Tentacles supports.
 * @type {!Object<string,!ApiHandlerFunction>}
 */
const API_HANDLERS = Object.freeze({
  [GoogleAnalyticsDataImport.name]: GoogleAnalyticsDataImport,
  [MeasurementProtocol.name]: MeasurementProtocol,
  [CampaignManagerConversionsUpload.name]: CampaignManagerConversionsUpload,
  [SftpUpload.name]: SftpUpload,
  [SheetsLoadCsv.name]: SheetsLoadCsv,
});

/**
 * Gets all supported API names.
 *
 * @return {!Array<string>}
 */
exports.getApiNameList = () => {
  return Object.keys(API_HANDLERS);
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
exports.getApiOnGcs = () => {
  return Object.keys(API_HANDLERS)
      .filter((key) => API_HANDLERS[key].defaultOnGcs);
};

/**
 * Gets the handler for the given API.
 *
 * @param {string} api The API name.
 * @return {(!ApiHandlerFunction|undefined)}
 */
exports.getApiHandler = (api) => {
  if (API_HANDLERS[api]) return API_HANDLERS[api].sendData;
};
