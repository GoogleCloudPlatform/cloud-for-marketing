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
 * @fileoverview API modules index file. Define all APIs exported modules.
 */

'use strict';

/** @const {!AuthClient} Authentication helper class.*/
exports.AuthClient = require('./auth_client.js');

/**
 * APIs integration class for DFA Reporting API.
 * @const {{
 *   DfaReporting:!DfaReporting,
 *   InsertConversionsConfig:!InsertConversionsConfig,
 * }}
 */
exports.dfareporting = require('./dfa_reporting.js');

/**
 * APIs integration class for Google Analytics Data Import API.
 * @const {{
 *   Analytics:!Analytics,
 *   DataImportConfig:!DataImportConfig,
 * }}
 */
exports.analytics = require('./analytics.js');

/**
 * APIs integration class for Google Analytics Measurement Protocol.
 * @const {{MeasurementProtocol:!MeasurementProtocol}}
 */
exports.measurementprotocol = require('./measurement_protocol.js');

/**
 * Cloud Resource Manager for checking the permissions.
 * @const {{CloudPlatformApis:!CloudPlatformApis}}
 */
exports.cloudplatform = require('./cloud_platform_apis.js');

/**
 * APIs integration class for Google Spreadsheets.
 * @const {{
 *   Spreadsheets:!Spreadsheets,
 *   ParseDataRequest:!ParseDataRequest,
 * }}
 */
exports.spreadsheets = require('./spreadsheets.js');

/**
 * APIs integration class for DoubleClick Search Ads.
 * @const {{
 *   InsertConversionsConfig:!InsertConversionsConfig,
 *   DoubleClickSearch:!DoubleClickSearch,
 * }}
 */
exports.doubleclicksearch = require('./doubleclick_search.js');

/**
 * APIs integration class for DoubleClick BidManager (DV360).
 * @const {{
 *   QueryResource:!QueryResource,
 *   DoubleClickBidManager:!DoubleClickBidManager,
 * }}
 */
exports.doubleclickbidmanager = require('./doubleclick_bidmanager.js');

/**
 * APIs integration class for BigQuery.
 * @const {{BigQuery:!BigQuery}}
 */
exports.bigquery = require('./bigquery.js');

/**
 * APIs integration class for Google Ads.
 * @const {{
 *   GoogleAds:!GoogleAds,
 *   ClickConversionConfig:!ClickConversionConfig,
 *   ReportQueryConfig:!ReportQueryConfig,
 * }}
 */
exports.googleads = require('./google_ads.js');
