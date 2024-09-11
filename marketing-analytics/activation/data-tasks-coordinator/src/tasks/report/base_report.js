// Copyright 2019 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this fileAccessObject except in compliance with the License.
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
 * @fileoverview Interface for an external reporting task.
 */

'use strict';

const {TableSchema: BqTableSchema} = require('@google-cloud/bigquery');
const {
  api: {
    doubleclicksearch: { ReportRequest: Sa360LegacyReportConfig },
    googleads: ReportQueryConfig,
    doubleclickbidmanager: { RequestBody: Dv360RequestBody },
  }
} = require('@google-cloud/nodejs-common');
const { StorageFileConfig } = require('../../task_config/task_config_dao.js');

/**
 * Campaign Manager report configuration.
 * @typedef {{
 *   secretName: (string|undefined),
 *   accountId: string,
 *   profileId: string,
 *   reportId: string,
 * }}
 */
let CmReportConfig;

/**
 * DV360 (DoubleClick BidManager) report configuration.
 * For 'requestBody', see:
 * https://developers.google.com/bid-manager/v1.1/queries/runquery#request-body
 * @typedef {{
 *   secretName: (string|undefined),
 *   queryId:string,
 *   requestBody: Dv360RequestBody | undefined,
 * }}
 */
let Dv360ReportConfig;

/**
 * SA360 (Search Ads 360) Reporting API configuration.
 * @typedef {{
 *   secretName: (string|undefined),
 *   customerId: string,
 *   loginCustomerId: string,
 *   query:(string|undefined),
 *   file:(!StorageFileConfig|undefined),
 * }}
 */
let Sa360ReportConfig;

/**
 * General API result configuration.
 * @typedef {{
 *   secretName: (string|undefined),
 *   packageName: string,
 *   api: string,
 *   resource: string,
 *   functionName: (string|undefined),
 *   args: (Object|undefined),
 *   limit: (number|undefined),
 *   entityPath: string,
 *   pageTokenPath: (string|undefined),
 *   fieldMask: (string|undefined),
 * }}
 */
let ApiResultConfig;

/**
 * GoogleAds report configuration.
 * @typedef {{
 *   secretName: (string|undefined),
 *   developerToken: string,
 *   customerId: string|undefined,
 *   loginCustomerId: string|undefined,
 *   query:(string|undefined),
 *   file:(!StorageFileConfig|undefined),
 *   reportQuery: ReportQueryConfig|undefined,
 * }}
 */
let AdsReportConfig;

/**
 * Options for extracts BigQuery Table to Cloud Storage file(s).
 * @typedef {{
 *   target: 'CM',
 *   config: CmReportConfig,
 * } | {
 *   target: 'DV360',
 *   config: Dv360ReportConfig,
 * } | {
 *   target: 'DS',
 *   config: Sa360LegacyReportConfig,
 * } | {
 *   target: 'SA360',
 *   config: Sa360ReportConfig,
 * } | {
 *   target: 'API',
 *   config: ApiResultConfig,
 * } | {
 *   target: 'ADS',
 *   config: AdsReportConfig,
 * }}
 */
let ReportConfig;

/**
 * The base class for a Report what will be used in ReportTask to generate and
 * download the report in a asynchronous way.
 * @abstract
 */
class Report {

  /** @param {ReportConfig} config */
  constructor(config) {
    this.config = config || {};
  }

  /**
   * Gets option object to create a new API object.
   * By default, the API classes will figure out the authorization from env
   * variables. The authorization can also be set in the 'config' so each
   * integration can have its own authorization. This function is used to get the
   * authorization related information from the 'config' object and form an
   * 'option' object for the API classes.
   *
   * @return {{SECRET_NAME:(string|undefined)}}
   */
  getOption() {
    const options = {};
    if (this.config.secretName) options.SECRET_NAME = this.config.secretName;
    return options;
  }

  /**
   * Checks the given error message is a fatal error that should fail
   *  immediately without retry.
   * @param {string} errorMessage
   * @return {bool} Is a fatal error or not.
   */
  isFatalError(errorMessage) { return false; }

  /**
   * Starts to generate a report.
   * @param {Object<string,string>=} parameters Parameters of this instance.
   *     For example, 'config' defines the account id, report id, but to run a
   *     report, there could be detailed conditions, e.g. start date or end
   *     date, these can be passed in parameters.
   *     Same for the other two functions.
   * @return {!Promise<!Object<string,string>>} Information of external
   *     reporting job.
   */
  generate(parameters) { }

  /**
   * Checks whether the report is ready.
   * @param {Object<string,string>=} reportJobInformation
   * @return {!Promise<boolean>}
   */
  isReady(reportJobInformation) { }

  /**
   * Returns the content of the report.
   * @param {Object<string,string>=} reportJobInformation
   * @return {!Promise<string>} Report content
   */
  getContent(reportJobInformation) { }

  /**
   * Returns the schema of current report's data structure to help BigQuery load
   * the data into Table.
   * @param {Object<string,string>=} parameters Parameters of this instance.
   * @return {!BqTableSchema} BigQuery load schema, see:
   *     https://cloud.google.com/bigquery/docs/schemas
   */
  generateSchema(parameters) {
    throw new Error('Unimplemented method.');
  }

  /**
   * Returns whether this report is asynchronous.
   * Different systems have different ways to return reports, synchronous or
   * asynchronous.
   * For synchronous ones, e.g. Google Ads, it will response reports at the
   * request immediately.
   * For asynchronous ones, e.g. Campaign Manager, it will return a 'fileId' to
   * the request of a report. You need to use the 'fileId' to check the status
   * manually. Then it's done, Campaign Manager will returns a 'fileUrl' which
   * is the report file.
   * So different reports determine whether this task is asynchronous.
   * @return {boolean}
   */
  isAsynchronous() {
    return true;
  }
}

module.exports = {
  ReportConfig,
  Report,
};
