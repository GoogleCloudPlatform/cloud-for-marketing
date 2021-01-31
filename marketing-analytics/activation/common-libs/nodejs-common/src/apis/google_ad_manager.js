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
 * @fileoverview Google Ad Manager API adapter.
 */

const dfp = require('node-google-dfp');
const AuthClient = require('./auth_client.js');

/** @type {!ReadonlyArray<string>} */
const API_SCOPES = Object.freeze(['https://www.googleapis.com/auth/dfp',]);
const API_VERSION = 'v202005';

/**
 * This is a partial definition of ReportQuery copied from:
 * https://developers.google.com/ad-manager/api/reference/v202005/ReportService.ReportQuery
 *
 * @typedef {{
 *   dimensions: !Array<string>,
 *   adUnitView: 'TOP_LEVEL'|'FLAT'|'HIERARCHICAL',
 *   columns: !Array<string>,
 *   dateRangeType: string,
 *   startDate: Date|undefined,
 *   endDate: Date|undefined,
 *   statement: dfp.Statement,
 *   adxReportCurrency: string,
 *   timeZoneType:'UNKNOWN'|'PUBLISHER'|'AD_EXCHANGE',
 * }}
 */
let ReportQuery;

/**
 * The existent DFP Api library is based on callback functions, so it is
 * inconvenient to use it directly. This class offers the promise-style
 * functions based on the callback-style functions.
 */
class DfpApiPromiseWrapper {

  /**
   * Initialize the service that this instance will use.
   * For available services, see:
   * https://developers.google.com/ad-manager/api/rel_notes
   * @param {dfp.User} dfpService Dfp server instance with authentication.
   * @param {string} serviceName
   * @return {Promise<void>}
   */
  initialize(dfpService, serviceName) {
    return new Promise((resolve, reject) => {
      dfpService.getService(serviceName, (err, service) => {
        if (err) {
          reject(err);
        }
        this.service = service;
        resolve();
      });
    });
  };

  /**
   * Retrieves a saved query with the given Id. This can then be passed to
   * `runReportJob`.
   * See:
   * https://developers.google.com/ad-manager/api/reference/v202005/ReportService#getsavedqueriesbystatement
   * @param {number} id
   * @return {!Promise<!ReportQuery>}
   */
  getSavedQueriesById(id) {
    const statement = new dfp.Statement(`WHERE id = ${id} LIMIT 1`);
    return new Promise((resolve, reject) => {
      this.service.getSavedQueriesByStatement(statement, (err, result) => {
        if (err) {
          reject(err);
        }
        if (!result.rval || !result.rval.results[0]) {
          reject(`Didn't find the report with ID: ${statement}.`);
        }
        resolve(result.rval.results[0].reportQuery);
      });
    });
  };

  /**
   * Initiates the execution of a ReportQuery on the server.
   * See:
   * https://developers.google.com/ad-manager/api/reference/v202005/ReportService#runReportJob
   * @param {!ReportQuery} savedQuery
   * @return {!Promise<long>} The ID of the ReportJob.
   */
  runReportJob(savedQuery) {
    const reportQuery = Object.assign({}, savedQuery);
    // There will be an error if `statement` (filter conditions) missed in the
    // saved query due to the validation of WSDL message. So set a default
    // value for `statement` here.
    if (!reportQuery.statement) {
      reportQuery.statement = {};
    }
    console.log(reportQuery);
    return new Promise((resolve, reject) => {
      this.service.runReportJob({reportJob: {reportQuery}}, (err, result) => {
        if (err) {
          reject(err);
        }
        if (!result.rval) {
          reject(`Error in runReportJob`);
        }
        resolve(result.rval.id);
      });
    });
  };

  /**
   * Returns the ReportJobStatus of the report job with the specified ID.
   * See:
   * https://developers.google.com/ad-manager/api/reference/v202005/ReportService#getreportjobstatus
   * @param {number} reportJobId
   * @return {!Promise<'COMPLETED'|'IN_PROGRESS'|'FAILED'>}
   */
  getReportJobStatus(reportJobId) {
    return new Promise((resolve, reject) => {
      this.service.getReportJobStatus({reportJobId}, (err, result) => {
        if (err) {
          reject(err);
        }
        resolve(result.rval);
      });
    });
  };

  /**
   * Returns the URL at which the report file can be downloaded.
   * The report will be generated as a gzip archive. For more download options,
   * see:
   * https://developers.google.com/ad-manager/api/reference/v202005/ReportService#getreportdownloadurlwithoptions
   * @param {number} reportJobId
   * @param {string=} exportFormat
   * @return {!Promise<string>} Download link.
   */
  getReportDownloadURL(reportJobId, exportFormat = 'CSV_DUMP') {
    return new Promise((resolve, reject) => {
      this.service.getReportDownloadURL({reportJobId, exportFormat},
          (err, result) => {
            if (err) {
              reject(err);
            }
            resolve(result.rval);
          });
    });
  };
}

/**
 * Google Ad Manager (DFP) API stub.
 * Note: this API is SOAP based, the library is
 */
class GoogleAdManager {

  constructor(userId, appName = 'Nodejs DFP API') {
    const authClient = new AuthClient(API_SCOPES);
    //TODO test OAuth?
    const auth = authClient.getServiceAccount();
    this.instance = new dfp.User(userId, appName, API_VERSION);
    this.instance.setClient(auth);
  }

  async getDfpInstance(serviceName = 'ReportService') {
    const instance = new DfpApiPromiseWrapper();
    const dfpServer = await this.instance;
    await instance.initialize(dfpServer, serviceName);
    return instance;
  };

  /**
   * Starts to run a saved query based on the given query Id.
   * @param {number} queryId
   * @return {!Promise<long>} The ID of the ReportJob.
   */
  async runReport(queryId) {
    const dfp = await this.getDfpInstance();
    const savedQuery = await dfp.getSavedQueriesById(queryId);
    console.log(savedQuery);
    const jobId = await dfp.runReportJob(savedQuery);
    return jobId;
  }

  /**
   * Returns the ReportJobStatus of the report job with the specified ID.
   * @param {number} reportJobId
   * @return {!Promise<'COMPLETED'|'IN_PROGRESS'|'FAILED'>}
   */
  async getReportStatus(reportJobId) {
    const dfp = await this.getDfpInstance();
    return dfp.getReportJobStatus(reportJobId);
  }

  /**
   * Returns the URL at which the report file can be downloaded.
   * The report will be generated as a gzip archive. For more download options,
   * see:
   * https://developers.google.com/ad-manager/api/reference/v202005/ReportService#getreportdownloadurlwithoptions
   * @param {number} reportJobId
   * @return {!Promise<string>} Download link.
   */
  async getReport(reportJobId) {
    const dfp = await this.getDfpInstance();
    return dfp.getReportDownloadURL(reportJobId);
  }
}

module.exports = {GoogleAdManager};
