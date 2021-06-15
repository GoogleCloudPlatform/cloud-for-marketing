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
 * @fileoverview Interface for DoubleClick BidManager (DV360) Reporting.
 */

'use strict';

const {request} = require('gaxios');
const {api: {doubleclickbidmanager: {DoubleClickBidManager}}} = require(
    '@google-cloud/nodejs-common');
const {Report} = require('./base_report.js');

/** DoubleClick BidManager (DV360) Report class. */
class DoubleClickBidManagerReport extends Report {

  constructor(config, dbm = new DoubleClickBidManager()) {
    super(config);
    this.queryId = this.config.queryId;
    this.requestBody = this.config.requestBody;
    this.dbm = dbm;
  }

  /**
   * DV360 report(query) doesn't return anything (reportId or fileId, etc.) for
   * a `runQuery` request, it just returns status of a report(query) through
   * `getQuery`.
   * So, to get the status of when a report is complete, using `getQuery` to get
   * the `latestReportRunTimeMs` before `runQuery`. After `runQuery`, the
   * `isReady` function will check the value of `latestReportRunTimeMs` for
   * whether the report is ready.
   *
   * Also, there is other way to runQuery, e.g. scheduled reports. At the same
   * time, all 'getQuery' of the same report will share the same result. That is
   * why there is chance we hit a 'running' report when tries to get the status.
   * It's not very clear how to handle that from the business perspective, so
   * currently, just log a warn message here.
   * @override
   */
  async generate(parameters) {
    const {running, latestReportRunTimeMs} = await this.dbm.getQuery(
        this.queryId);
    if (running) {
      console.warn(`DBM Query[${this.queryId}] is running when try to start.`);
    }
    const started = await this.dbm.runQuery(this.queryId, this.requestBody);
    if (started) return {latestReportRunTimeMs};
    throw new Error(`Failed to start DV360 report ${this.queryId}`);
  }

  /** @override */
  async isReady(parameters) {
    const {running, latestReportRunTimeMs} = await this.dbm.getQuery(
        this.queryId);
    return !running &&
        (latestReportRunTimeMs - parameters.latestReportRunTimeMs > 0);
  }

  /** @override */
  async getContent(parameters) {
    const {googleCloudStoragePathForLatestReport: url} = await this.dbm.getQuery(
        this.queryId);
    const response = await request({method: 'GET', url,});
    return this.clean(response.data);
  }

  /**
   * Cleans up the content of report. DV360 reports are unable to customized, so
   * use this function to get rid of unwanted lines, e.g. summary line.
   * @param {string} content
   * @return {string}
   */
  clean(content) {
    const lines = content.split('\n');
    const emptyLine = lines.indexOf('');
    if (emptyLine === 0) {
      throw new Error('First line is empty, wrong DV360 Report format?');
    }
    if (emptyLine === -1) {
      throw new Error('No empty line, wrong DV360 Report format?');
    }
    if (emptyLine === 1) {
      throw new Error('Empty report');
    }
    const possibleSummaryIndex = emptyLine - 1;
    const lastLineExcluded = lines[possibleSummaryIndex].startsWith(',')
        ? possibleSummaryIndex
        : possibleSummaryIndex + 1;
    return lines.slice(0, lastLineExcluded).join('\n');
  }
}

module.exports = {DoubleClickBidManagerReport};
