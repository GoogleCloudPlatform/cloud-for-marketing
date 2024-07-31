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
const { Transform } = require('stream');
const {api: {doubleclickbidmanager: {DoubleClickBidManager}}} = require(
    '@google-cloud/nodejs-common');
const {Report} = require('./base_report.js');

/** DoubleClick BidManager (DV360) Report class. */
class DoubleClickBidManagerReport extends Report {

  constructor(config, dbm) {
    super(config);
    this.queryId = this.config.queryId;
    this.requestBody = this.config.requestBody;
    this.dbm = dbm || new DoubleClickBidManager(super.getOption());
  }

  /**
   * @override
   */
  async generate(parameters) {
    const reportId = await this.dbm.runQuery(this.queryId, this.requestBody);
    if (reportId) return { reportId };
    throw new Error(`Failed to start DV360 report ${this.queryId}`);
  }

  /** @override */
  async isReady(parameters) {
    const { status } =
      await this.dbm.getQueryReport(this.queryId, parameters.reportId);
    if (status && status.state === 'FAILED') {
      throw new Error(
        `Report run failed Query[${this.queryId}], report[${parameters.reportId}].`);
    }
    return status && status.state === 'DONE';
  }

  /** @override */
  async getContent(parameters) {
    const { googleCloudStoragePath: url } =
      await this.dbm.getQueryReport(this.queryId, parameters.reportId);
    if (!url) {
      throw new Error(
        `Can't find url query[${this.queryId}], report[${parameters.reportId}]`);
    }
    const response = await request({
      method: 'GET',
      url,
      responseType: 'stream',
    });
    return this.clean(response.data);
  }

  /**
   * Cleans up the content of report. DV360 reports are unable to be customized.
   * The summary and content are separated by an empty line. So '\n\n' is used
   * to check whether the content is completed. To prevent the two '\n' being
   * separated in two different chunks, an extra parameter 'last' is used to
   * store data from the last chunk.
   *
   * @param {stream} reportStream
   * @return {string}
   */
  clean(reportStream) {
    const END_TAG = '\n\n';
    let previousPiece = '';
    let ended = false;
    const streamReportTransform = new Transform({
      transform(chunk, encoding, callback) {
        const currentPiece = chunk.toString();
        const toCheck = previousPiece + currentPiece;
        if (ended) return callback();
        if (toCheck.indexOf(END_TAG) === -1) {
          const output = previousPiece;
          previousPiece = currentPiece;
          callback(null, output);
        } else {
          ended = true;
          const latest =
            toCheck.substring(0, toCheck.indexOf(END_TAG)).split('\n');
          if (latest[latest.length - 1].startsWith(',')) {
            latest.pop();
          }
          this.push(latest.join('\n'));
          this.push(null);
          callback();
        }
      }
    });
    reportStream
      .on('error', (error) => streamReportTransform.emit('error', error));
    return reportStream.pipe(streamReportTransform);
  }
}

module.exports = {DoubleClickBidManagerReport};
