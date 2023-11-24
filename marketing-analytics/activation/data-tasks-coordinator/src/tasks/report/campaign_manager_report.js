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
 * @fileoverview Interface for Campaign Manager Reporting.
 */

'use strict';

const { Transform } = require('stream');
const {api: {dfareporting: {DfaReporting}}} = require(
    '@google-cloud/nodejs-common');
const {Report} = require('./base_report.js');

/** Campaign Manager Report class. */
class CampaignManagerReport extends Report {

  constructor(config, dfa) {
    super(config);
    this.dfa = dfa || new DfaReporting(super.getOption());
  }

  /** @override */
  async generate(parameters) {
    const fileId = await this.dfa.runReport(this.config);
    return { fileId };
  }

  /** @override */
  async isReady({ fileId }) {
    const config = Object.assign({}, this.config, { fileId });
    const fileUrl = await this.dfa.getReportFileUrl(config);
    if (fileUrl) return true;
    return false;
  }

  /** @override */
  async getContent({ fileId }) {
    const config = Object.assign({}, this.config, { fileId });
    const fileUrl = await this.dfa.getReportFileUrl(config);
    const stream = await this.dfa.getReportFileStream(fileUrl);
    return this.clean(stream);
  }

  /**
   * Cleans up the content of report. CM reports are unable to be customized, so
   * use this function to get rid of unwanted lines, e.g. summary line.
   * @param {stream} stream
   * @return {string}
   */
  clean(stream) {
    let last = '';
    let started = false;
    const streamReportTransform = new Transform({
      transform(chunk, encoding, callback) {
        const data = chunk.toString();
        let toCheck = last + data;
        if (!started) {
          const startIndex = toCheck.indexOf('Report Fields');
          if (startIndex === -1) {
            last = toCheck;
            callback(null, '');
          } else {
            last = '';
            toCheck = toCheck.substring(startIndex + 'Report Fields'.length + 1);
            started = true;
          }
        }
        if (started) {
          const endIndex = toCheck.indexOf('Grand Total:');
          if (endIndex === -1) {
            const output = last;
            last = last === '' ? toCheck : data;
            callback(null, output);
          } else {
            callback(null, toCheck.substring(0, endIndex));
            this.end();
          }
        }
      }
    });
    stream.on('error', (error) => streamReportTransform.emit('error', error));
    streamReportTransform.on('end', () => {
      if (!started) {
        streamReportTransform.emit('error',
          new Error(`Can't find 'Report Fields' line. Wrong report format?`));
      }
    });
    return stream.pipe(streamReportTransform);
  }
}

module.exports = {CampaignManagerReport};
