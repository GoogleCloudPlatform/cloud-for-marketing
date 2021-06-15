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

const {api: {dfareporting: {DfaReporting}}} = require(
    '@google-cloud/nodejs-common');
const {Report} = require('./base_report.js');

/** Campaign Manager Report class. */
class CampaignManagerReport extends Report {

  constructor(config, dfa = new DfaReporting()) {
    super(config);
    this.dfa = dfa;
  }

  /** @override */
  generate(parameters) {
    return this.dfa.runReport(this.config).then((fileId) => ({fileId}));
  }

  /** @override */
  isReady(parameters) {
    return this.dfa.getReportFileUrl(this.config).then((fileUrl) => !!fileUrl);
  }

  /** @override */
  getContent(parameters) {
    return this.dfa.getReportFileUrl(this.config)
        .then((fileUrl) => this.dfa.downloadReportFile(fileUrl))
        .then(this.clean);
  }

  /**
   * Cleans up the content of report. CM reports are unable to customized, so
   * use this function to get rid of unwanted lines, e.g. summary line.
   * @param {string} content
   * @return {string}
   */
  clean(content) {
    const lines = content.split('\n');
    let index = 0;
    while (lines[index] !== 'Report Fields') {
      index++;
      if (index >= lines.length) {
        throw Error(`Can't find 'Report Fields' line. Wrong report format?`);
      }
    }
    let endIndex = lines.length - 1;
    while (!lines[endIndex].startsWith('Grand Total')) {
      endIndex--;
      if (endIndex < 0) {
        throw Error(`Can't find 'Grand Total' line. Wrong report format?`);
      }
    }
    return lines.slice(index + 1, endIndex).join('\n');
  }
}

module.exports = {CampaignManagerReport};
