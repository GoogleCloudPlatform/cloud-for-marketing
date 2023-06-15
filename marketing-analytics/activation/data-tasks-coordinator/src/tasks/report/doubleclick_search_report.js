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
 * @fileoverview Interface for DoubleClick Search 360 Reporting.
 */

'use strict';

const {request} = require('gaxios');
const {api: {doubleclicksearch: {DoubleClickSearch}}} =
    require('@google-cloud/nodejs-common');
const {Report} = require('./base_report.js');

/** DoubleClick Search 360 Report class. */
class DoubleClickSearchReport extends Report {

  constructor(config, ds) {
    super(config);
    this.ds = ds || new DoubleClickSearch(super.getOption());
  }

  /** @override */
  generate(parameters) {
    return this.ds.requestReports(this.config).then((reportId) => ({reportId}));
  }

  /** @override */
  isReady(parameters) {
    return this.ds.getReportUrls(parameters.reportId).then((files) => !!files);
  }

  /** @override */
  generateSchema() {
    /** @type {Array<string>} */ const { columns } = this.config;
    const fields = columns.map(({ columnName }) => {
      return {
        name: columnName,
        type: this.getBigQueryDataType_(columnName),
        mode: 'NULLABLE',
      };
    });
    return { fields };
  }

  /**
   * Get BigQuery data type based on column name.
   * @param {string} columnName
   * @return {string} BigQuery data type. See:
   *     https://cloud.google.com/bigquery/docs/schemas#standard_sql_data_types
   */
  getBigQueryDataType_(columnName) {
    switch (columnName) {
      case 'date':
        return 'DATETIME';
      case 'clicks':
      case 'impr':
      case 'visits':
        return 'INT64';
      case 'cost':
      case 'ctr':
        return 'FLOAT64';
      case 'keywordNearMatchEnabled': // in report 'campaign'
      case 'networkTarget': // in report 'campaign'
        return 'STRING';
      default:
        if (columnName.endsWith('Id')) return 'STRING';
        if (columnName.endsWith('Date')) return 'DATETIME';
        if (columnName.endsWith('Enabled')) return 'BOOL';
        if (columnName.endsWith('Target')) return 'FLOAT64';
        if (columnName.endsWith('Bid')) return 'FLOAT64';
        if (columnName.endsWith('Budget')) return 'FLOAT64';
        if (columnName.endsWith('Adjustment')) return 'FLOAT64';
        if (columnName.endsWith('Modifier')) return 'FLOAT64';
        return 'STRING';
    }
  };

  /** @override */
  getContent(parameters) {
    const {reportId} = parameters;
    return this.ds.getReportUrls(reportId)
        .then((files) => {
          // SA360 supports really large size reports (over 900MB).
          // So it's better to let it output single report file.
          if (files.length === 1) {
            console.log('Single file report, using stream to transfer size: ',
                files[0].byteCount);
            return this.ds.getReportFileStream(files[0].url);
          }
          // For a report with multiple files, each files will have a head line. It will cause
          // errors if they are merged together and loaded to BigQuery. Those extra head lines need
          // to be removed. On the otherhand, SA360 can support really large size report, so the
          // best way is to keep it single file and use the Stream way to put it to Cloud Stroage.
          // Following code is more like legacy.
          console.log('Start to extract data from SA360 directly.');
          return files.reduce((results, file, index) => {
            return results.then(
                (array) => this.ds.getReportFile(reportId, index)
                    .then((content) => array.fill(content, index, index + 1)));
          }, Promise.resolve(new Array(files.length))).then(this.merge);
        });
  }

  /**
   * SA360 reports may split into multiple files and each file will have a head
   * line. This function removes redundant headlines when merge files into one
   * report.
   * @param {!Array<string>} contents
   * @return {string}
   */
  merge(contents) {
    if (contents.length === 1) return contents[0];
    return contents.map((content, index) => {
      let result = content;
      // If this is not the first report, remove the head line.
      if (index > 0) {
        result = result.substring(result.indexOf('\n') + 1);
      }
      // If this is not the last report, remove the last line breaker to reduce
      // blank line when merge to another report.
      if (index < contents.length - 1) {
        result = result.substring(0, result.length - 1);
      }
      return result;
    }).join('\n');
  }
}

module.exports = {DoubleClickSearchReport};
