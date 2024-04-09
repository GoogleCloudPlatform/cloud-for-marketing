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
 * @fileoverview Interface for Google Ads Reporting.
 */

'use strict';

const {Transform} = require('stream');
const {
  api: {
    googleads: {
      GoogleAds,
      GoogleAdsField,
    }
  },
  utils: { extractObject, changeObjectNamingFromSnakeToLowerCamel },
} = require('@google-cloud/nodejs-common');
const {Report} = require('./base_report.js');
const {getSchemaFields} = require('./googleads_report_helper.js');

/**
 * Error messages that the task should fail directly without retry process.
 * @type {Array<string>}
 */
const FatalErrors = ['PERMISSION_DENIED: The caller does not have permission'];

/** Google Ads Report class. */
class GoogleAdsReport extends Report {

  constructor(config, ads) {
    super(config);
    this.ads = ads ||
      new GoogleAds(config.developerToken, false, super.getOption());
  }

  /** @override */
  isFatalError(errorMessage) {
    return FatalErrors.some(
        (fatalErrorMessage) => errorMessage.indexOf(fatalErrorMessage) > -1
    );
  }

  /** @override */
  generate(parameters) {
    return Promise.resolve();
  }

  /** @override */
  isReady(parameters) {
    return Promise.resolve(true);
  }

  /** @override */
  async generateSchema() {
    /** @type {Array<string>} */ const adsFieldNames =
        (this.config.reportQuery.segments || [])
            .concat(this.config.reportQuery.metrics || [])
            .concat(this.config.reportQuery.attributes || []);
    const adsFields = (await this.ads.searchMetaData(this.config.loginCustomerId,
      adsFieldNames)).map(changeObjectNamingFromSnakeToLowerCamel);
    const fields = getSchemaFields(adsFieldNames, adsFields, true);
    return {fields};
  }

  /** @override */
  isAsynchronous() {
    return false;
  }

  /**
   * Get content from Google Ads API based on the source AdsReportConfig.
   * @override
   */
  async getContent(parameters) {
    const customerId = this.config.customerId;
    const loginCustomerId = this.config.loginCustomerId;
    const reportQueryConfig = this.config.reportQuery;

    const streamReportTransform = new Transform({
      writableObjectMode: true,
      transform(chunk, encoding, callback) {
        const {field_mask: {paths}} = chunk;
        const extractor = extractObject(paths);
        // Add a line break after each chunk to keep files in proper format.
        const data = chunk.results.map(extractor).map(JSON.stringify).join('\n')
            + '\n';
        callback(null, data);
      }
    });

    const stream = (await this.ads.streamReport(customerId, loginCustomerId,
        reportQueryConfig))
        .on('error', (error) => streamReportTransform.emit('error', error));
    return stream.pipe(streamReportTransform);
  }
}

module.exports = {GoogleAdsReport};
