// Copyright 2024 Google Inc.
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
 * @fileoverview Interface for Search Ads 360 Reporting API.
 */

'use strict';

const {
  api: { searchads: { SearchAds } },
  storage: { StorageFile },
  utils: { replaceParameters, changeNamingFromSnakeToLowerCamel },
} = require('@google-cloud/nodejs-common');
const { Report } = require('./base_report.js');
const { getSchemaFields } = require('./googleads_report_helper.js');

/** Search Ads 360 Report class. */
class SearchAdsReport extends Report {

  /**@override */
  constructor(config, apiInstance) {
    super(config);
    this.apiInstance = apiInstance;
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
  isAsynchronous() {
    return false;
  }

  /**
   * Initializes and returns the Api instance which will handle requests to the
   * target Api.
   */
  getApiInstance() {
    if (!this.apiInstance) {
      this.apiInstance = new SearchAds(super.getOption());
    }
    return this.apiInstance;
  }

  /** @override */
  async generateSchema() {
    const query = await this.getFinalQuery(this.parameters);
    const { snakeCase = false } = this.config;
    /** @type {Array<string>} */ const adsFieldNames =
      query.replaceAll('\n', '').replace(/select(.*)from.*/gi, '$1')
        .split(',').map((field) => field.trim());
    const adsFields =
      await this.getApiInstance().searchReportField(adsFieldNames);
    const fields = getSchemaFields(adsFieldNames, adsFields, snakeCase);
    return { fields };
  }

  /**
   * @override
   */
  async getContent(parameters) {
    const query = await this.getFinalQuery(parameters);
    const { customerId, loginCustomerId, snakeCase = false } = this.config;
    const stream = await this.getApiInstance().cleanedRestStreamReport(
      customerId, loginCustomerId, query, snakeCase);
    return stream;
  }

  /**
   * Returns the report query with parameters replaced.
   * @return {!Promise<string>}
   */
  async getFinalQuery(parameters) {
    const query = await this.getQuery();
    return replaceParameters(query, parameters);
  }

  /**
   * Returns the report query based on configuration.
   * @return {!Promise<string>}
   */
  async getQuery() {
    if (this.config.query) return this.config.query;
    if (this.config.file) {
      /** @const {StorageFileConfig} */
      const { bucket, name, projectId } = this.config.file;
      const query = await StorageFile
        .getInstance(bucket, name, { projectId }).loadContent(0);
      return query;
    }
    throw new Error(`Fail to find report query for ReportTask.`);
  }
}

module.exports = { SearchAdsReport };
