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
 * @fileoverview Interface for Google Ads Reporting.
 */

'use strict';

const { api: { googleadsapi: { GoogleAdsApi } } }
  = require('@google-cloud/nodejs-common');
const { SearchAdsReport } = require('./search_ads_report.js');

/**
 * Error messages that the task should fail directly without retry process.
 * @type {Array<string>}
 */
const FatalErrors = ['PERMISSION_DENIED: The caller does not have permission'];

/** Google Ads Report class. */
class GoogleAdsReport extends SearchAdsReport {

  constructor(config, apiInstance) {
    super(config, apiInstance);
  }

  /** @override */
  isFatalError(errorMessage) {
    return FatalErrors.some(
      (fatalErrorMessage) => errorMessage.indexOf(fatalErrorMessage) > -1
    );
  }

  /** @override */
  getApiInstance() {
    if (!this.apiInstance) {
      this.apiInstance =
        new GoogleAdsApi(super.getOption(),
          { developerToken: this.config.developerToken });
    }
    return this.apiInstance;
  }

}

module.exports = { GoogleAdsReport };
