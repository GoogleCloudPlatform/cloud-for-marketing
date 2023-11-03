// Copyright 2022 Google Inc.
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

/** @fileoverview Google Analytics API handler class.*/

class Analytics extends ExternalApi {

  constructor(option) {
    super(option);
    this.apiUrl = 'https://www.googleapis.com/analytics';
    this.version = 'v3';
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/management`;
  }

  /** @override */
  getScope() {
    return 'https://www.googleapis.com/auth/analytics';
  }

  /**
   * Verifies the existence of the custom data source.
   * @see https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/customDataSources/list
   * @param {string} accountId
   * @param {string} webPropertyId
   * @param {string} customDataSourceId
   * @return {VerifyResult}
   */
  verifyDataSource(accountId, webPropertyId, customDataSourceId) {
    const customDataSources = [];
    let link =
      `accounts/${accountId}/webproperties/${webPropertyId}/customDataSources`;
    do {
      const { error, items, nextLink } = this.get(link);
      if (error) {
        return {
          valid: false,
          reason: error.content || error.errors[0].message,
        };
      }
      customDataSources.push(...items);
      link = nextLink;
    } while (link)
    if (customDataSources.some(({ id }) => id === customDataSourceId)) {
      return { valid: true };
    }
    return {
      valid: false,
      reason: `Can not find custom data sources ${customDataSourceId}`,
    };
  }
}
