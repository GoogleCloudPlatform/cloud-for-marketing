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

/** @fileoverview Search Ads Reporting API handler class.*/

class SearchAdsReporting extends ExternalApi {
  constructor(option, mccCid) {
    super(option);
    this.name = 'Search Ads 360 Reporting API';
    this.api = 'searchads360.googleapis.com';
    this.apiUrl = 'https://searchads360.googleapis.com';
    this.version = 'v0';
    this.mccCid = GoogleAds.getCleanedId(mccCid);
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /** @override */
  getScope() {
    return 'https://www.googleapis.com/auth/doubleclicksearch';
  }

  /**
   * Verifies the accessibility of current user to the Search Ads account.
   * @return {VerifyResult}
   */
  verifyReportAccess() {
    const payload = {query: 'SELECT customer.id FROM customer'};
    const {error} = this.mutate(
      `customers/${this.mccCid}/searchAds360:search`,
      payload,
    );
    if (error) {
      return {
        valid: false,
        reason: error.details[0].errors.map(({message}) => message).join(' '),
      };
    }
    return {valid: true};
  }

  listAccessibleCustomers() {
    const response = this.get(`customers:listAccessibleCustomers`);
    console.log(response);
    const {error} = response;
    if (error) {
      return {
        valid: false,
        reason: error.details[0].errors.map(({message}) => message).join(' '),
      };
    }
    return {valid: true};
  }
}
