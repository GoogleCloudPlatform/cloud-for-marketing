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

/** @fileoverview Campaign Manager 360 API handler class.*/

class CampaignManager extends ExternalApi {

  constructor(option) {
    super(option);
    this.apiUrl = 'https://dfareporting.googleapis.com/dfareporting';
    this.version = 'v4';
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /** @override */
  getScope() {
    return [
      'https://www.googleapis.com/auth/ddmconversions',
      'https://www.googleapis.com/auth/dfareporting',
      'https://www.googleapis.com/auth/dfatrafficking',
    ];
  }

  /**
   * Gets the user profile Id for the given account.
   * @see https://developers.google.com/doubleclick-advertisers/rest/v4/userProfiles/list
   * @param {string} targetAccountId
   * @return {string}
   */
  getUserProfileId(targetAccountId) {
    const { items } = this.get('userprofiles');
    if (!items) return;
    const userProfiles =
      items.filter(({ accountId }) => accountId === targetAccountId);
    if (userProfiles.length === 0) return;
    return userProfiles[0].profileId;
  }

  /**
   * Verifies the existence of the given floodlight activity.
   * @see https://developers.google.com/doubleclick-advertisers/rest/v4/floodlightActivities/get
   * @param {string} accountId
   * @param {string} floodlightActivityId
   * @return {VerifyResult}
   */
  verifyFloodlightActivity(accountId, floodlightActivityId) {
    const profileId = this.getUserProfileId(accountId);
    if (!profileId) {
      return {
        valid: false,
        reason: `Can not find user profile for CM account ID ${accountId}`,
      };
    }
    const { error } = this.get(
      `userprofiles/${profileId}/floodlightActivities/${floodlightActivityId}`);
    if (error) {
      return {
        valid: false,
        reason: error.errors[0].message,
      };
    }
    return { valid: true };
  }

  /**
   * Verifies the existence of the report and the report is exported as CSV.
   * @see https://developers.google.com/doubleclick-advertisers/rest/v4/reports/get
   * @param {string} accountId
   * @param {string} reportId
   * @return {VerifyResult}
   */
  verifyReport(accountId, reportId) {
    const profileId = this.getUserProfileId(accountId);
    if (!profileId) {
      return {
        valid: false,
        reason: `Can not find user profile for CM account ID ${accountId}`,
      };
    }
    const { error, format } =
      this.get(`userprofiles/${profileId}/reports/${reportId}`);
    if (error) {
      return {
        valid: false,
        reason: error.message,
      };
    }
    if (format !== 'CSV') {
      return {
        valid: false,
        reason: `Report[${reportId}]'s format is not CSV`,
      };
    }
    return { valid: true };
  }
}
