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

/** @fileoverview Search Ads API handler class.*/

class SearchAds extends ExternalApi {
  constructor(option) {
    super(option);
    this.name = 'Search Ads 360 API';
    this.api = 'doubleclicksearch.googleapis.com';
    this.apiUrl = 'https://www.googleapis.com/doubleclicksearch';
    this.version = 'v2';
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
   * Verifies floodlight activities by updating the availabilities.
   * @see https://developers.google.com/search-ads/v2/reference/conversion/updateAvailability
   * @param {Array<>} availabilities
   * @return {VerifyResult}
   */
  verifyFloodlightActivities(availabilities) {
    const availabilityTimestamp = Date.now();
    availabilities.forEach(activity => {
      activity.availabilityTimestamp = availabilityTimestamp;
    });
    const {error} = this.mutate('conversion/updateAvailability', {
      availabilities,
    });
    if (error) {
      return {
        valid: false,
        reason: error.message || error.errors[0].message,
      };
    }
    return {valid: true};
  }
}
