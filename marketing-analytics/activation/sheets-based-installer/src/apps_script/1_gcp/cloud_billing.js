// Copyright 2019 Google Inc.
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

/** @fileoverview Cloud Billing API handler class.*/

/**
 * This class is used to check the (GCP) project billing status.
 */
class CloudBilling extends ApiBase {

  constructor(projectId) {
    super();
    this.name = 'Cloud Billing API';
    this.api = 'cloudbilling.googleapis.com';
    this.apiUrl = `https://${this.api}`;
    this.version = 'v1';
    this.projectId = projectId;
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/projects/${this.projectId}`;
  }

  /**
   * Gets the billing information for a project.
   * @see https://cloud.google.com/billing/docs/reference/rest/v1/projects/getBillingInfo
   * @return {ProjectBillingInfo}
   * @see https://cloud.google.com/billing/docs/reference/rest/v1/ProjectBillingInfo
   */
  getBillingInfo() {
    return this.get('billingInfo');
  }

}
