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

/** @fileoverview Cloud Service Usage API handler class.*/

class ServiceUsage extends GcpApiBase {
  /**
   * @param {string=} projectId
   */
  constructor(projectId) {
    super();
    this.name = 'Service Usage API';
    this.api = 'serviceusage.googleapis.com';
    this.permissions = [
      'serviceusage.services.enable',
      'serviceusage.services.get',
    ];
    this.adminRole = 'Service Usage Admin';
    this.runtimeRoles = [];
    this.apiUrl = `https://${this.api}`;
    this.version = 'v1';
    this.projectId = projectId;
  }

  /**
   * @override
   * @return {string}
   */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/projects/${this.projectId}/services`;
  }

  /**
   * Returns the service configuration and enabled state for a given service.
   * @see https://cloud.google.com/service-usage/docs/reference/rest/v1/services/get
   * @param {string} service
   * @return {!Service}
   * @see https://cloud.google.com/service-usage/docs/reference/rest/v1/services#Service
   */
  getService(service) {
    return this.get(service);
  }

  /**
   * Enables a service so that it can be used with a project and waits until
   * the operation completes using the inherited waitAndResolveOperation.
   * @see https://cloud.google.com/service-usage/docs/reference/rest/v1/services/enable
   * @param {string} service
   * @return {!Object}
   */
  enableService(service) {
    const operation = this.mutate(`${service}:enable`);
    return this.waitAndResolveOperation(operation, `Service ${service}`);
  }

  /**
   * Checks the state for a given service and enables it if it is disabled.
   * @param {string} service
   * @return {boolean} Whether the service is enabled.
   */
  checkOrEnableService(service) {
    const {state} = this.getService(service);
    if (state === 'ENABLED') return true;

    const res = this.enableService(service);
    if (res && res.error) {
      console.error('Failed to enable service:', service, res.error);
      return false;
    }
    return (res && res.service && res.service.state === 'ENABLED') || true;
  }
  /**
   * Returns a link to the API page in Google Cloud Console.
   * @param {string} service
   * @return {string}
   */
  getLinkToCloudConsole(service) {
    const link = `https://console.cloud.google.com/apis/library/${
      service
    }?project=${this.projectId}`;
    return link;
  }
}
