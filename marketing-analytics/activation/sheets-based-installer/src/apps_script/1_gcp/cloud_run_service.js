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

/** @fileoverview Cloud Run Service API handler class. */

class CloudRunService extends GcpApiBase {
  /** @return {string} The canonical GCP asset type for this service. */
  static get ASSET_TYPE() {
    return 'run.googleapis.com/Service';
  }

  /**
   * @param {string=} projectId
   * @param {string=} locationId
   */
  constructor(projectId, locationId) {
    super();
    this.name = 'Cloud Run Admin API';
    this.api = 'run.googleapis.com';
    this.permissions = [
      'run.services.create',
      'run.services.get',
      'run.services.update',
      'run.routes.get',
    ];
    this.adminRole = 'Cloud Run Admin';
    this.runtimeRoles = ['roles/run.developer', 'roles/iam.serviceAccountUser'];
    this.apiUrl = `https://${this.api}`;
    this.version = 'v2';
    this.projectId = projectId;
    this.locationId = locationId;
  }

  /**
   * @override
   * @return {string}
   */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /**
   * Lists information about the supported locations for this service.
   * @see https://docs.cloud.google.com/run/docs/reference/rest/v1/projects.locations/list
   * @return {!Array<!Location>} An array of resources that represents Google
   *   Cloud Platform location.
   * @see https://docs.cloud.google.com/run/docs/reference/rest/v1/projects.locations/list#google.cloud.location.Locations.ListLocations
   */
  listLocations() {
    return super.listLocations('v1');
  }

  /**
   * Gets information about a Service.
   * @see https://docs.cloud.google.com/run/docs/reference/rest/v2/projects.locations.services/get
   * @param {string} service
   * @return {!Service}
   */
  getService(service) {
    return this.get(
      `projects/${this.projectId}/locations/${this.locationId}/services/${service}`,
    );
  }

  /**
   * Returns a complete list of services that belong to the requested project
   * and location, handling pagination automatically.
   * @see https://docs.cloud.google.com/run/docs/reference/rest/v2/projects.locations.services/list
   * @param {string=} locationId
   * @param {!Object=} parameters
   * @return {!Array<!Service>}
   */
  listServices(locationId = this.locationId, parameters = {}) {
    return this.list(
      'services',
      `projects/${this.projectId}/locations/${locationId}/services`,
      parameters,
    );
  }

  /**
   * Returns a list of services that belong to the requested project and
   * all locations.
   * @see https://docs.cloud.google.com/run/docs/reference/rest/v2/projects.locations.services/list
   * @return {{services:!Array<!Service>}}
   */
  listServicesForAllLocations() {
    return this.listServices('-');
  }

  /**
   * Returns whether a Cloud Run Service with the given name exists.
   * @param {string} serviceId
   * @return {boolean}
   */
  existService(serviceId) {
    const response = this.getService(serviceId);
    if (response.name) return true;
    if (response.error && response.error.code === 404) return false;
    throw new Error(`Unknown status of Cloud Run Service ${serviceId}`);
  }

  /**
   * Returns the primary endpoint HTTPS URL for a Cloud Run service by name.
   * Throws an error if the service or its URL cannot be resolved.
   * @param {string} service Name or ID of the Cloud Run service.
   * @return {string} The public HTTPS URL of the service.
   */
  getServiceUrl(service) {
    const response = this.getService(service);
    if (!response || response.error) {
      const errMsg =
        response && response.error ? response.error.message : 'Not found';
      throw new Error(`Cloud Run Service "${service}" not found: ${errMsg}`);
    }
    const url =
      response.uri ||
      (response.urls && response.urls[0]) ||
      (response.status && response.status.url);
    if (!url) {
      throw new Error(
        `URL for Cloud Run Service "${service}" could not be resolved.`,
      );
    }
    return url;
  }

  /**
   * Returns the Service Account email configured for a Cloud Run service.
   * If no custom service account was set, falls back to the project's
   * default Compute Engine service account.
   * Throws an error if the service cannot be found.
   * @param {string} service Name or ID of the Cloud Run service.
   * @return {string} Service Account email string.
   */
  getServiceAccount(service) {
    const response = this.getService(service);
    if (!response || response.error) {
      const errMsg =
        response && response.error ? response.error.message : 'Not found';
      throw new Error(`Cloud Run Service "${service}" not found: ${errMsg}`);
    }
    if (response.template && response.template.serviceAccount) {
      return response.template.serviceAccount;
    }
    console.log(
      'No serviceAccount found for Cloud Run Service',
      service,
      'Falling back to default Compute Engine service account...',
    );
    const resourceManager = new CloudResourceManager(this.projectId);
    return resourceManager.getDefaultComputeServiceAccount();
  }

  /**
   * Creates a new service.
   * @see https://docs.cloud.google.com/run/docs/reference/rest/v2/projects.locations.services/create
   * @param {string} serviceId
   * @param {!Service} payload
   * @return {!Operation}
   */
  createService(serviceId, payload) {
    console.log(
      'Creating Cloud Run Service',
      serviceId,
      JSON.stringify(payload),
    );
    return this.mutate(
      `projects/${this.projectId}/locations/${this.locationId}/services?serviceId=${serviceId}`,
      payload,
    );
  }

  /**
   * Updates an existing service.
   * @see https://docs.cloud.google.com/run/docs/reference/rest/v2/projects.locations.services/patch
   * @param {string} serviceName
   * @param {!Service} payload
   * @return {!Operation}
   */
  updateService(serviceName, payload) {
    return this.mutate(
      `projects/${this.projectId}/locations/${this.locationId}/services/${serviceName}`,
      payload,
      'PATCH',
    );
  }

  /**
   * Creates or updates the service.
   * @param {string} serviceId
   * @param {!Service} payload
   * @return {!Object} The response of the operation.
   */
  createOrUpdateService(serviceId, payload) {
    return this.waitAndResolveOperation(
      this.asyncCreateOrUpdateService(serviceId, payload),
      'Service',
    );
  }

  /**
   * Creates or updates the service. Returns the operation instantly and will
   * not wait for it to be done.
   * @param {string} serviceId
   * @param {!Service} payload
   * @return {!Object} The operation.
   */
  asyncCreateOrUpdateService(serviceId, payload) {
    const response = this.getService(serviceId);
    let operation;

    if (response.name) {
      operation = this.updateService(serviceId, payload);
    } else if (response.error && response.error.code === 404) {
      console.log('Not exist, now create Service', serviceId);
      operation = this.createService(serviceId, payload);
    } else {
      throw new Error(`Unknown status for Cloud Run Service ${serviceId}`);
    }
    return operation;
  }

  /**
   * Returns a link to the cloud run service in Google Cloud Console.
   * @param {string} serviceId
   * @return {string}
   */
  getLinkToCloudConsole(serviceId) {
    return `https://console.cloud.google.com/run/detail/${this.locationId}/${
      serviceId
    }?project=${this.projectId}`;
  }
}

CloudRunService.ATTRIBUTE_NAMES = Object.freeze({
  'template.containers.0.image': 'Docker Image',
  'template.containers.0.env': 'Environment Variables',
  'template.containers.0.resources.limits.cpu': 'CPU',
  'template.containers.0.resources.limits.memory': 'Memory(GiB)',
  'template.timeout': 'Task Timeout (s)',
  'template.serviceAccount': 'Service Account',
  'template.maxInstanceRequestConcurrency': 'Concurrency',
  ingress: 'Ingress',
});

CloudRunService.INGRESS_TRAFFIC = Object.freeze({
  INTERNAL: 'internal',
  ALL: 'all',
  INTERNAL_AND_CLOUD_LOAD_BALANCING: 'internal-and-cloud-load-balancing',
});

if (typeof ServiceProviderRegistry !== 'undefined') {
  ServiceProviderRegistry.register(CloudRunService);
}
