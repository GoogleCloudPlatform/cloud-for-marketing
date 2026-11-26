// Copyright 2026 Google Inc.
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

/** @fileoverview Cloud Run Job API handler class. */

class CloudRunJob extends GcpApiBase {
  /** @return {string} The canonical GCP asset type for this service. */
  static get ASSET_TYPE() {
    return 'run.googleapis.com/Job';
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
      'run.jobs.create',
      'run.jobs.get',
      'run.jobs.update',
      'run.jobs.run',
    ];
    this.adminRole = 'Cloud Run Admin';
    this.runtimeRoles = ['roles/run.developer'];
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
   * Returns a complete list of jobs that belong to the requested project and
   * location, handling pagination automatically. Note: The Cloud Run REST API
   * requires a specific region for jobs (wildcard '-' is not supported).
   * @see https://cloud.google.com/run/docs/reference/rest/v2/projects.locations.jobs/list
   * @param {string=} locationId Specific GCP location ID (e.g. 'us-central1').
   * @param {!Object=} parameters
   * @return {!Array<!Job>}
   */
  listJobs(locationId = this.locationId, parameters = {}) {
    return this.list(
      'jobs',
      `projects/${this.projectId}/locations/${locationId}/jobs`,
      parameters,
    );
  }

  /**
   * Returns the Service Account email configured for a Cloud Run job.
   * If no custom service account was set, falls back to the project's
   * default Compute Engine service account.
   * Throws an error if the job cannot be found.
   * @param {string} jobId Name or ID of the Cloud Run job.
   * @return {string} Service Account email string.
   */
  getServiceAccount(jobId) {
    const response = this.getJob(jobId);
    if (!response || response.error) {
      const errMsg =
        response && response.error ? response.error.message : 'Not found';
      throw new Error(`Cloud Run Job "${jobId}" not found: ${errMsg}`);
    }
    if (
      response.template &&
      response.template.template &&
      response.template.template.serviceAccount
    ) {
      return response.template.template.serviceAccount;
    }
    console.log(
      'No serviceAccount found for Cloud Run Job',
      jobId,
      'Falling back to default Compute Engine service account...',
    );
    const resourceManager = new CloudResourceManager(this.projectId);
    return resourceManager.getDefaultComputeServiceAccount();
  }

  /**
   * Gets information about a Job.
   * @see https://cloud.google.com/run/docs/reference/rest/v2/projects.locations.jobs/get
   * @param {string} jobId
   * @return {!Job}
   */
  getJob(jobId) {
    return this.get(
      `projects/${this.projectId}/locations/${this.locationId}/jobs/${jobId}`,
    );
  }

  /**
   * Returns whether a Cloud Run Job with the given name exists.
   * @param {string} jobId
   * @return {boolean}
   */
  existJob(jobId) {
    const response = this.getJob(jobId);
    if (response.name) return true;
    if (response.error && response.error.code === 404) return false;
    throw new Error(`Unknown status of Cloud Run Job ${jobId}`);
  }

  /**
   * Creates a new job.
   * @see https://docs.cloud.google.com/run/docs/reference/rest/v2/projects.locations.jobs/create
   * @param {string} jobId
   * @param {!CloudFunction} payload
   * @return {!Operation}
   */
  createJob(jobId, payload) {
    console.log('Creating Cloud Run Job', jobId, JSON.stringify(payload));
    return this.mutate(
      `projects/${this.projectId}/locations/${this.locationId}/jobs?jobId=${
        jobId
      }`,
      payload,
    );
  }

  /**
   * Updates an existing job.
   * @see https://docs.cloud.google.com/run/docs/reference/rest/v2/projects.locations.jobs/patch
   * @param {string} jobName
   * @param {!CloudFunction} payload
   * @return {!Operation}
   */
  updateJob(jobName, payload) {
    return this.mutate(
      `projects/${this.projectId}/locations/${this.locationId}/jobs/${jobName}`,
      payload,
      'PATCH',
    );
  }

  /**
   * Creates or updates the job.
   * @param {string} jobId
   * @param {!CloudFunction} payload
   * @return {string|!Object} The response of operation.
   */
  createOrUpdateJob(jobId, payload) {
    return this.waitAndResolveOperation(
      this.asyncCreateOrUpdateJob(jobId, payload),
      'Job',
    );
  }

  /**
   * Creates or updates the job. Returns the operation instantly and will
   * not wait for it to be done.
   * @param {string} jobId
   * @param {!CloudFunction} payload
   * @return {!Object} The operation.
   */
  asyncCreateOrUpdateJob(jobId, payload) {
    const response = this.getJob(jobId);
    let operation;

    if (response.name) {
      operation = this.updateJob(jobId, payload);
    } else if (response.error && response.error.code === 404) {
      console.log('Not exist, now create Job', jobId);
      operation = this.createJob(jobId, payload);
    } else {
      throw new Error(`Unknown status for Cloud Run Job ${jobId}`);
    }

    return operation;
  }

  /**
   * Gets information about a Cloud Run Job execution.
   * @see <https://cloud.google.com/run/docs/reference/rest/v2/projects.locations.jobs.executions/get>
   * @param {string} executionName
   * @return {!Execution}
   */
  getJobExecution(executionName) {
    return this.get(executionName);
  }

  /**
   * Triggers the execution of a Cloud Run Job with the given arguments.
   * @see <https://cloud.google.com/run/docs/reference/rest/v2/projects.locations.jobs/run>
   * @param {string} jobName
   * @param {!Object|!Array} args
   * @return {string} The name of the execution.
   */
  runJob(jobName, args) {
    const operation = this.mutate(`${jobName}:run`, {
      overrides: {
        containerOverrides: [
          {
            args: [JSON.stringify(args)],
          },
        ],
      },
    });
    console.log('Triggered Cloud Run Job', jobName, args, operation);
    return operation.metadata.name;
  }

  /**
   * Returns a link to the cloud run job in Google Cloud Console.
   * @param {string} jobId
   * @return {string}
   */
  getLinkToCloudConsole(jobId) {
    return `https://console.cloud.google.com/run/jobs/details/${
      this.locationId
    }/${jobId}?project=${this.projectId}`;
  }
}

CloudRunJob.ATTRIBUTE_NAMES = Object.freeze({
  'template.template.containers.0.image': 'Docker Image',
  'template.template.containers.0.env': 'Environment Variables',
  'template.template.containers.0.resources.limits.cpu': 'CPU',
  'template.template.containers.0.resources.limits.memory': 'Memory(GiB)',
  'template.template.timeout': 'Task Timeout (s)',
  'template.template.serviceAccount': 'Service Account',
  'template.template.maxRetries': 'Max Retries',
  'template.parallelism': 'Parallelism',
});

if (typeof ServiceProviderRegistry !== 'undefined') {
  ServiceProviderRegistry.register(CloudRunJob);
}
