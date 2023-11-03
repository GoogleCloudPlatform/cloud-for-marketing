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

/** @fileoverview Cloud Scheduler API handler class.*/

class CloudScheduler extends ApiBase {

  constructor(projectId, locationId) {
    super();
    this.apiUrl = 'https://cloudscheduler.googleapis.com';
    this.version = 'v1';
    this.projectId = projectId;
    this.locationId = locationId;
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/projects/${this.projectId}`;
  }

  /**
   * Lists information about the supported locations for this service.
   * @see https://cloud.google.com/scheduler/docs/reference/rest/v1/projects.locations/list
   * @return {!ListLocationsResponse}
   * @see https://cloud.google.com/scheduler/docs/reference/rest/Shared.Types/ListLocationsResponse
   */
  listLocations() {
    return this.get('locations');
  }

  /**
   * Gets a job.
   * @see https://cloud.google.com/scheduler/docs/reference/rest/v1/projects.locations.jobs/get
   * @param {string} jobId
   * @return {!Job}
   * @see https://cloud.google.com/scheduler/docs/reference/rest/v1/projects.locations.jobs#Job
   */
  getJob(jobId) {
    return this.get(`locations/${this.locationId}/jobs/${jobId}`);
  }

  /**
   * Creates a job.
   * @see https://cloud.google.com/scheduler/docs/reference/rest/v1/projects.locations.jobs/create
   * @param {string} jobId
   * @param {!Job} options
   * @return {!Job}
   */
  createJob(jobId, options) {
    const name = `projects/${this.projectId}/locations/${this.locationId}/jobs/${jobId}`;
    const payload = Object.assign({ name }, options);
    return this.mutate(`locations/${this.locationId}/jobs`, payload);
  }

  /**
   * Updates a job. If successful, the updated job is returned. If the job does
   * not exist, NOT_FOUND is returned.
   * @see https://cloud.google.com/scheduler/docs/reference/rest/v1/projects.locations.jobs/patch
   * @param {string} jobId
   * @param {!Job} options
   * @return {!Job}
   */
  updateJob(jobId, options) {
    const name = `projects/${this.projectId}/locations/${this.locationId}/jobs/${jobId}`;
    const payload = Object.assign({ name }, options);
    const updateMask = encodeURIComponent(Object.keys(options).join(','));
    return this.mutate(
      `locations/${this.locationId}/jobs/${jobId}?updateMask=${updateMask}`,
      payload, 'PATCH');
  }

  /**
   * Creates or updates a job.
   * @param {string} jobId
   * @param {!Job} options
   * @return {!Job}
   */
  createOrUpdateJob(jobId, options) {
    const current = this.getJob(jobId);
    if (current.error) {
      if (current.error.status === 'NOT_FOUND') {
        return this.createJob(jobId, options);
      }
      throw new Error(`Unknown Scheduler job status: ${current.error.message}`);
    }
    return this.updateJob(jobId, options);
  }

  /**
   * Forces a job to run now.
   * @see https://cloud.google.com/scheduler/docs/reference/rest/v1/projects.locations.jobs/run
   * @param {string} jobId
   * @return {!Job}
   */
  runJob(jobId) {
    return this.mutate(`locations/${this.locationId}/jobs/${jobId}:run`);
  }

}
