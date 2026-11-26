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

/**
 * @fileoverview Cloud Scheduler wrapper class, including: pause/resume a job.
 */

'use strict';
const {CloudSchedulerClient} = require('@google-cloud/scheduler');

/**
 * Cloud Scheduler REST API v1 wrapper. This class offers convenient functions
 * to get/pause/resume a job.
 */
class CloudScheduler {
  /**
   * @constructor
   */
  constructor(options = {}) {
    const clientOptions = Object.assign({}, options);
    if (!clientOptions.projectId) {
      clientOptions.projectId = process.env['GCP_PROJECT'];
    }
    this.client = new CloudSchedulerClient(clientOptions);
    this.projectId = clientOptions.projectId;
  }

  /**
   * Gets the GCP project Id. In Cloud Functions, it *should* be passed in
   * through environment variable during the deployment. But if it doesn't exist
   * (for example, in local unit tests), this function will fallback to ADC
   * (Application Default Credential) auth's asynchronous function to get the
   * project Id.
   * @return {string}
   * @private
   */
  async getProjectId_() {
    if (!this.projectId) this.projectId = await this.client.getProjectId();
    return this.projectId;
  }

  /**
   * Pauses the jobs with the given name and locations. If the locations
   * are not specified given, it pauses jobs with the name in all locations.
   * For more information, see:
   * https://cloud.google.com/scheduler/docs/reference/rest/v1/projects.locations.jobs/pause
   *
   * @param {string} name Job name, with no project id or locations prefix.
   * @param {(string|Array<string>)=} targetLocations Locations of the
   *     scheduler job.
   * @return {!Promise<boolean>} Whether the job is paused.
   */
  async pauseJob(name, targetLocations) {
    const jobs = await this.getJobs_(name, targetLocations);
    if (jobs.length === 0) return false;
    try {
      const results = await Promise.all(
        jobs.map(job => this.client.pauseJob({name: job})),
      );
      return results.every(([job]) => job.state === 'PAUSED');
    } catch (error) {
      console.error('Fail to pause Scheduler Job', error);
      return false;
    }
  }

  /**
   * Resumes the jobs with the given name and locations. If the locations
   * are not specified given, it resumes jobs with the name in all locations.
   * For more information, see:
   * https://cloud.google.com/scheduler/docs/reference/rest/v1/projects.locations.jobs/resume
   *
   * @param {string} name Job name, with no project id or locations prefix.
   * @param {(string|Array<string>)=} targetLocations Locations of the
   *     scheduler job.
   * @return {!Promise<boolean>} Whether the job is resumed.
   */
  async resumeJob(name, targetLocations) {
    const jobs = await this.getJobs_(name, targetLocations);
    if (jobs.length === 0) return false;
    try {
      const results = await Promise.all(
        jobs.map(job => this.client.resumeJob({name: job})),
      );
      return results.every(([job]) => job.state === 'ENABLED');
    } catch (error) {
      console.error('Fail to resume Scheduler Job', error);
      return false;
    }
  }

  /**
   * Returns the job list with the given name and locations. If the locations
   * are not specified given, it returns jobs with the name in all locations.
   * For more information, see:
   * https://cloud.google.com/scheduler/docs/reference/rest/v1/projects.locations.jobs/get
   *
   * @param {string} name Job name, with no project id or locations prefix.
   * @param {(string|Array<string>)=} targetLocations
   * @return {!Promise<Array<string>>} The array of jobs name.
   * @private
   */
  async getJobs_(name, targetLocations) {
    const jobName = `/jobs/${name}`;
    const allJobs = await this.listJobs_(targetLocations);
    const jobs = allJobs.filter(job => job.endsWith(jobName));
    if (jobs.length === 0) console.error(`Can not find job: ${name}`);
    return jobs;
  }

  /**
   * Returns the job list for given locations. If the locations are not
   * specified, it returns jobs in all locations.
   * For more information, see:
   * https://cloud.google.com/scheduler/docs/reference/rest/v1/projects.locations.jobs/list
   *
   * @param {(string|Array<string>)=} targetLocations
   * @return {!Promise<Array<string>>} The array of jobs name.
   * @private
   */
  async listJobs_(targetLocations) {
    const locations = await this.getTargetLocations_(targetLocations);
    const projectId = await this.getProjectId_();
    const jobs = locations.map(async location => {
      const parent = this.client.locationPath(projectId, location);
      try {
        const [jobs] = await this.client.listJobs({parent});
        return jobs.map(job => job.name);
      } catch (error) {
        console.warn(`Failed to list jobs for ${location}`, error);
        return [];
      }
    });
    return (await Promise.all(jobs)).flat();
  }

  /**
   * Returns the current locations for Cloud Scheduler.
   * For more information, see:
   * https://cloud.google.com/scheduler/docs/reference/rest/v1/projects.locations/list
   * @return {!Promise<Array<string>>} The array of location ids.
   * @private
   */
  async listLocations_() {
    const locationIds = [];
    const request = {name: `projects/${await this.getProjectId_()}`};
    for await (const location of this.client.listLocationsAsync(request)) {
      locationIds.push(location.locationId);
    }
    return locationIds;
  }

  /**
   * Returns the locations based on a given location or an array of locations.
   * If there is no given location, then returns all available locations.
   * @param {(string|Array<string>)=} targetLocations
   * @return {!Promise<Array<!string>>|!Array<string>}
   * @private
   */
  async getTargetLocations_(targetLocations) {
    if (!targetLocations) return this.listLocations_();
    return typeof targetLocations === 'string'
      ? [targetLocations]
      : targetLocations;
  }
}

exports.CloudScheduler = CloudScheduler;
