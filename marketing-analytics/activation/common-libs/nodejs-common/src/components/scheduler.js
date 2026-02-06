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
const {google: {cloudscheduler}} = require('googleapis');
const AuthClient = require('../apis/auth_client.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/cloud-platform',
]);
const API_VERSION = 'v1';

/**
 * Cloud Scheduler REST API v1 wrapper. This class offers convenient functions
 * to get/pause/resume a job.
 */
class CloudScheduler {
  constructor(env = process.env, options = {}) {
    if (!options.authClient) {
      /** @const {!AuthClient} */
      const authClient = new AuthClient(API_SCOPES, env);
      /**
       * By default, `AuthClient` (getDefaultAuth()) will return an auth client
       * based on the settings in ENV while the OAuth is the most preferred.
       * This works for most of the external API clients (in the '../apis'
       * folder), however this won't work in the Cloud Functions, as those OAuth
       * token usually won't have enough permission to invoke Google Cloud API.
       * Using the method `getApplicationDefaultCredentials` to force
       * `AuthClient` return an ADC auth client, which will work in the Cloud.
       *
       * Cloud Scheduler API Client Library is used here as Cloud Client Library
       * is still at beta stage. (For the difference, see
       * https://cloud.google.com/apis/docs/client-libraries-explained)
       * Eventually, when we migrate this to the cloud client library, which
       * automatically takes ADC as the authentication method, the 'AuthClient'
       * is not required here and can be removed.
       */
      this.auth = authClient.getApplicationDefaultCredentials();
    } else {
      /**
       * `authClient` can be consumed by cloud client library as the auth
       * client. By passing this in, we can offer more flexible auth clients in
       * test cases for API client library and cloud client library in future.
       */
      this.auth = options.authClient;
    }
    this.projectId = env['GCP_PROJECT'];
    this.instance = cloudscheduler({
      version: API_VERSION,
      auth: this.auth,
    });
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
    if (!this.projectId) this.projectId = await this.auth.getProjectId();
    return this.projectId;
  }

  /**
   * Pauses the jobs with the given name and locations. If the locations
   * are not specified given, it pauses jobs with the name in all locations.
   * For more information, see:
   * https://cloud.google.com/scheduler/docs/reference/rest/v1/projects.locations.jobs/pause
   *
   * @param {string} name Job name, with no project id or locations prefix.
   * @param {(string|Array<string>|undefined)=} targetLocations Locations of the
   *     scheduler job.
   * @return {boolean} Whether the job is paused.
   */
  async pauseJob(name, targetLocations = undefined) {
    const jobs = await this.getJobs_(name, targetLocations);
    if (jobs.length === 0) return false;
    const results = await Promise.all(jobs.map(
        (job) => this.instance.projects.locations.jobs.pause({name: job})
    ));
    return results.every((response) => response.data.state === 'PAUSED');
  }

  /**
   * Resumes the jobs with the given name and locations. If the locations
   * are not specified given, it resumes jobs with the name in all locations.
   * For more information, see:
   * https://cloud.google.com/scheduler/docs/reference/rest/v1/projects.locations.jobs/resume
   *
   * @param {string} name Job name, with no project id or locations prefix.
   * @param {(string|Array<string>|undefined)=} targetLocations Locations of the
   *     scheduler job.
   * @return {boolean} Whether the job is resumed.
   */
  async resumeJob(name, targetLocations = undefined) {
    const jobs = await this.getJobs_(name, targetLocations);
    if (jobs.length === 0) return false;
    try {
      const results = await Promise.all(jobs.map(
          (job) => this.instance.projects.locations.jobs.resume({name: job})
      ));
      return results.every((response) => response.data.state === 'ENABLED');
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
   * @param {(string|Array<string>|undefined)=} targetLocations
   * @return {Array<string>} The array of jobs name.
   * @private
   */
  async getJobs_(name, targetLocations = undefined) {
    const jobName = `/jobs/${name}`;
    const allJobs = await this.listJobs_(targetLocations);
    const jobs = allJobs.filter((job) => job.endsWith(jobName));
    if (jobs.length === 0) console.error(`Can not find job: ${name}`);
    return jobs;
  }

  /**
   * Returns the job list for given locations. If the locations are not
   * specified, it returns jobs in all locations.
   * For more information, see:
   * https://cloud.google.com/scheduler/docs/reference/rest/v1/projects.locations.jobs/list
   *
   * @param {(string|Array<string>|undefined)=} targetLocations
   * @return {Array<string>} The array of jobs name.
   * @private
   */
  async listJobs_(targetLocations = undefined) {
    const locations = await this.getTargetLocations_(targetLocations);
    const projectId = await this.getProjectId_();
    const requestPrefix = `projects/${projectId}/locations`;
    const jobs = locations.map(async (location) => {
      const request = { parent: `${requestPrefix}/${location}` };
      try {
        const response =
          await this.instance.projects.locations.jobs.list(request);
        if (!response.data.jobs) return [];
        return response.data.jobs.map((job) => job.name);
      } catch (error) {
        console.warn(`Failed to list jobs for ${location}`, error);
        return [];
      }
    });
    // Waits for all jobs names and flattens nested job name arrays, however
    // there is no 'flat' available in current Cloud Functions runtime.
    return [].concat(...(await Promise.all(jobs)));
  }

  /**
   * Returns the current locations for Cloud Scheduler.
   * For more information, see:
   * https://cloud.google.com/scheduler/docs/reference/rest/v1/projects.locations/list
   * @return {Array<string>} The array of location ids.
   * @private
   */
  async listLocations_() {
    const projectId = await this.getProjectId_();
    const request = {name: `projects/${projectId}`};
    const response = await this.instance.projects.locations.list(request);
    return response.data.locations.map((location) => location.locationId);
  }

  /**
   * Returns the locations based on a given location or an array of locations.
   * If there is no given location, then returns all available locations.
   * @param {(string|Array<string>|undefined)=} targetLocations
   * @return {!Promise<Array<!string>>|!Array<string>}
   * @private
   */
  async getTargetLocations_(targetLocations = undefined) {
    if (!targetLocations) return this.listLocations_();
    return typeof targetLocations === 'string'
        ? [targetLocations]
        : targetLocations;
  }
}

exports.CloudScheduler = CloudScheduler;
