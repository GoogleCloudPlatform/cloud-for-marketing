// Copyright 2021 Google Inc.
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
 * @fileoverview Google Cloud Vertex AI API helper.
 */

'use strict';

const {JobServiceClient} = require('@google-cloud/aiplatform');

/**
 * Wrapper class for AI Platform (Vertex AI) API.
 * 1. Create or get batch prediction jobs.
 * @see https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.batchPredictionJobs#BatchPredictionJob
 */
class VertexAi {

  /**
   * Initialize an instance.
   * @param {{keyFilename:(string|undefined)}} options
   */
  constructor(options = {}) {
    this.options = options;
  }

  /**
   * Starts a BatchPredictionJob and returns the job name.
   * @see https://googleapis.dev/nodejs/aiplatform/latest/v1.JobServiceClient.html#createBatchPredictionJob
   * @see https://googleapis.dev/nodejs/aiplatform/latest/google.cloud.aiplatform.v1.BatchPredictionJob.html
   * @param {string} projectId
   * @param {string} location
   * @param {string} modelId
   * @param {google.cloud.aiplatform.v1.BatchPredictionJob.IInputConfig} inputConfig
   * @param {google.cloud.aiplatform.v1.BatchPredictionJob.IOutputConfig} outputConfig
   * @param {string=} displayName
   * @return {Promise<string>} BatchPredictionJob name.
   */
  async batchPredict(projectId, location, modelId, inputConfig,
      outputConfig, displayName = 'Batch Prediction Job') {
    const jobServiceClient = this.getJobServiceClient_(location);
    const parent = `projects/${projectId}/locations/${location}`;
    const model = jobServiceClient.modelPath(projectId, location, modelId);
    const batchPredictionJob = {
      displayName,
      model,
      inputConfig,
      outputConfig,
    };
    const request = {
      parent,
      batchPredictionJob,
    };
    const [response] = await jobServiceClient.createBatchPredictionJob(request);
    return response.name;
  }

  /**
   * Gets a BatchPredictionJob.
   * @see https://googleapis.dev/nodejs/aiplatform/latest/v1.JobServiceClient.html#getBatchPredictionJob
   * @param {string} jobName
   * @param {(string|undefined)=} explicitLocation Location of the service
   *     endpoint. A location will be extracted from jobName if omitted.
   * @return {Promise<google.cloud.aiplatform.v1.BatchPredictionJob>}
   */
  async getBatchPredictionJob(jobName, explicitLocation = undefined) {
    let location = explicitLocation;
    if (!location) {
      location = /projects\/[^\/]*\/locations\/([^\/]*)\/.*/.exec(jobName)[1];
    }
    const jobServiceClient = this.getJobServiceClient_(location);
    const request = {name: jobName};
    const [response] = await jobServiceClient.getBatchPredictionJob(request);
    return response;
  }

  /**
   * Gets the service endpoint for Vertex AI.
   * @see https://cloud.google.com/vertex-ai/docs/general/locations
   * @see https://cloud.google.com/vertex-ai/docs/reference/rest#service-endpoint
   * @param {string} location
   * @return {string} The service endpoint.
   * @private
   */
  getServiceEndpoint_(location) {
    return `${location}-aiplatform.googleapis.com`;
  }

  /**
   * Gets the service for creating and managing AI Platform's jobs.
   * @see https://googleapis.dev/nodejs/aiplatform/latest/v1.JobServiceClient.html
   * @param {string} location
   * @return {!JobServiceClient}
   * @private
   */
  getJobServiceClient_(location) {
    const clientOptions = {
      apiEndpoint: this.getServiceEndpoint_(location),
    };
    return new JobServiceClient(clientOptions);
  }
}

exports.VertexAi = VertexAi;
