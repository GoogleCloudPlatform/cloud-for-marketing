// Copyright 2021 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this fileAccessObject except in compliance with the License.
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
 * @fileoverview Interface for Vertex AI API.
 */

'use strict';

const {vertexai: {VertexAi}} = require('@google-cloud/nodejs-common');
const {AutoMlPredict} = require('./automl_predict.js');

/** Vertex AI batch predict. */
class VertexPredict extends AutoMlPredict {

  /** @constructor */
  constructor(service = new VertexAi()) {
    super();
    this.service = service;
  }

  /** @override */
  async batchPredict(config) {
    const {model, source, destination} = config;
    const inputUri = await this.getInputUri(source);
    const outputUri = this.getOutputUri(destination);
    const jobName = await this.service.batchPredict(
        model.projectId,
        model.location,
        model.modelId,
        inputUri,
        outputUri,
        model.displayName);
    return jobName;
  }

  /** @override */
  async isPredictDone(jobName) {
    const result = await this.getPredictJob(jobName);
    return result.state === 'JOB_STATE_SUCCEEDED' || !!result.error;
  }

  /** @override */
  async getPredictJob(jobName) {
    return this.service.getBatchPredictionJob(jobName);
  }

  /** @override */
  getOutputInfo(predictionJob) {
    return predictionJob.outputInfo;
  }

  /** @override */
  async getInputUri(options) {
    const inputUri = await super.getInputUri(options);
    inputUri.instancesFormat = this.getFormat_(options);
    return inputUri;
  }

  /** @override */
  getOutputUri(options) {
    const outputUri = super.getOutputUri(options);
    outputUri.predictionsFormat = this.getFormat_(options);
    return outputUri;
  }

  /**
   * Gets the format for inputUri and outputUri of a batch prediction.
   * @see https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.models#Model.FIELDS.supported_input_storage_formats
   * @see https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.models#Model.FIELDS.supported_output_storage_formats
   * @param options
   * @return {string|*}
   * @private
   */
  getFormat_(options) {
    return options.table ? 'bigquery' : options.format;
  }

  /**
   * @override
   * @see https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.batchPredictionJobs#OutputInfo
   * @param {{
   *   bigqueryOutputDataset:string,
   *   bigqueryOutputTable:string,
   *   }|{
   *   gcsOutputDirectory:string,
   * }} output
   * @return {{
   *   projectId: string,
   *   datasetId: string,
   *   tableId: string,
   *   }|{
   *   bucket: string,
   *   name: string,
   * }}
   */
  getOutputObject(output) {
    const result = super.getOutputObject(output);
    if (output.bigqueryOutputTable) {
      result.tableId = output.bigqueryOutputTable;
    }
    return result;
  }
}

module.exports = {VertexPredict};
