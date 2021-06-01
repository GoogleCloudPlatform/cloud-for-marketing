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
 * @fileoverview Google Cloud AutoML helper.
 */

'use strict';
const {request} = require('gaxios');
const {JWT, Compute} = require('google-auth-library');
const automl = require('@google-cloud/automl');
const google = automl.protos.google;
const AuthClient = require('../apis/auth_client.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/cloud-platform',
]);

/**
 * For version `v1beta1`, BigQuery can be the source and destination.
 * https://googleapis.dev/nodejs/automl/latest/google.cloud.automl.v1beta1.IBatchPredictInputConfig.html
 * https://googleapis.dev/nodejs/automl/latest/google.cloud.automl.v1beta1.IBatchPredictOutputConfig.html
 *
 * For version `v1`, currently only Cloud Storage are allowed.
 * https://googleapis.dev/nodejs/automl/latest/google.cloud.automl.v1.IBatchPredictInputConfig.html
 * https://googleapis.dev/nodejs/automl/latest/google.cloud.automl.v1.IBatchPredictOutputConfig.html
 *
 * NOTE: currently use `v1beta1`
 * @type {string}
 */
const API_VERSION = 'v1beta1';
const PredictionServiceClient = automl[API_VERSION].PredictionServiceClient;

/**
 * AutoML Tables API doesn't have fully cloud client library support yet. This
 * class acts as the single client class for different API requests of AutoML
 * Tables API, including:
 * 1. 'batch predict' based on Google Cloud Client Library,
 * https://googleapis.dev/nodejs/automl/latest/index.html
 * 2. 'get Operations' based on REST API, see:
 * https://cloud.google.com/automl/docs/reference/rest/v1/projects.locations.operations/get
 * https://cloud.google.com/automl-tables/docs/long-operations#get-operation
 */
class AutoMl {

  /**
   * Initialize an instance.
   * @param {{keyFilename:(string|undefined)}} options
   */
  constructor(options = {}) {
    this.options = options;
  }

  /**
   * Batch predicts based on Google Cloud Client Library.
   * See https://googleapis.dev/nodejs/automl/latest/index.html
   * @param {string} projectId
   * @param {string} computeRegion
   * @param {string} modelId
   * @param {google.cloud.automl.v1.IBatchPredictInputConfig} inputConfig
   * @param {google.cloud.automl.v1.IBatchPredictOutputConfig} outputConfig
   * @return {Promise<string>} Predict operation name.
   */
  async batchPredict(projectId, computeRegion, modelId, inputConfig,
      outputConfig) {
    const client = new PredictionServiceClient(this.options);
    const modelFullId = client.modelPath(projectId, computeRegion, modelId);
    const responses = await client.batchPredict({
      name: modelFullId,
      inputConfig: inputConfig,
      outputConfig: outputConfig,
    });
    const operation = responses[1];
    console.log(`Operation name: ${operation.name}`);
    return operation.name;
  }

  /**
   * Gets Operations based on REST API, see:
   * https://cloud.google.com/automl/docs/reference/rest/v1/projects.locations.operations/get
   * https://cloud.google.com/automl-tables/docs/long-operations#get-operation
   * @param {string} operationName
   * @return {Promise<google.longrunning.Operation>}
   */
  async getOperation(operationName) {
    const url = `https://automl.googleapis.com/${API_VERSION}/${operationName}`;
    const headers = await this.getAuthClient_().getRequestHeaders();
    const requestOptions = {
      method: 'GET',
      headers,
      url,
    };
    const response = await request(requestOptions);
    return response.data;
  }

  /**
   * Gets authentication client of AutoML API.
   * This API belongs to GCP, however it is not fully supported by Google Cloud
   * Client Library. So we need to manage some functions on its REST API,
   * in the way like we invoke other external APIs.
   * @returns {(!JWT|!Compute)}
   * @private
   */
  getAuthClient_() {
    /** @const {!AuthClient} */ const authClient = new AuthClient(API_SCOPES);
    return authClient.getApplicationDefaultCredentials();
  }

}

exports.AutoMl = AutoMl;
