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
const { ClientOptions } = require('google-gax');
const automl = require('@google-cloud/automl');
const google = automl.protos.google;

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
const { PredictionServiceClient } = automl[API_VERSION];

/**
 * AutoML Tables API class for:
 * 1. start a batch predict job:
 * @see https://cloud.google.com/nodejs/docs/reference/automl/latest/automl/v1beta1.predictionserviceclient#_google_cloud_automl_v1beta1_PredictionServiceClient_batchPredict_member_3_
 * 2. get the status of an operation:
 * @see https://cloud.google.com/nodejs/docs/reference/automl/latest/automl/v1beta1.predictionserviceclient#_google_cloud_automl_v1beta1_PredictionServiceClient_checkBatchPredictProgress_member_1_
 */
class AutoMl {
  /**
   * Initialize an instance.
   * @param {ClientOptions=} options
   */
  constructor(options = {}) {
    this.client = new PredictionServiceClient(options);
  }

  /**
   * Batch predicts based on Google Cloud Client Library.
   * @param {string} projectId
   * @param {string} computeRegion
   * @param {string} modelId
   * @param {google.cloud.automl.v1beta1.IBatchPredictInputConfig} inputConfig
   * @param {google.cloud.automl.v1beta1.IBatchPredictOutputConfig} outputConfig
   * @return {Promise<string>} Predict operation name.
   */
  async batchPredict(
    projectId,
    computeRegion,
    modelId,
    inputConfig,
    outputConfig
  ) {
    const modelFullId = this.client.modelPath(
      projectId,
      computeRegion,
      modelId
    );
    const responses = await this.client.batchPredict({
      name: modelFullId,
      inputConfig,
      outputConfig,
    });
    const operation = responses[1];
    console.log(`Operation name: ${operation.name}`);
    return operation.name;
  }

  /**
   * Gets status of an operation.
   * @param {string} operationName
   * @return {Promise<{{
   *   done: boolean,
   *   error: Error|undefined,
   *   metadata: OperationMetadata,
   *   name: string,
   * }}>}
   */
  async getOperation(operationName) {
    const response = await this.client.checkBatchPredictProgress(operationName);
    const { done, error, metadata, name } = response;
    return { done, error, metadata, name };
  }
}

exports.AutoMl = AutoMl;
