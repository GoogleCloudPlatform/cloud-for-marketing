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
 * @fileoverview Predict task class.
 */

'use strict';

const {BaseTask} = require('./base_task.js');
const {PREDICTION_SERVICE} = require('./predict/base_predict.js');
const {AutoMlPredict} = require('./predict/automl_predict.js');
const {VertexPredict} = require('./predict/vertex_predict.js');

/** @const {string} The prediction job identity name. */
const JOB_ID_PARAMETER = 'operationName';

/**
 * Creates a batch prediction job and get the output information (BigQuery or
 * Cloud Storage). This task supports different external prediction services,
 * including AutoML Tables API, Vertex AI batch prediction.
 */
class PredictTask extends BaseTask {

  /** @constructor */
  constructor(config, parameters, defaultProject = undefined) {
    super(config, parameters, defaultProject);
    this.predictService = this.getPredictService_(config.model.api);
  }

  /** @override */
  isManualAsynchronous() {
    return true;
  }

  /** @override */
  async doTask() {
    const jobName = await this.predictService.batchPredict(this.config);
    return {
      parameters: this.appendParameter({[JOB_ID_PARAMETER]: jobName}),
    };
  }

  /** @override */
  async isDone() {
    const jobName = this.parameters[JOB_ID_PARAMETER];
    return this.predictService.isPredictDone(jobName);
  }

  /** @override */
  async completeTask() {
    const jobName = this.parameters[JOB_ID_PARAMETER];
    const predictOutput = await this.predictService.getPredictOutput(jobName);
    return {
      parameters: this.appendParameter({predictOutput}),
    };
  }

  /**
   * Gets the corresponding prediction service based on config.
   * @param {PREDICTION_SERVICE=} api, default is AutoMl Tables API
   * @return {VertexPredict|AutoMlPredict}
   * @private
   */
  getPredictService_(api = PREDICTION_SERVICE.AUTOML_TABLES) {
    switch (api) {
      case PREDICTION_SERVICE.AUTOML_TABLES:
        return new AutoMlPredict();
      case PREDICTION_SERVICE.VERTEX_AI:
        return new VertexPredict();
      default:
        throw new Error(`Unknown predict Api: ${api}`);
    }
  }
}

module.exports = {
  PredictTask,
};
