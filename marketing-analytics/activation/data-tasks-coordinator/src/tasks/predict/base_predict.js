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
 * @fileoverview Interface for a ML batch prediction task.
 */

'use strict';
const {
  TaskType,
  BigQueryTableConfig,
  StorageFileConfig,
} = require('../../task_config/task_config_dao.js');

/**
 * Types of prediction services.
 * @enum {string}
 */
const PREDICTION_SERVICE = Object.freeze({
  AUTOML_TABLES: 'automl',
  VERTEX_AI: 'vertex',
});

/**
 * @typedef {{
 *   type:TaskType.PREDICT,
 *   model:{
 *     api:(PREDICTION_SERVICE|undefined),
 *     projectId:string,
 *     location:string,
 *     modelId:string,
 *     displayName:(string|undefined),
 *   },
 *   source:{
 *     format:(string|undefined),
 *     table:(!BigQueryTableConfig|undefined),
 *     file:(!StorageFileConfig|undefined),
 *   },
 *   destination:{
 *     format:(string|undefined),
 *     table:!BigQueryTableConfig,
 *     file:(!StorageFileConfig|undefined),
 *   },
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let PredictTaskConfig;

/**
 * The base class for a predict service what will be used in PredictTask to
 * do batch prediction.
 * @abstract
 */
class Predict {

  /**
   * Starts a BatchPredictionJob and returns the job name.
   * @param {ReportConfig} config
   * @return {string} Batch prediction job name
   */
  batchPredict(config) {}

  /**
   * Returns whether the given prediction job is completed.
   * @param {string} jobName
   * @return {boolean}
   */
  isPredictDone(jobName) {}

  /**
   * Returns the output results of a predict job.
   * @param {string} jobName
   * @return {{
   *   bucket: string,
   *   name: string,
   *   }|{
   *   projectId: string,
   *   datasetId: string,
   *   tableId: string
   * }}
   */
  getPredictOutput(jobName) {}

}

module.exports = {
  PREDICTION_SERVICE,
  Predict,
  PredictTaskConfig,
};
