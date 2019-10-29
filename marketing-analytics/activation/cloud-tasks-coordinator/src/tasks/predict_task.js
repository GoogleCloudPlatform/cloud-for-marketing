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
 * @fileoverview Task base class.
 */

'use strict';

const {StorageUtils} = require('nodejs-common');
const {AutoMlService} = require('../automl_service');
const {BaseTask} = require('./base_task.js');
const {TaskType, BigQueryTableConfig} = require(
    '../dao/task_config_dao.js');
/**
 * @typedef {{
 *   type:TaskType.PREDICT,
 *   model:{
 *     projectId:string,
 *     location:string,
 *     modelId:string,
 *     modelName:string
 *   },
 *   source:{
 *     table:!BigQueryTableConfig,
 *   },
 *   destination:{
 *     table:!BigQueryTableConfig,
 *   },
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let PredictTaskConfig;

exports.PredictTaskConfig = PredictTaskConfig;

class PredictTask extends BaseTask {
  /** @override */
  doTask() {
    const model = this.config.model;
    const autoMlService = new AutoMlService();
    return Promise.resolve(this.getInputUri_(this.config.source)).then(
        (inputUri) => {
          return autoMlService.batchPredict(
              model.projectId,
              model.location,
              model.modelId,
              inputUri,
              this.getOutputUri_(this.config.destination));
        });
  }

  getInputUri_(options) {
    if (options.table) {
      return {
        bigquerySource: {
          inputUri: `bq://${super.getCloudProject(
              options.table)}.${options.table.datasetId}.${options.table.tableId}`
        }
      };
    }
    if (options.file) {
      const storageUtils = new StorageUtils(options.file.bucket,
          options.file.name);
      return storageUtils.listFiles().then((files) => {
        return {
          gcsSource: {
            inputUris: files.map(
                (fileName) => `gs://${options.file.bucket}/${fileName}`)
          }
        }
      });
    }
    throw new Error('Unimplemented AutoML source.')
  }

  getOutputUri_(options) {
    if (options.table) {
      return {
        bigqueryDestination: {
          outputUri: `bq://${super.getCloudProject(options.table)}`
        }
      };
    }
    throw new Error('Unimplemented AutoML destination.')
  }
}

exports.PredictTask = PredictTask;