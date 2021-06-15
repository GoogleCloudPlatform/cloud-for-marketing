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

const automl = require('@google-cloud/automl');
const google = automl.protos.google;
const {
  automl: {AutoMl},
  storage: {StorageFile},
} = require('@google-cloud/nodejs-common');
const {BaseTask} = require('./base_task.js');
const {
  TaskType,
  BigQueryTableConfig,
  StorageFileConfig,
} = require('../task_config/task_config_dao.js');

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
 *     table:(!BigQueryTableConfig|undefined),
 *     file:(!StorageFileConfig|undefined),
 *   },
 *   destination:{
 *     table:!BigQueryTableConfig,
 *     file:(!StorageFileConfig|undefined),
 *   },
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let PredictTaskConfig;

/** @const {string} The prediction job identity name. */
const JOB_ID_PARAMETER = 'operationName';

class PredictTask extends BaseTask {

  /** @override */
  isManualAsynchronous() {
    return true;
  }

  /** @override */
  doTask() {
    const model = this.config.model;
    return this.getInputUri_(this.config.source)
        .then((inputUri) => {
          return this.getAutoMLService_().batchPredict(
              model.projectId,
              model.location,
              model.modelId,
              inputUri,
              this.getOutputUri_(this.config.destination));
        })
        .then((operationName) => {
          return {
            parameters: this.appendParameter(
                {[JOB_ID_PARAMETER]: operationName})
          };
        });
  }

  /** @override */
  isDone() {
    return this.getAutoMLService_()
        .getOperation(this.parameters[JOB_ID_PARAMETER])
        // Error task's response from AutoML server also has 'done' as true .
        .then((result) => !!result.done);
  }

  /** @override */
  completeTask() {
    return this.getAutoMLService_()
        .getOperation(this.parameters[JOB_ID_PARAMETER])
        .then((result) => {
          if (result.error) throw new Error(result.error.message);
          const output = result.metadata.batchPredictDetails.outputInfo;
          const predictOutput = this.getOutputResult_(output);
          return {
            parameters: this.appendParameter({predictOutput})
          };
        });
  }

  /**
   * Returns AutoML service instance.
   * @return {!AutoMl}
   * @private
   */
  getAutoMLService_() {
    return new AutoMl();
  }

  /**
   * Returns data source for batch prediction. If Cloud Storage is the source,
   * it will list all the files that contains the `name` in the config, so it
   * will return a Promise of the inputUri.
   * @param {{
   *   table:(!BigQueryTableConfig|undefined),
   *   file:(!StorageFileConfig|undefined),
   * }} options Source configuration.
   * @return {!Promise<!google.cloud.automl.v1.IBatchPredictInputConfig>}
   * @private
   */
  getInputUri_(options) {
    if (options.table) {
      return Promise.resolve({
        bigquerySource: {
          inputUri: `bq://${this.getCloudProject(options.table)}.${
              options.table.datasetId}.${
              options.table.tableId}`,
        },
      });
    }
    if (options.file) {
      const storageFile = StorageFile.getInstance(options.file.bucket,
          options.file.name);
      return storageFile.listFiles().then((files) => {
        return {
          gcsSource: {
            inputUris: files.map(
                (fileName) => `gs://${options.file.bucket}/${fileName}`),
          },
        }
      });
    }
    throw new Error('Unimplemented AutoML source.')
  }

  /**
   * Returns where to output the results of batch prediction.
   * @param {{
   *   table:(!BigQueryTableConfig|undefined),
   *   file:(!StorageFileConfig|undefined),
   * }} options Destination configuration.
   * @return {!Promise<!google.cloud.automl.v1.IBatchPredictOutputConfig>}
   * @private
   */
  getOutputUri_(options) {
    if (options.table) {
      return {
        bigqueryDestination: {
          outputUri: `bq://${this.getCloudProject(options.table)}`,
        },
      };
    }
    if (options.file) {
      return {
        gcsDestination: {
          outputUriPrefix: `gs://${options.file.bucket}/${options.file.name}`,
        },
      };
    }
    throw new Error('Unimplemented AutoML destination.')
  }

  /**
   * Returns the output results of a predict job.
   * @param output
   * @return {{
   *   bucket: string,
   *   name: string,
   *   }|{
   *   projectId: string,
   *   datasetId: string,
   * }}
   * @private
   */
  getOutputResult_(output) {
    if (output.bigqueryOutputDataset) {
      const bqOutput = output.bigqueryOutputDataset; // format 'bq://...'
      const [projectId, datasetId] =
          bqOutput.substr('bq://'.length).split('.');
      return {
        projectId,
        datasetId,
      };
    }
    if (output.gcsOutputDirectory) {
      const gcsOutput = output.gcsOutputDirectory; // format 'gs://...'
      const path = gcsOutput.substr('gs://'.length).split('/');
      const bucket = path.shift();
      return {
        bucket,
        name: path.join('/'),
      };
    }
    throw new Error(`Unknown AutoML output: ${output}`);
  }
}

module.exports = {
  PredictTaskConfig,
  PredictTask,
};
