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
 * @fileoverview Interface for AutoMl Tables API.
 */

'use strict';

const automl = require('@google-cloud/automl');
const google = automl.protos.google;
const {
  automl: {AutoMl},
  storage: {StorageFile},
} = require('@google-cloud/nodejs-common');
const {Predict} = require('./base_predict.js');

/** AutoML batch predict. */
class AutoMlPredict extends Predict {

  /** @constructor */
  constructor(service = new AutoMl()) {
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
        outputUri);
    return jobName;
  }

  /** @override */
  async isPredictDone(jobName) {
    const result = await this.getPredictJob(jobName);
    return !(!result.done);
  }

  /** @override */
  async getPredictOutput(jobName) {
    const predictionJob = await this.getPredictJob(jobName);
    if (predictionJob.error) throw new Error(predictionJob.error.message);
    const outputInfo = this.getOutputInfo(predictionJob);
    return this.getOutputObject(outputInfo);
  }

  /**
   * Get the prediction job.
   * @param {string} jobName
   * @return {Promise<google.longrunning.Operation>}
   */
  async getPredictJob(jobName) {
    return this.service.getOperation(jobName);
  }

  /** Get the OutputInfo of the given prediction job. */
  getOutputInfo(predictionJob) {
    return predictionJob.metadata.batchPredictDetails.outputInfo;
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
   */
  async getInputUri(options) {
    if (options.table) {
      const {projectId, datasetId, tableId} = options.table;
      return {
        bigquerySource: {
          inputUri: `bq://${projectId}.${datasetId}.${tableId}`,
        },
      };
    }
    if (options.file) {
      const {bucket, name} = options.file;
      const storageFile = StorageFile.getInstance(bucket, name);
      const files = await storageFile.listFiles();
      return {
        gcsSource: {
          inputUris: files.map((fileName) => `gs://${bucket}/${fileName}`),
        },
      };
    }
    throw new Error('Unimplemented predict source.')
  }

  /**
   * Returns where to output the results of batch prediction.
   * @param {{
   *   table:(!BigQueryTableConfig|undefined),
   *   file:(!StorageFileConfig|undefined),
   * }} options Destination configuration.
   * @return {!google.cloud.automl.v1.IBatchPredictOutputConfig}
   */
  getOutputUri(options) {
    if (options.table) {
      return {
        bigqueryDestination: {
          outputUri: `bq://${options.table.projectId}`,
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
    throw new Error('Unimplemented predict destination.')
  }

  /**
   * Returns the output results of a predict job.
   * @see https://cloud.google.com/automl/docs/reference/rpc/google.cloud.automl.v1beta1#google.cloud.automl.v1beta1.OutputConfig
   * @param {{
   *   bigqueryOutputDataset:string,
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
    if (output.bigqueryOutputDataset) {
      const bqOutput = output.bigqueryOutputDataset; // format 'bq://...'
      const [projectId, datasetId] =
          bqOutput.substr('bq://'.length).split('.');
      return {
        projectId,
        datasetId,
        tableId: 'predictions', //AutoML Tables API has a fixed tableId
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
    throw new Error(`Unknown predict output: ${output}`);
  }
}

module.exports = {AutoMlPredict};
