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
 * @fileoverview BigQuery Data Transfer task class.
 */

'use strict';
const {
  DataTransferServiceClient,
} = require('@google-cloud/bigquery-data-transfer');

const {BaseTask} = require('../base_task.js');
const {TaskType} = require('../../task_config/task_config_dao.js');

/**
 * @typedef {{
 *   type:TaskType.DATA_TRANSFER,
 *   source: {
 *     projectId:string,
 *     location:string,
 *     transferId:string,
 *   }
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let DataTransferTaskConfig;

/** Executes BigQuery Data Transfer run job. */
class DataTransferTask extends BaseTask {
  /**
   * Gets a BigQuery Data Transfer service client instance.
   * @param {Object<string,string>} options
   * @return {DataTransferServiceClient}
   * @private
   */
  getDataTransfer_(options) {
    const authOptions = {
      projectId: this.getCloudProject(options),
    };
    return new DataTransferServiceClient(authOptions);
  }

  /**
   * Gets a name of BigQuery Data Transfer config.
   * @return {string} The name of BigQuery Data Transfer config.
   * @private
   */
  getDataTransferConfigName_() {
    const {projectId, location, transferId} = this.config.source;
    return `projects/${projectId}/locations/${location}/transferConfigs/${transferId}`;
  }

  /** @override */
  async isDone() {
    const client = this.getDataTransfer_();
    const {state} = await client.getTransferRun({name: this.jobId});
    return state === 'SUCCEEDED' || state === 'FAILED';
  }

  /**
   * Executes a BigQuery Data Transfer run job by a given config name.
   * @override
   */
  async doTask() {
    const client = this.getDataTransfer_();
    const dataTransferConfigName = this.getDataTransferConfigName_();
    try {
      const [runsRes] = await client.startManualTransferRuns({
        requestedRunTime: {},
        parent: dataTransferConfigName,
      });
      const transferRuns = runsRes.runs;
      if (transferRuns.length === 0) {
        const errorMessage = `There is not any Data Transfer config be triggered by the given parent, ${dataTransferConfigName}.`;
        this.logger.error(errorMessage);
        throw new Error(errorMessage);
      }
      const jobId = transferRuns[0].name;
      this.jobId = jobId;
      return {jobId};
    } catch (error) {
      this.logger.error(
        `Error when triggering Data Transfer config, ${dataTransferConfigName}`,
        error
      );
      throw error;
    }
  }
}

module.exports = {
  DataTransferTaskConfig,
  DataTransferTask,
};
