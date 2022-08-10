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

/**
 * String value of BigQuery Data Transfer run states.
 * @see https://cloud.google.com/bigquery-transfer/docs/reference/datatransfer/rest/v1/TransferState
 * @const {string}
 */
const TRANSFER_STATE_SUCCEEDED = 'SUCCEEDED';
const TRANSFER_STATE_FAILED = 'FAILED';
const TRANSFER_STATE_CANCELLED = 'CANCELLED';

/** Executes BigQuery Data Transfer run job. */
class DataTransferTask extends BaseTask {

  /**
   * Gets a BigQuery Data Transfer service client instance.
   * @return {DataTransferServiceClient}
   * @private
   */
  getDataTransfer_() {
    const options = this.config.source;
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
    const dataTransfer = this.getDataTransfer_();
    const { transferRunName } = this.parameters;
    const [transferRun] =
      await dataTransfer.getTransferRun({ name: transferRunName });
    return transferRun.state === TRANSFER_STATE_SUCCEEDED
      || transferRun.state === TRANSFER_STATE_FAILED
      || transferRun.state === TRANSFER_STATE_CANCELLED;
  }

  /** @override */
  async completeTask() {
    const dataTransfer = this.getDataTransfer_();
    const { transferRunName } = this.parameters;
    const [transferRun] =
      await dataTransfer.getTransferRun({ name: transferRunName });
    if (transferRun.errorStatus && transferRun.errorStatus.message) {
      throw new Error(transferRun.errorStatus.message);
    }
    if (transferRun.state !== TRANSFER_STATE_SUCCEEDED) {
      throw new Error(`Wrong run state: ${transferRun.state}.`);
    }
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
        requestedRunTime: { seconds: Math.floor(Date.now() / 1000) },
        parent: dataTransferConfigName,
      });
      const transferRuns = runsRes.runs;
      if (transferRuns.length === 0) {
        throw new Error(
          `Data Transfer[${dataTransferConfigName}] can't be started manually.`);
      }
      const transferRunName = transferRuns[0].name;
      return {
        jobId: transferRunName,
        parameters: this.appendParameter({ transferRunName }),
      };
    } catch (error) {
      this.logger.error(`Error when trigger[${dataTransferConfigName}]`, error);
      throw error;
    }
  }
}


module.exports = {
  DataTransferTaskConfig,
  DataTransferTask,
};
