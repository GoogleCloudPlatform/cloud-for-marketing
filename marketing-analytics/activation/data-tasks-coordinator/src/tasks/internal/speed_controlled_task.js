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
 * @fileoverview Task to act as a pacing speed manager for a massive number of
 * requests to a target service.
 */

'use strict';

const { join } = require('path');
const lodash = require('lodash');
const {
  cloudfunctions: { getIdTokenForFunction },
  utils: { requestWithRetry, replaceParameters },
} = require('@google-cloud/nodejs-common');
const {
  TaskType,
  StorageFileConfig,
  TaskGroup,
  ErrorOptions,
} = require('../../task_config/task_config_dao.js');
const { BaseTask } = require('../base_task.js');

/**
 * The configuration for submitting a file to Tentacles HTTP Cloud Functions.
 *
 * `service` stands for the target Tentacles instance.
 * `fileUrl` or `file` are used to present a GCS file resource that Tentacles
 * will handle.
 * `attributes` are attributes that Tentacles will figure out from the file
 * name, e.g. `api`, `config`(name), `size`, etc.
 * `config` is the object for Tentacles config. If this object is present, then
 * in the `attribute`, the `config` will be ignored; otherwise, Tentacles will
 * try to load config from Tentacles Firestore with the `api` and `config` in
 * the `attributes` object.
 *
 * @typedef {{
 *   service: {
 *     projectId: (string|undefined),
 *     locationId: (string|undefined),
 *     namespace: (string|undefined),
 *     url: (string|undefined),
 *   }
 *   fileUrl: (string|undefined),
 *   file: (!StorageFileConfig|undefined),
 *   attributes: (!Object|undefined),
 *   config: (!object|undefined)
 * }}
 */
let TentaclesFileOption;

/**
 * The configuration object for SpeedControlledTask.
 * @see TentaclesFileOption
 * `dueTime` Task execution timeout time in minutes. If task group times out, it
 * will be marked with 'timeout' error code.
 *
 * @typedef {{
 *   type:TaskType.SPEED_CONTROLLED,
 *   source:!TentaclesFileOption,
 *   dueTime:(number|undefined),
 *   errorOptions:(!ErrorOptions|undefined),
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(!TaskGroup|undefined),
 * }}
 */
let SpeedControlledTaskConfig;

/**
 * This task is used to send a massive number of requests to a target service
 * with a managed pacing speed, including QPS, records per request, etc.
 * It is built based on the Tentacles:
 * 1. This task starts by submitting the information of a GCS file and
 *    configuration to the TentaclesFile management Cloud Function to create a
 *    new Tentacles File and related Tentacles Tasks;
 * 2. Tentacles sends the requests with the specified speed setting;
 * 3. This task checks whether all related Tentacles Tasks have been done from
 *    time to time and mark this task as done after all are done.
 */
class SpeedControlledTask extends BaseTask {

  /** @override */
  isManualAsynchronous() {
    return true;
  }

  /**
   * @override
   * This task can have un-replaced parameters as it manages other tasks, so
   * it is possible that the managed ones need to replace those parameters.
   */
  setConfig_() {
    this.config = JSON.parse(
      replaceParameters(this.originalConfigString, this.parameters, true));
  }

  /**
   * Triggers TentaclesFile job to start this 'speed controlled' task.
   * It submits the file and options to Tentacles HTTP CF to start the job and
   * merges the TentaclesFile Id as `tentaclesFileId` to the parameters. This
   * will be used to check the status of the task.
   * @override
   */
  async doTask() {
    const { source } = this.config;
    const options = await getRequestOption(source);
    const file = getFileUrl(source);
    const { attributes, config } = source;
    const defaultConfig = {
      attributes: { api: 'CF' },
    };
    options.data = lodash.merge(defaultConfig, {
      file,
      attributes,
      config,
    });
    const result = await requestWithRetry(options, this.logger);
    const { fileId } = result;
    this.logger.info(`Start tentacles tasks with fileId: ${fileId}`);
    return {
      parameters: this.appendParameter({
        tentaclesFileId: fileId,
        startTime: Date.now(),
      }),
    }
  }

  /** @override */
  async isDone() {
    const { source } = this.config;
    const options = await getRequestOption(source);
    const { tentaclesFileId: fileId } = this.parameters;
    if (!fileId) {
      throw new Error('Fail to find TentaclesFileId');
    }
    options.data = { fileId };
    const result = await requestWithRetry(options, this.logger);
    const { status, error } = result;
    if (status === 'done') return true;
    if (error || status === 'error') {
      this.logger.error(`TentaclesFile ${fileId} error.`, result);
      throw new Error(`TentaclesFile ${fileId} error: ${error}`);
    }
    return false;
  }

}

/**
 * Gets the request options for the TentaclesFile management Cloud Function.
 * This function will prepare the url and authorization token.
 * @param {!TentaclesFileOption} options
 * @return {{
*   url: string,
*   headers: object,
* }} A request option for Gaxios request.
*/
async function getRequestOption(options) {
  const { service } = options;
  const url = getServiceUrl(service);
  const token = await getIdTokenForFunction(url);
  return {
    url,
    headers: { Authorization: `bearer ${token}` },
  };
}

/**
* Returns the HTTP Cloud Functions url. By default, it returns the url of the
* TentaclesFile management Cloud Function of the given Tentacles instance.
* @param {object} service
* @return {string} The url for Tentacles HTTP Cloud Functions.
*/
function getServiceUrl(service) {
  if (service.url) return service.url;
  const { projectId, locationId, namespace } = service;
  return `https://${locationId}-${projectId}.cloudfunctions.net/${namespace}_http`;
}

/**
* Returns the gsutil Url of the given GCS file.
* @param {!TentaclesFileOption} options
* @return {string} The gsutil Url
*/
function getFileUrl(options) {
  if (options.fileUrl) return options.fileUrl;
  const { bucket, name } = options.file;
  return 'gs://' + join(bucket, name);
}

module.exports = {
  TentaclesFileOption,
  SpeedControlledTaskConfig,
  SpeedControlledTask,
  getRequestOption,
  getFileUrl,
};
