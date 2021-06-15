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
const {Storage} = require('@google-cloud/storage');
const {
  utils: {
    replaceParameters,
    getLogger,
  }
} = require('@google-cloud/nodejs-common');
const {TaskGroup} = require('../task_config/task_config_dao.js');
const {FIELD_NAMES} = require('../task_log/task_log_dao.js');
const {TaskManager, TaskManagerOptions} = require('../task_manager.js');

/**
 * Base class of Sentinel tasks. Tasks are divided into three kinds based on the
 * way of executing and target systems:
 *  1. Synchronous tasks. Tasks will be completed in 'start()' function and
 *     Sentinel will continue to invoke 'finish()' of this task, e.g.
 *     KnotTask without embedded tasks, or Google Ads ReportTask.
 *  2. Automated asynchronous tasks. Tasks based on BigQuery, e.g. LoadTask,
 *     ExportTask and QueryTask. These tasks will get notification when BigQuery
 *     completes the job and Sentinel can continue to next tasks automatically.
 *  3. Manual asynchronous tasks. There's no notification when these task
 *     complete, so their status should be polled regularly, e.g. AutoML Tables
 *     API batchpredict job, or DCM generating offline reports.
 */
class BaseTask {

  /**
   * Initiates a task instance based on the configuration and parameters.
   * @param {Object<string,string>} config The configuration of the task. Parts
   *     of configuration items have the placeholder values, in the format as
   *     ${placeholder}.
   * @param {Object<string,string>} parameters The parameters for this task,
   *     e.g. the value of current day. The parameters need to be kept here, in
   *     the case we need to replace placeholders in places other than the task
   *     configuration, e.g. the query sql loaded from Cloud Storage may contain
   *     'the date of current day' as partition day.
   * @param {string|undefined=} defaultProject The default Cloud Project Id in
   *     the case Cloud Project is missed in some task configuration.
   * @constructor
   */
  constructor(config, parameters, defaultProject = undefined) {
    this.originalConfigString = JSON.stringify(config);
    this.parameters = parameters;
    this.setConfig_();
    this.defaultProject = defaultProject;
    this.logger = this.getLogger();
  }

  /**
   * Some tasks may need Sentinel context to do Sentinel-internal jobs, e.g.
   * loading running manual asynchronous tasks and checking whether they are
   * completed; or sending out 'task completed messages for tasks'.
   * For most of 'regular' tasks, this methods can just left as the default
   * empty.
   *
   * @param {!TaskManagerOptions} options
   */
  injectContext(options) {
    /** @const {!TaskManagerOptions} */ this.options = options;
    /** @const {!TaskManager} */ this.taskManager = new TaskManager(options);
  }

  /**
   * Starts the task and returns properties need to be merged to TaskLog.
   * @return {!Promise<{
   *   next:!TaskGroup,
   *   needCheck:boolean,
   *   jobId:(string|undefined),
   *   parameters:(string|undefined),
   * }>} The properties that need to be saved to TaskLog, e.g. Job ID, or
   * other external system id.
   */
  async start() {
    const updatesToTaskLog = (await this.doTask()) || {};
    const needCheck = this.isManualAsynchronous();
    if (needCheck) {
      this.logger.info(
          `Asynchronous tasks started. Resume the Status Check Task.`);
      await this.options.statusCheckCronJob.resume();
    }
    return {
      next: this.config.next || '',
      [FIELD_NAMES.REGULAR_CHECK]: needCheck,
      ...updatesToTaskLog,
    };
  }

  /**
   * Finishes the task and returns properties need to be merged to TaskLog.
   * @return {!Promise<{
   *   needCheck:boolean,
   * }>} The properties that need to be saved to TaskLog.
   */
  finish() {
    return this.completeTask().then((updatesToTaskLog = {}) => {
      if (this.config.appendedParameters) {
        updatesToTaskLog.parameters = this.appendParameter(
            this.config.appendedParameters);
      }
      return {
        [FIELD_NAMES.REGULAR_CHECK]: false,
        ...updatesToTaskLog,
      };
    });
  }

  /**
   * Returns whether this Task is a manual asynchronous task.
   * After the task is started, a field named'FIELD_NAMES.REGULAR_CHECK' will be
   * written into TaskLog to indicate whether this TaskLog is a manual
   * asynchronous task and need regular status checks for its running status.
   * @return {boolean}
   */
  isManualAsynchronous() {
    return false;
  }

  /**
   * Returns whether this task is done. There are two use cases for this:
   * 1. External tasks use this to report its status to the scheduled check job.
   * 2. Synchronized tasks use this to notify the invoker to complete task.
   * @return {!Promise<boolean>}
   */
  isDone() {
    return Promise.resolve(false);
  }

  /**
   * Executes the detailed operation and returns the job Id in where the
   * operation is carried out, e.g. BigQuery.
   * @return {!Promise<!Object<string,string>>} The properties that need to be
   *     saved to TaskLog, e.g. Job ID, or other external system id.
   * @abstract
   */
  doTask() { }

  /**
   * Executes operations that needs to be done after the job is finished, e.g.
   * fetching the result from an external task.
   * @return {Promise<(!Object<string,string>|undefined)>} The properties that
   *     need to be saved to TaskLog.
   */
  completeTask() {
    return Promise.resolve({});
  }

  /**
   * Replaces placeholders in the original 'config' JSON object based on the
   * 'parameters'. This function needs to be called when 'parameters' updated.
   * @private
   */
  setConfig_() {
    this.config = JSON.parse(
        replaceParameters(this.originalConfigString, this.parameters));
  }

  /**
   * Merges the new parameters to the original one and use the new parameters
   * to refresh the 'this.config' object, then returns the new parameters.
   * @param {!Object<string,string>=} addedParameters
   * @return {string} JSON string of the updated parameters object.
   */
  appendParameter(addedParameters = {}) {
    this.parameters = {
      ...this.parameters,
      ...addedParameters,
    };
    this.setConfig_();
    return JSON.stringify(this.parameters);
  }

  /**
   * Gets the Cloud Project Id from the the options with key 'projectId'. If it
   * doesn't exist, returns the defaultProject.
   * @param {Object<string,string>} options
   * @return {string} Cloud Project Id.
   */
  getCloudProject(options) {
    if (options && options.projectId) return options.projectId;
    return this.defaultProject;
  }

  /**
   * Get a Cloud Storage client library instance.
   * @param {Object<string,string>} options
   */
  getStorage(options) {
    return new Storage({projectId: this.getCloudProject(options)});
  }

  /**
   * Get a Logger tagged with current class name.
   * @return {!winston.Logger}
   */
  getLogger() {
    return getLogger(this.constructor.name);
  }
}

/**
 * Tasks throw this error to let Sentinel retry this task.
 */
class RetryableError extends Error {

}

module.exports = {
  BaseTask,
  RetryableError,
};
