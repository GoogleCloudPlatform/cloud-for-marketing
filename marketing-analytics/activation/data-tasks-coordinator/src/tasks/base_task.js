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
  utils: {replaceParameters, getLogger},
} = require('@google-cloud/nodejs-common');
const {FIELD_NAMES} = require('../task_log/task_log_dao.js');
const {TaskManager} = require('../task_manager.js');

/**
 * @typedef {import('../task_manager.js').TaskManagerOptions} TaskManagerOptions
 * @typedef {import('../task_config/task_config_dao.js').TaskGroup} TaskGroup
 */

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
 *     complete, so their status should be polled regularly, e.g. Vertex AI
 *     API batch predict job, or DCM generating offline reports.
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
  async finish() {
    const updatesToTaskLog = (await this.completeTask()) || {};
    if (this.config.appendedParameters) {
      updatesToTaskLog.parameters = this.appendParameter(
        this.config.appendedParameters,
      );
    }
    return {
      [FIELD_NAMES.REGULAR_CHECK]: false,
      ...updatesToTaskLog,
    };
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
  doTask() {}

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
      replaceParameters(this.originalConfigString, this.parameters),
    );
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

  /**
   * Validates that a single value interpolated into a SQL query does not
   * contain SQL metacharacters, spaces, or comment sequences.
   * @param {*} value The value to validate.
   * @param {string} paramName Parameter path or field name for error reporting.
   * @protected
   */
  assertSafeSqlValue_(value, paramName) {
    if (
      typeof value !== 'string' &&
      typeof value !== 'number' &&
      typeof value !== 'boolean'
    ) {
      const actualType = Array.isArray(value) ? 'array' : typeof value;
      throw new Error(
        `Unsafe SQL parameter "${paramName}": expected a primitive value ` +
          `(string, number, or boolean), but received ${actualType}.`,
      );
    }
    const strValue = String(value);
    if (strValue.includes('--')) {
      throw new Error(
        `Unsafe SQL parameter "${paramName}": value contains disallowed SQL ` +
          'comment sequence "--".',
      );
    }
    if (!/^[a-zA-Z0-9_.\-:/$]*$/.test(strValue)) {
      const invalidChars = Array.from(
        new Set(strValue.match(/[^a-zA-Z0-9_.\-:/$]/g)),
      )
        .map(ch => JSON.stringify(ch))
        .join(', ');
      throw new Error(
        `Unsafe SQL parameter "${paramName}": value contains disallowed ` +
          `character(s) ${invalidChars}. Only alphanumeric characters and ` +
          '"_", ".", "-", ":", "/", "$" are allowed (spaces, quotes, ' +
          'semicolons, backticks, and other SQL metacharacters are forbidden).',
      );
    }
  }

  /**
   * Validates all parameter values referenced by `${...}` placeholders in a
   * raw SQL template string. Only parameters actually referenced in the SQL
   * template are checked so unrelated pipeline metadata (e.g. GCS file
   * metadata or upstream records) does not cause false rejections.
   * @param {string} sqlTemplate Raw SQL template before parameter replacement.
   * @protected
   */
  assertSafeSqlTemplateParameters_(sqlTemplate) {
    if (typeof sqlTemplate !== 'string' || !this.parameters) return;
    const regex = /\$\{([^}]*)\}/g;
    let match;
    while ((match = regex.exec(sqlTemplate)) !== null) {
      const paramPath = match[1];
      const splitNames = paramPath.split('.');
      let value = this.parameters;
      let found = true;
      for (const namePiece of splitNames) {
        if (
          !value ||
          typeof value !== 'object' ||
          !Object.prototype.hasOwnProperty.call(value, namePiece) ||
          value[namePiece] === undefined ||
          value[namePiece] === null
        ) {
          found = false;
          break;
        }
        value = value[namePiece];
      }
      if (found) {
        this.assertSafeSqlValue_(value, paramPath);
      }
    }
  }
}

module.exports = {
  BaseTask,
};
