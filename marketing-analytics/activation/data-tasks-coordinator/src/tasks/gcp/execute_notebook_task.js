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
 * @fileoverview Execute a Notebook task class.
 */

'use strict';

const lodash = require('lodash');
const { NotebookServiceClient } = require('@google-cloud/notebooks');
const { utils: { changeStringToBigQuerySafe } }
  = require('@google-cloud/nodejs-common');
const { BaseTask } = require('../base_task.js');



/**
 * Configuration of a executable notebook. For the template,
 * @see https://cloud.google.com/vertex-ai/docs/workbench/reference/rest/v1/ExecutionTemplate
 *
 * @typedef {{
 *   projectId:string,
 *   locationId:string,
 *   template:{
 *     inputNotebookFile:string,
 *     outputNotebookFolder:string,
 *     containerImageUri:(string|undefined),
 *     vertexAiParameters:{
 *       env: Object<string, string>,
 *     }
 *   }
 * }}
 */
let NotebookSource;

/**
 * @const {object} DEFAULT_TEMPLATE
 * Default properties for ExecutionTemplate. The properties could be
 * over-written by the task configuration.
 */
const DEFAULT_TEMPLATE = {
  scaleTier: 'CUSTOM',
  masterType: 'n1-standard-4',
  containerImageUri:
    'gcr.io/deeplearning-platform-release/base-cpu:latest',
  jobType: 'VERTEX_AI',
  vertexAiParameters: {},
  kernelSpec: 'python3',
};

/**
 * The configuration object for ExecuteNotebookTask.
 * `dueTime` Task execution timeout time in minutes. If task group times out, it
 * will be marked with 'timeout' error code.
 *
 * @typedef {{
*   type:TaskType.EXECUTE_NOTEBOOK,
*   source:!NotebookSource,
*   dueTime:(number|undefined),
*   errorOptions:(!ErrorOptions|undefined),
*   appendedParameters:(Object<string,string>|undefined),
*   next:(!TaskGroup|undefined),
* }}
*/
let ExecuteNotebookTaskConfig;

/** @const {string} EXECUTION_NAME The execution name. */
const EXECUTION_NAME = 'executionName';

/**
 * Execute a notebook (Colab file) through Vertex AI Execution API.
 * @see https://cloud.google.com/vertex-ai/docs/workbench/reference/rest/v1/projects.locations.executions
 */
class ExecuteNotebookTask extends BaseTask {

  /** @constructor */
  constructor(config, parameters, defaultProject = undefined) {
    super(config, parameters, defaultProject);
    this.client = new NotebookServiceClient();
  }

  /** @override */
  isManualAsynchronous() {
    return true;
  }

  /** @override */
  async doTask() {
    const { projectId, locationId, template } = this.config.source;
    const executionTemplate = lodash.merge({},DEFAULT_TEMPLATE, template);
    const parent = `projects/${projectId}/locations/${locationId}`;
    const executionId = this.getExecutionId_(this.config.source);
    const request = {
      parent,
      executionId,
      execution: {
        executionTemplate,
      },
    };
    const [response] = await this.client.createExecution(request);
    const { target } = response.metadata || {};
    this.logger.info('Start executing notebook:', target);
    return {
      parameters: this.appendParameter({ [EXECUTION_NAME]: target }),
    };
  }

  /**
   * Returns a time based string as the execute id.
   * @private
   */
  getExecutionId_(source) {
    if (source.executionId) return source.executionId;
    const notebookFile = source.template.inputNotebookFile;
    const id = changeStringToBigQuerySafe(
      notebookFile.substring(notebookFile.lastIndexOf('/') + 1) + '_'
      + new Date().toISOString());
    return id;
  }

  /** @override */
  async isDone() {
    const name = this.parameters[EXECUTION_NAME];
    const [response] = await this.client.getExecution({ name });
    const { state } = response;
    switch (state) {
      case 'SUCCEEDED':
        return true;
      case 'INITIALIZING':
      case 'QUEUED':
      case 'PREPARING':
      case 'RUNNING':
        return false;
      default:
        this.logger.error('Wrong execution state', response);
        throw new Error(`Wrong execution state ${state}`);
    }
  }
}

module.exports = {
  ExecuteNotebookTask,
  ExecuteNotebookTaskConfig,
};
