// Copyright 2026 Google Inc.
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
 * @fileoverview Execute a Cloud Run Job.
 */

'use strict';

const lodash = require('lodash');
const {JobsClient, ExecutionsClient, protos} = require('@google-cloud/run');

const {
  utils: {wait},
  storage: {StorageFile},
} = require('@google-cloud/nodejs-common');
const {BaseTask} = require('../base_task.js');
const {StorageFileConfig} = require('../../task_config/task_config_dao.js');

/**
 * Configuration of a Cloud Run Job notebook. For the overrides,
 * @see https://docs.cloud.google.com/run/docs/reference/rest/v2/projects.locations.jobs/run#Overrides
 *
 * @typedef {{
 *   projectId:string,
 *   locationId:string,
 *   runJobId:string,
 *   jobParameters:{
 *     taskParameters:(string|Array<object>|StorageFileConfig),
 *     timeout:(string|undefined),
 *     args:object,
 *     env:Array<object>,
 *   }
 * }}
 */
let RunJobSource;

/**
 * The configuration object for ExecuteRunJobTask.
 * `dueTime` Task execution timeout time in minutes. If task group times out, it
 * will be marked with 'timeout' error code.
 *
 * @typedef {{
 *   type:TaskType.EXECUTE_RUN_JOB,
 *   source:!RunJobSource,
 *   dueTime:(number|undefined),
 *   errorOptions:(!ErrorOptions|undefined),
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(!TaskGroup|undefined),
 * }}
 */
let ExecuteRunJobTaskConfig;

/** @const {string} EXECUTION_NAME The execution name. */
const EXECUTION_NAME = 'executionName';

/**
 * Execute a Cloud Run Job.
 * @see https://docs.cloud.google.com/run/docs/reference/rest/v2/projects.locations.jobs/run
 */
class ExecuteRunJobTask extends BaseTask {
  /** @constructor */
  constructor(config, parameters, defaultProject = undefined) {
    super(config, parameters, defaultProject);
    this.jobsClient = new JobsClient();
  }

  /** @override */
  async doTask() {
    const {projectId, locationId, runJobId, jobParameters} = this.config.source;
    const fullJobName = this.jobsClient.jobPath(
      projectId,
      locationId,
      runJobId,
    );
    const {
      taskParameters,
      taskCount: explicitTaskCount = 0,
      timeout,
      args,
      env = {},
    } = jobParameters;

    let taskCount;
    if (taskParameters) {
      const taskParametersArray = await this.getTaskParameters(taskParameters);
      taskCount =
        explicitTaskCount > 0 && explicitTaskCount < taskParametersArray.length
          ? explicitTaskCount
          : taskParametersArray.length;
      env.taskParameters = JSON.stringify(taskParametersArray);
    }
    const envForJob = Object.keys(env).map(key => {
      const value = env[key];
      return {
        name: key,
        value: typeof value === 'string' ? value : JSON.stringify(value),
      };
    });

    const overrides = {
      taskCount,
      timeout,
      containerOverrides: [
        {
          args: [args ? JSON.stringify(args) : undefined],
          env: envForJob,
        },
      ],
    };

    const [operation] = await this.jobsClient.runJob({
      name: fullJobName,
      overrides: overrides,
    });
    const operationName = operation.name;
    const executionName = operation.metadata.name;
    this.logger.info(
      `Start cloud run job ${operationName}, as ${executionName}`,
    );
    const executionId = executionName.substring(
      executionName.lastIndexOf('/') + 1,
    );
    return {
      jobId: executionId, // used to find this task with a logging message
      operationName, // reserved for audit
      parameters: this.appendParameter({
        [EXECUTION_NAME]: executionName,
      }),
    };
  }

  /**
   *
   * Retrieves task parameters either from a string, a Cloud Storage file, or
   * directly from an array.
   * If `taskParameters` is a string, it is parsed as a JSON array of objects.
   * If `taskParameters` is a `StorageFileConfig`, it reads the file from GCS,
   * splits it by newlines, filters out empty lines, and parses each valid line
   * as a JSON object.
   * If `taskParameters` is already an array, it is returned directly.
   * @param {string|!Array<!Object<string, *>>|!StorageFileConfig} taskParameters
   *   The raw task parameters.
   * @return {!Promise<!Array<!Object<string, *>>>} An array of task parameter objects.
   */
  async getTaskParameters(taskParameters) {
    if (typeof taskParameters === 'string') return JSON.parse(taskParameters);
    if (!Array.isArray(taskParameters)) {
      const {bucket, name, projectId} = taskParameters;
      const content = await StorageFile.getInstance(bucket, name, {
        projectId,
      }).loadContent(0);
      return content
        .split('\n')
        .filter(line => line.trim())
        .map(line => JSON.parse(line));
    }
    return parameters;
  }

  /** @override */
  async completeTask() {
    const executionName = this.parameters[EXECUTION_NAME];
    const execClient = new ExecutionsClient();
    const [execution] = await execClient.getExecution({name: executionName});
    const {
      taskCount,
      runningCount,
      succeededCount,
      failedCount,
      cancelledCount,
      completionTime,
    } = execution;
    if (succeededCount === 0) {
      this.logger.error(
        `Sentinel Task [Cloud Run Job] all failed: ${failedCount}/${taskCount}`,
      );
    } else if (failedCount > 0) {
      this.logger.warn(
        `Sentinel Task [Cloud Run Job] has failed tasks: ${failedCount}/${taskCount}`,
      );
    }
    return {
      executionResults: {
        taskCount,
        succeededCount,
        failedCount,
        cancelledCount,
        completionTime,
      },
    };
  }
}

module.exports = {
  ExecuteRunJobTask,
  ExecuteRunJobTaskConfig,
};
