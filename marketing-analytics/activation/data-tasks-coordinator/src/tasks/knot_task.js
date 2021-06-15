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
 * @fileoverview Task to act as a knot to trigger multiple tasks.
 */

'use strict';
const {nanoid} = require('nanoid');
const {
  FIELD_NAMES,
  TaskLogStatus,
  TaskLogDao,
} = require('../task_log/task_log_dao.js');
const {
  TaskType,
  TaskGroup,
  ErrorOptions,
} = require('../task_config/task_config_dao.js');
const {BaseTask} = require('./base_task.js');
const {getTaskArray} = require('../task_manager.js');

/**
 * `embedded` stands for the embedded tasks.
 * `estimateRunningTime` Number of minutes to wait before doing the first task
 * status check for the embedded task group.
 * `dueTime` Task execution timeout time in minutes. If task group times out, it
 * will be marked with 'timeout' error code.
 *
 * @typedef {{
 *   type:TaskType.KNOT,
 *   appendedParameters:(Object<string,string>|undefined),
 *   embedded:{
 *     tasks:TaskGroup,
 *     estimateRunningTime:(number|undefined),
 *     dueTime:(number|undefined),
 *   }|undefined,
 *   errorOptions:(!ErrorOptions|undefined),
 *   next:(!TaskGroup|undefined),
 * }}
 */
let KnotTaskConfig;

/** @const {number} */
const MILLISECONDS_IN_A_MINUTE = 60000;

/** Helper function to tell whether this TaskLog has error occurred. */
const taskHasError = ({entity}) => {
  return entity.status === TaskLogStatus.ERROR
      && entity[FIELD_NAMES.IGNORED_FAILURE] !== true;
};

/** Helper function to tell whether this TaskLog hasn't finished. */
const taskIsNotFinished = ({entity}) => {
  const haveIgnoredError = entity.status === TaskLogStatus.ERROR &&
      entity[FIELD_NAMES.IGNORED_FAILURE] === true;
  return entity.status !== TaskLogStatus.FINISHED && !haveIgnoredError;
};

/**
 * Generate a knot task. A knot task can be used to manage other tasks, e.g.
 *  1. For a group of tasks need to be triggered at the same time, use a knot
 *     task can simplify the setting of cronjob.
 *  2. For tasks with dependencies, use 'embedded' to bind a group of tasks. The
 *     'next' tasks are only triggered after all embedded tasks and their all
 *     descendent tasks are completed.
 */
class KnotTask extends BaseTask {

  /** @override */
  isManualAsynchronous() {
    return !!this.config.embedded;
  }

  /**
   * Returns whether the task needs to be checked based on the execution time.
   * If there is a 'estimateRunningTime' and the task hasn't run at least the
   * `estimateRunningTime` minutes, it returns false;
   * If there is a 'dueTime' and the time is over due, it throws an 'Timeout'
   * error;
   * Else returns true.
   * @param {{estimateRunningTime:number, dueTime:number}} config Two items of
   *     the task execution time. @see KnotTaskConfig
   * @return {boolean} Whether the task should be checked the status.
   */
  shouldCheckTaskStatus(config) {
    const pastTime = Date.now() - this.parameters.startTime;
    if (config.estimateRunningTime &&
        pastTime < config.estimateRunningTime * MILLISECONDS_IN_A_MINUTE) {
      return false;
    }
    if (config.dueTime &&
        pastTime > config.dueTime * MILLISECONDS_IN_A_MINUTE) {
      throw new Error('Timeout');
    }
    return true;
  }

  /**
   * Return true if there is no embedded tasks.
   * Otherwise return false if there is a 'estimateRunningTime' and the time is
   * not over due; throw an out of time error if there is a 'dueTime' and the
   * time is over due; or check the status of embedded tasks.
   *
   * @override
   */
  async isDone() {
    const embeddedConfig = this.config.embedded;
    if (!embeddedConfig) return true;
    const shouldCheck = this.shouldCheckTaskStatus(embeddedConfig);
    const filter = [{
      property: FIELD_NAMES.EMBEDDED_TAG,
      value: this.parameters[FIELD_NAMES.EMBEDDED_TAG],
    }];
    return shouldCheck ?
        this.areTasksFinished(filter, getTaskArray(embeddedConfig.tasks).length)
        : false;
  }

  /**
   * Triggers embedded tasks if there are any.
   * It generates a random Id as the embedded tag and assigns it to the messages
   * that trigger all embedded tasks.
   * @override
   */
  async doTask() {
    if (!this.config.embedded) return Promise.resolve({});
    const embeddedTag = nanoid();
    const messageIds = await this.taskManager.startTasks(
        this.config.embedded.tasks,
        {[FIELD_NAMES.EMBEDDED_TAG]: embeddedTag},
        this.appendParameter(this.config.appendedParameters));
    console.log(`Start embedded ${messageIds.length} tasks`);
    return {
      parameters: this.appendParameter({
        [FIELD_NAMES.EMBEDDED_TAG]: embeddedTag,
        startTime: Date.now(),
      }),
    }
  }

  /**
   * Returns whether the expected tasks are all finished.
   * 1. If any task's status is 'ERROR', then throw an error.
   * 2. Return false: if the number tasks from the 'filter' condition is less
   *    than the number of tasks from 'expectedTasks'; or if any tasks form the
   *    'filter' condition have the status other than 'FINISHED'.
   * 2. Else, get all 'next' definition in the task lists and check the status
   *    of those task lists recursively. Return true only if they all finished.
   * @param {!Array<!Filter>} filter Conditions to filter the list.
   * @param {number} expectedNumber Expected number of tasks.
   * @return {Promise<boolean>} Whether these tasks are finished.
   */
  async areTasksFinished(filter, expectedNumber) {
    if (expectedNumber === 0) return true;
    const targetTasks = await this.options.taskLogDao.list(filter);
    if (targetTasks.some(taskHasError)) {
      throw new Error('Failed embedded task(s).');
    }
    if (targetTasks.length < expectedNumber ||
        targetTasks.some(taskIsNotFinished)) {
      return false;
    }
    const nextTaskStatus = await Promise.all(
        targetTasks.filter(({entity}) => entity.next).map(({id, entity}) => {
          const nextExpectedNumber = getTaskArray(entity.next).length;
          return this.areTasksFinished(
              [{property: FIELD_NAMES.PARENT_ID, value: id}],
              nextExpectedNumber);
        }));
    return nextTaskStatus.indexOf(false) === -1;
  }

}

module.exports = {
  KnotTaskConfig,
  KnotTask,
};
