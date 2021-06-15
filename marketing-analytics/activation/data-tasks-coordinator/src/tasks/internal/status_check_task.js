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
 * @fileoverview
 * Sentinel internal task to check manual asynchronous tasks' status.
 */

'use strict';

const {TaskLog, FIELD_NAMES} = require('../../task_log/task_log_dao.js');
const {BaseTask} = require('../base_task.js');

/**
 * Status of manual asynchronous tasks.
 * @enum {string}
 */
const EXTERNAL_JOB_STATUS = Object.freeze({
  DONE: 'DONE',
  RUNNING: 'RUNNING',
  ERROR: 'ERROR',
  ERROR_IGNORED: 'ERROR_IGNORED',
});

/**
 * @typedef {{
 *   taskLogId: (string|number),
 *   status: EXTERNAL_JOB_STATUS,
 *   messageId: (string|undefined),
 * }}
 */
let TaskLogStatus;

/**
 * Internal Task to update all running manual asynchronous tasks' status:
 * It should be set as a scheduled job.
 *   1. If completed, sends out a message to notify Sentinel to finish it;
 *   2. If there is an error, logs the error message to TaskLog;
 *   3. If it is not completed, then do nothing to that TaskLog.
 *
 * To start this task, send a message to the 'task monitor' topic with the
 * parameters includes '{intrinsic: "status_check"}'.
 *
 * @see 'FIELD_NAMES.REGULAR_CHECK' in './src/task_log/task_log_dao.js'.
 * @type {StatusCheckTask}
 */
class StatusCheckTask extends BaseTask {

  /**
   * Sets the function to initialize a task. This function depends on all Tasks
   * that will be created, so it can't be put into TaskManagerOptions to be
   * reused by all the other Tasks. It is only reused by the main class and this
   * internal status checking task.
   *
   * @see Sentinel.prepareExternalTask
   * @param externalTaskFn
   */
  setPrepareExternalTaskFn(externalTaskFn) {
    this.prepareExternalTask = externalTaskFn;
  }

  /** @override */
  isDone() {
    return Promise.resolve(true);
  }

  /**
   * @return {!Promise<{results:!Array<!TaskLogStatus>}>}
   * @override
   */
  async doTask() {
    const startTime = new Date();
    const filter = {property: FIELD_NAMES.REGULAR_CHECK, value: true};
    const taskLogs = await this.options.taskLogDao.list([filter]);
    // If there is no task need to be checked, try to pause the cron job.
    if (taskLogs.length === 0) {
      await this.pauseCronjobIfNoNewTasks_(startTime);
      return {results: []};
    }
    /**
     * Function to update status of TaskLogs and returns the results.
     * @param {!Array<TaskLogStatus>} previous
     * @param {number|string} taskLogId
     * @param {TaskLog} taskLog
     * @return {!Promise<!Array<TaskLogStatus>>}
     */
    const reduceFn = async (previous, {id: taskLogId, entity: taskLog}) => {
      const results = await previous;
      const currentResult = await this.updateTaskLog_(taskLogId, taskLog);
      results.push(currentResult);
      return results;
    };
    // To reduce the impact of concurrent requests, using 'reduce' to check the
    // tasks one by one. If 'reduce' causes a performance issue, then change to
    // use 'apiSpeedControl' nodejs-common.
    return {results: await taskLogs.reduce(reduceFn, [])};
  }

  /**
   * Checks and returns one taskLog status.
   * @param {string} taskLogId
   * @param {!TaskLog} taskLog
   * @return {!TaskLogStatus}
   * @private
   */
  async updateTaskLog_(taskLogId, taskLog) {
    const taskConfigId = taskLog.taskId;
    const parameters = JSON.parse(taskLog.parameters);
    try {
      const task = await this.prepareExternalTask(taskConfigId, parameters);
      const done = await task.isDone();
      if (done) return this.sendFinishTaskMessage_(taskLogId);
      return {taskLogId, status: EXTERNAL_JOB_STATUS.RUNNING,};
    } catch (error) {
      this.logger.error(`Error in task: ${taskLogId}`, error);
      const taskConfig = await this.options.taskConfigDao.load(taskConfigId);
      const errorOptions = taskConfig.errorOptions || {};
      if (errorOptions.ignoreError !== true) {
        await this.options.taskLogDao.saveErrorMessage(taskLogId, error);
        return {taskLogId, status: EXTERNAL_JOB_STATUS.ERROR,};
      }
      this.logger.debug('Ignore error task: ', taskLogId);
      await this.options.taskLogDao.saveErrorMessage(taskLogId, error, true);
      if (taskConfig.next) {
        await this.taskManager.startTasks(taskConfig.next,
            {[FIELD_NAMES.PARENT_ID]: taskLogId},
            taskLog.parameters); // Error task pass out origin parameters.
      }
      return {taskLogId, status: EXTERNAL_JOB_STATUS.ERROR_IGNORED,};
    }
  }

  /**
   * Sends a 'finish task' message to Pub/Sub to notify Sentinel and returns the
   * TaskLogStatus.
   * @param {string} taskLogId
   * @return {!TaskLogStatus}
   * @private
   */
  async sendFinishTaskMessage_(taskLogId) {
    const messageId = await this.taskManager.sendTaskMessage(
        '', // No parameter is required for a finish task message.
        {taskLogId}
    );
    return {
      taskLogId,
      messageId,
      status: EXTERNAL_JOB_STATUS.DONE,
    };
  }

  /**
   * Pauses the cronjob to trigger this task if there is no more tasks need
   * check.
   * @param {!Date} startTime Start time of this check
   * @return {!Array<boolean>}
   * @private
   */
  async pauseCronjobIfNoNewTasks_(startTime) {
    const filter = {property: 'createTime', value: startTime, operator: '>'};
    const taskLogs = await this.options.taskLogDao.list([filter],
        {name: 'createTime', desc: true}, 1);
    if (taskLogs.length === 0) {
      this.logger.info(
          'There is no tasks need to be checked. Pause cronjob now.');
      return this.options.statusCheckCronJob.pause();
    }
  }
}

module.exports = {
  EXTERNAL_JOB_STATUS,
  StatusCheckTask,
};
