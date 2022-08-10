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

const { utils: { wait } } = require('@google-cloud/nodejs-common');
const {
  TaskLogStatus,
  TaskLogDao,
  FIELD_NAMES,
} = require('../../task_log/task_log_dao.js');
const { ErrorHandledStatus } = require('../error/error_handled_status.js');
const { RetryableError } = require('../error/retryable_error.js');
const {BaseTask} = require('../base_task.js');

/**
 * Status of checked tasks.
 * @enum {string}
 */
const CHECKED_TASK_STATUS = Object.freeze({
  DONE: 'DONE',
  RUNNING: 'RUNNING',
  RETRIED: 'RETRIED',
  ERROR: 'ERROR',
  ERROR_IGNORED: 'ERROR_IGNORED',
});

/**
 * When the number of async tasks is lower, it is very likely it starts handle
 * the mutilple-layered embedded tasks.
 * @const{number}
 */
const EXTRA_CHECK_THRESHOLD = 10;

/** @const{number} Maximum times of extra checks. */
const EXTRA_CHECK_TIMES = 5;

/**
 * Log data structure for a check of a task.
 *
 * @typedef {{
 *   taskLogId: (string|number),
 *   status: CHECKED_TASK_STATUS,
 *   messageId: (string|undefined),
 * }}
 */
let TaskCheckLog;

/**
 * Internal Task to update all running manual asynchronous tasks' status:
 * It should be set as a scheduled job.
 *   1. If completed, sends out a message to notify Sentinel to finish it;
 *   2. If there is an error, logs the error message to TaskLog;
 *   3. If it is not completed, then do nothing to that TaskLog.
 *
 * Updated 2022/06/08:
 *   Add handling process for left over tasks due to Cloud Functions' crashes.
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
   * @return {!Promise<{results:!Array<!TaskCheckLog>}>}
   * @override
   */
  async doTask() {
    const startTime = new Date();
    const filter = {property: FIELD_NAMES.REGULAR_CHECK, value: true};
    const asyncTaskLogs = await this.options.taskLogDao.list([filter]);
    const crashedTaskLogs = await this.getCrashedTasks_();
    const taskLogs = asyncTaskLogs.concat(crashedTaskLogs);
    // If there is no task need to be checked, try to pause the cron job.
    if (taskLogs.length === 0) {
      await this.pauseCronjobIfNoNewTasks_(startTime);
      return {results: []};
    }
    const extraCheck = asyncTaskLogs.length < EXTRA_CHECK_THRESHOLD;
    // To reduce the impact of concurrent requests, using 'reduce' to check the
    // tasks one by one. If 'reduce' causes a performance issue, then change to
    // use 'apiSpeedControl' nodejs-common.
    const results = await taskLogs.reduce(this.getReduceFn(), []);
    if (!extraCheck) return { results };
    return { results: await this.extraCheck_(asyncTaskLogs, results) };
  }
  /**
   * Gets the reduce function to update status of TaskLogs and returns results.
   * @param {!Array<TaskCheckLog>} previous
   * @param {number|string} taskLogId
   * @param {TaskLog} taskLog
   * @return {!Promise<!Array<TaskCheckLog>>}
   */
  getReduceFn() {
    return async (previous, { id: taskLogId, entity: taskLog }) => {
      const results = await previous;
      const currentResult = await this.updateTaskLog_(taskLogId, taskLog);
      results.push(currentResult);
      return results;
    };
  }

  /**
   * Returns an array of crashed tasks.
   * Currently, only check those tasks in 'FINISHING' status.
   * @return {!Array<{id:string,entity:!TaskLog}>}
   * @private
   */
  async getCrashedTasks_() {
    const filter = { property: 'status', value: TaskLogStatus.FINISHING };
    const taskLogs = await this.options.taskLogDao.list([filter]);
    const now = new Date();
    return taskLogs.filter(({ id: taskLogId, entity: taskLog }) => {
      if (taskLog.prefinishTime) {
        //Firestore returns Timestamp type which has the 'toDate' function,
        //while Datastore returns as Date type.
        const prefinishTime = taskLog.prefinishTime.toDate ?
          taskLog.prefinishTime.toDate() : taskLog.prefinishTime;
        if (now - prefinishTime > 9 * 60 * 1000) {
          this.logger.warn('Got a timeout finishing task:', taskLog);
          return true;
        }
      } else {
        this.logger.warn(`Finishing task:[${taskLogId}] without prefinishTime`,
          taskLog);
      }
      return false;
    });
  }

  /**
   * Checks and returns one taskLog status.
   * @param {string} taskLogId
   * @param {!TaskLog} taskLog
   * @return {!TaskCheckLog}
   * @private
   */
  async updateTaskLog_(taskLogId, taskLog) {
    const taskConfigId = taskLog.taskId;
    const parameters = JSON.parse(taskLog.parameters);
    try {
      if (taskLog.status === TaskLogStatus.FINISHING) {
        throw new RetryableError('Cloud Functions crashed.');
      } else {
        const task = await this.prepareExternalTask(taskConfigId, parameters);
        const done = await task.isDone();
        if (done) return this.sendFinishTaskMessage_(taskLogId);
        return { taskLogId, status: CHECKED_TASK_STATUS.RUNNING, };
      }
    } catch (error) {
      this.logger.error(`Error in task: ${taskLogId}`, error);
      const [errorHandledStatus] =
        await this.taskManager.handleFailedTask(taskLogId, taskLog, error);
      switch (errorHandledStatus) {
        case ErrorHandledStatus.RETRIED:
          return { taskLogId, status: CHECKED_TASK_STATUS.RETRIED, };
        case ErrorHandledStatus.FAILED:
          return { taskLogId, status: CHECKED_TASK_STATUS.ERROR, };
        case ErrorHandledStatus.IGNORED:
          this.logger.debug('Ignore error task: ', taskLogId);
          const taskConfig = await this.options.taskConfigDao.load(taskConfigId);
          if (taskConfig.next) {
            await this.taskManager.startTasks(taskConfig.next,
              { [FIELD_NAMES.PARENT_ID]: taskLogId },
              taskLog.parameters); // Error task pass out origin parameters.
          }
          return { taskLogId, status: CHECKED_TASK_STATUS.ERROR_IGNORED, };
      }
    }
  }

  /**
   * Run extra checks those running task with the conditions:
   *  1) some tasks are done(including failed);
   *  2) some tasks are still running.
   * It waits 10 seconds between each round extra checks.
   * It runs up to 5 times and merges the results to the original one.
   * @param {Array<{id:string,entity:!TaskLog}>} taskLogs The array of TaskLog
   *     that need to be extra checked.
   * @param {!Array<TaskCheckLog>} results The results of the original check.
   * @return {!Array<TaskCheckLog>} The combination of the original check and
   *     the extra checks.
   * @private
   */
  async extraCheck_(taskLogs, results) {
    const mappedTaskLogs = {};
    taskLogs.forEach(({ id: taskLogId, entity: taskLog }) => {
      mappedTaskLogs[taskLogId] = taskLog;
    });
    let checkedResults = results.filter(
      ({ taskLogId }) => !!mappedTaskLogs[taskLogId]
    );
    let checkedTimes = 0;
    while (this.hasDoneTasks_(checkedResults) && ++checkedTimes < EXTRA_CHECK_TIMES) {
      const runningTaskLogIds = checkedResults
        .filter(({ status }) => status === CHECKED_TASK_STATUS.RUNNING)
        .map(({ taskLogId }) => taskLogId);
      this.logger.info(`Extra check ${checkedTimes}`, runningTaskLogIds);
      if (runningTaskLogIds.length === 0) break;
      await wait(10000);
      checkedResults = await runningTaskLogIds.map(
        (id) => ({ id, entity: mappedTaskLogs[id] })
      ).reduce(this.getReduceFn(), []);
      results.push(...checkedResults);
    }
    return results;
  }

  /**
   * Returns whether there is a task done.
   * @param {!Array<TaskCheckLog>} results
   * @returns
   * @private
   */
  hasDoneTasks_(results) {
    return results.some(({ status }) => {
      return status === CHECKED_TASK_STATUS.DONE ||
        status === CHECKED_TASK_STATUS.ERROR ||
        status === CHECKED_TASK_STATUS.ERROR_IGNORED;
    });
  }

  /**
   * Sends a 'finish task' message to Pub/Sub to notify Sentinel and returns the
   * TaskCheckLog.
   * @param {string} taskLogId
   * @return {!TaskCheckLog}
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
      status: CHECKED_TASK_STATUS.DONE,
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
  CHECKED_TASK_STATUS,
  StatusCheckTask,
};
