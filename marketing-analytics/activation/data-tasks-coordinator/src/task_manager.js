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

/** @fileoverview TaskManager related classes. */

'use strict';

const {
  pubsub: {EnhancedPubSub},
  utils: { wait, getLogger },
} = require('@google-cloud/nodejs-common');
const {
  TaskGroup,
  TaskConfigDao,
  DEFAULT_RETRY_TIMES,
} = require('./task_config/task_config_dao.js');
const {
  TaskLogStatus,
  TaskLogDao,
  FIELD_NAMES,
} = require('./task_log/task_log_dao.js');
const { RetryableError } = require('./tasks/error/retryable_error.js');
const { ErrorHandledStatus } = require('./tasks/error/error_handled_status.js');
const {Report, ReportConfig} = require('./tasks/report/index.js');

/**
 * This option object contains settings, functions or objects for specific
 * tasks. Initially, it was used to make Sentinel main class more friendly to
 * unit tests by replacing those values with mocked functions/objects. But now
 * more components are using this type. For more details, see:
 * TODO(lushu) update the link to the diagram
 *
 * namespace: the prefix of Cloud Resource, e.g. topic name.
 * taskConfigDao: TaskConfig Data access object (DAO)
 * taskLogDao: TaskLog DAO
 * pubsub: an instance of EnhancedPubSub to help send Pub/Sub messages.
 * statusCheckCronJob: a holder contains pause/resume functions to control the
 *     Cloud Scheduler job of 'StatusCheckTask'
 * validatedStorageTrigger: a function wrapper to prevent duplicated Cloud
 *     Storage events messages.
 *
 * @typedef {{
 *   namespace:string,
 *   taskConfigDao:!TaskConfigDao,
 *   taskLogDao:!TaskLogDao,
 *   pubsub:!EnhancedPubSub,
 *   buildReport:{function(!ReportConfig):!Report},
 *   statusCheckCronJob:{
 *     pause:{function():boolean},
 *     resume:{function():boolean},
 *   },
 *   validatedStorageTrigger: undefined|
 *       {function(MainFunctionOfStorage,string,string=):!CloudFunction},
 * }}
 */
let TaskManagerOptions;

/**
 * This class offers basic operations related to the management of tasks,
 * including TaskConfig, TaskLog, Pub/Sub, etc. It can be used in the main class
 * of Sentinel and other detailed tasks.
 */
class TaskManager {
  /**
   * Initializes the TaskManager instance.
   * @param {!TaskManagerOptions} options.
   */
  constructor(options) {
    /** @const {string} */ this.namespace = options.namespace;
    /** @const {!EnhancedPubSub} */ this.pubsub = options.pubsub;
    /** @const {!TaskConfigDao} */ this.taskConfigDao = options.taskConfigDao;
    /** @const {!TaskLogDao} */ this.taskLogDao = options.taskLogDao;
    /** @const {!Logger} */ this.logger = getLogger('TaskManager');
  }

  /**
   * Returns the topic name to coordinate tasks.
   * @param {string=} topicPrefix
   * @return {string} Topic name.
   */
  getMonitorTopicName(topicPrefix = this.namespace) {
    return `${topicPrefix}-monitor`;
  };

  /**
   * Returns the topic to coordinate tasks.
   * @param {string=} topicPrefix
   * @return {Topic} Pub/sub topic.
   */
  async getMonitorTopic(topicPrefix = this.namespace) {
    if (this.topic) return this.topic;
    const topicName = this.getMonitorTopicName(topicPrefix);
    this.topic = await this.pubsub.getOrCreateTopic(topicName);
    return this.topic;
  };

  /**
   * Starts a group of tasks with given attributes and parameters.
   * @param {(!TaskGroup|undefined)} taskGroup Next tasks.
   * @param {{string,string}=} defaultAttributes Message attributes.
   * @param {string} parameterStr JSON string of parameters.
   * @return {Promise<!Array<string>>} Ids of messages to trigger next task(s).
   */
  startTasks(taskGroup, defaultAttributes = {}, parameterStr) {
    const tasks = getTaskArray(taskGroup);
    const reduceFn = async (previousResults, nextTask) => {
      const results = await previousResults;
      let nextTaskId;
      let param = parameterStr;
      if (typeof nextTask === 'string') {
        nextTaskId = nextTask;
      } else {
        nextTaskId = nextTask.taskId;
        param = JSON.stringify(Object.assign(JSON.parse(parameterStr),
          nextTask.appendedParameters));
      }
      const attributes = {
        ...defaultAttributes,
        taskId: nextTaskId,
      };
      this.logger.debug(`Trigger next task: ${nextTaskId}`);
      const [messageId] = await Promise.all([
        this.sendTaskMessage(param, attributes),
        wait(1000), // Wait one second between each task.
      ]);
      results.push(messageId);
      return results;
    };
    return tasks.reduce(reduceFn, Promise.resolve([]));
  }

  /**
   * Sends a message to the topic that Sentinel monitors.
   * @param {string} parameters Message data.
   * @param {Object<string,string>} attributes Message attributes.
   * @return {!Promise<string>} Message Id.
   */
  async sendTaskMessage(parameters, attributes) {
    const topic = await this.getMonitorTopic();
    return this.pubsub.publish(topic, parameters, attributes);
  }

  /**
   * Task error handling function.
   * @param  {(string|number)} taskLogId
   * @param {!TaskLog} taskLog
   * @param {!Error} error
   * @return {[ErrorHandledStatus,!Promise<string|number>]}
   *   whether to trigger next task(s).
   *   Ids of updated TaskLog entity.
   */
  async handleFailedTask(taskLogId, taskLog, error) {
    const taskConfig = await this.taskConfigDao.load(taskLog.taskId);
    if (!taskConfig) {
      this.logger.warn(
        `Obsolete task: ${taskLog.taskId} in TaskLog[${taskLogId}].`);
      return [
        ErrorHandledStatus.FAILED,
        this.taskLogDao.saveErrorMessage(taskLogId,
          new Error(`Task[${taskLog.taskId}] does not exist any more.`)),
      ];
    }
    /**@type {!ErrorOptions} */
    const errorOptions = Object.assign({
      retryTimes: DEFAULT_RETRY_TIMES,
      ignoreError: false, // Error is not ignored by default.
    }, taskConfig.errorOptions);
    if (error instanceof RetryableError) {
      const retriedTimes = taskLog[FIELD_NAMES.RETRIED_TIMES] || 0;
      if (retriedTimes < errorOptions.retryTimes) {
        // Roll back task status from 'FINISHING' to 'STARTED' and set task
        // needs regular status check (to trigger next retry).
        // TODO(lushu) If there are tasks other than report task needs retry,
        //  consider move this part into the detailed tasks.;
        return [
          ErrorHandledStatus.RETRIED,
          this.taskLogDao.merge({
            status: TaskLogStatus.STARTED,
            [FIELD_NAMES.REGULAR_CHECK]: true,
            [FIELD_NAMES.RETRIED_TIMES]: retriedTimes + 1,
          }, taskLogId),
        ];
      }
      this.logger.info(
        'Reached the maximum retry times, continue to mark this task failed.');
    }
    if (errorOptions.ignoreError !== true) {
      return [
        ErrorHandledStatus.FAILED,
        this.taskLogDao.saveErrorMessage(taskLogId, error),
      ];
    }
    return [
      ErrorHandledStatus.IGNORED,
      this.taskLogDao.saveErrorMessage(taskLogId, error, true),
    ];
  }
}

/**
 * Converts string or Array<string> based tasks list into an Array of tasks.
 * @param {!TaskGroup|undefined} tasks
 * @return {!Array<string>|!Array<{taskId:string, appendedParameters:object}>}
 */
const getTaskArray = (tasks) => {
  if (!tasks) return [];
  return (typeof tasks === 'string')
      ? tasks.split(',').map((task) => task.trim())
      : tasks;
}

module.exports = {
  TaskManagerOptions,
  TaskManager,
  getTaskArray,
};
