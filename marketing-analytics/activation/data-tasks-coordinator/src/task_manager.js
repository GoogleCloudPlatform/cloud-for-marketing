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
  utils: {getLogger},
} = require('@google-cloud/nodejs-common');
const {TaskGroup, TaskConfigDao} = require('./task_config/task_config_dao.js');
const {TaskLogDao} = require('./task_log/task_log_dao.js');
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
   * Starts a group of tasks with given attributes and parameters.
   * @param {(!TaskGroup|undefined)} taskGroup Next tasks.
   * @param {{string,string}=} defaultAttributes Message attributes.
   * @param {string} parameterStr JSON string of parameters.
   * @return {Promise<!Array<string>>} Ids of messages to trigger next task(s).
   */
  startTasks(taskGroup, defaultAttributes = {}, parameterStr) {
    const tasks = getTaskArray(taskGroup);
    return Promise.all(tasks.map((nextTask) => {
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
      return this.sendTaskMessage(param, attributes);
    }));
  }

  /**
   * Sends a message to the topic that Sentinel monitors.
   * @param {string} parameters Message data.
   * @param {Object<string,string>} attributes Message attributes.
   * @return {!Promise<string>} Message Id.
   */
  sendTaskMessage(parameters, attributes) {
    return this.pubsub.publish(this.getMonitorTopicName(), parameters,
        attributes);
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
