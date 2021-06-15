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
 * @fileoverview Helper functions for Sentinel. All the functions in this file
 *     are supposed to be invoked by bash shell for installation or test.
 */

'use strict';

const {
  firestore: {DataSource, FirestoreAccessBase},
} = require('@google-cloud/nodejs-common');
const {TaskConfigDao} = require('./task_config/task_config_dao.js');
const {guessSentinel} = require('./sentinel.js');

/**
 * Updates Sentinel Task Configs from a JSON object to Firestore or Datastore.
 * TODO define TaskConfigJSON in src/tasks/index.js after 'Tasks' merged.
 * @param {!TaskConfigJSON} taskConfig The module configuration JSON
 *     object.
 * @param {Object<string,string>=} parameters The properties from the
 *     installation process and have been saved in the configuration file
 *     `config.json`. The name of properties could be: `PROJECT_ID`,
 *     `GCS_BUCKET` and `OUTBOUND`, etc. Properties can be used in task configs
 *     as `#PROJECT_ID#` format.
 * @param {string} namespace Namespace for the Firestore.
 * @return {!Promise<!Array<{string:boolean}>>} Results records.
 */
exports.uploadTaskConfig = (taskConfig, parameters = {},
    namespace = process.env['PROJECT_NAMESPACE']) => {
  const tasks = JSON.parse(Object.keys(parameters).reduce(
      (previousValue, key) => {
        const regExp = new RegExp(`#${key}#`, 'g');
        return previousValue.replace(regExp, parameters[key]);
      },
      JSON.stringify(taskConfig)));
  return FirestoreAccessBase.isNativeMode().then((isNative) => {
    const dataSource = (isNative) ? DataSource.FIRESTORE : DataSource.DATASTORE;
    const taskConfigDao = new TaskConfigDao(dataSource, namespace);
    const reduceFn = (previousResults, key) => {
      return previousResults.then((results) => {
        const task = tasks[key];
        return taskConfigDao.update(task, key)
            .then((id) => {
              const taskInfo = `${task.type}[${id}]->[${task.next || ''}]`;
              console.log(`Update Task Config ${taskInfo} successfully.`);
              return true;
            }).catch((error) => {
              console.error(`Failed to update ${task.type}[${id}]`, error);
              return false;
            }).then((thisResult) => results.concat({[key]: thisResult}));
      });
    };
    return Object.keys(tasks).reduce(reduceFn, Promise.resolve([]));
  });
};

/**
 * Invokes 'start' of a Task directly to start a task from local environment.
 * However, it still expects all resources are available at Cloud , e.g. the
 * configuration of Task in Firestore, or the loaded file is available in Cloud
 * Storage.
 * It won't trigger next tasks because it doesn't save TaskLog.
 *
 * @param {string} taskConfigId
 * @param {string=} parameters JSON string of parameters.
 * @return {!Promise<(string|undefined)>} Task job Id.
 */
exports.startTaskFromLocal = async (taskConfigId, parameters = "{}",
    namespace = 'sentinel') => {
  console.log('Task: ', taskConfigId);
  console.log('Parameters: ', parameters);
  console.log('Namespace: ', namespace);
  console.log('OAUTH2_TOKEN_JSON: ', process.env['OAUTH2_TOKEN_JSON']);
  const sentinel = await guessSentinel(namespace);
  const task = await sentinel.prepareTask(taskConfigId, JSON.parse(parameters));
  const startedTask = await task.start();
  const idDone = await task.isDone();
  if (idDone) {
    console.log('Task is done, continue to finish.', startedTask);
    const newParameters = startedTask.parameters || parameters;
    console.log('new parameters', newParameters);
    const taskToFinish =
        await sentinel.prepareTask(taskConfigId, JSON.parse(newParameters));
    const output = await taskToFinish.finish();
    console.log('After finish: ', output);
  } else {
    console.log('Task is done yet.');
  }
};

/**
 * Sends a message to Sentinel monitor topic to start a task manually in Google
 * Cloud.
 * It just like the cronjob or event triggered tasks.
 *
 * @param {string} taskConfigId TaskConfigId
 * @param {string=} parameters JSON string of parameters.
 * @param {string=} namespace
 * @return {!Promise<string>} The start task message Id.
 */
exports.startTaskThroughPubSub = (taskConfigId, parameters = '{}',
    namespace = 'sentinel') => {
  console.log('Task: ', taskConfigId);
  console.log('Parameters: ', parameters);
  console.log('Namespace: ', namespace);
  return guessSentinel(namespace).then((sentinel) =>
          sentinel.taskManager.sendTaskMessage(parameters, {taskId: taskConfigId}))
      .then((messageId) => {
        console.log('Sent out message Id:', messageId);
      });
};
