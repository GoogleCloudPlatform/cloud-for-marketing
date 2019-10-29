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
 * @fileoverview Helper functions for Sentinel.
 */

'use strict';

const {v2: {ConfigServiceV2Client}} = require('@google-cloud/logging');
const {
  FirestoreAccessBase: {isNativeMode, DataSource},
  PubSubUtils,
} = require('nodejs-common');
const {TaskConfigDao} = require('./dao/task_config_dao.js');
const {Sentinel} = require('./sentinel.js');

exports.installSink = (topicPrefix) => {
  const projectId = process.env['GCP_PROJECT'];
  const filter = 'resource.type="bigquery_resource" AND protoPayload.methodName="jobservice.jobcompleted"';
  const sinkName = `${topicPrefix}-monitor`;
  const client = new ConfigServiceV2Client();
  const formattedSinkName = client.sinkPath(projectId, sinkName);
  const sink = {
    name: sinkName,
    destination: `pubsub.googleapis.com/projects/${projectId}/topics/${sinkName}`,
    filter: filter,
  };
  return PubSubUtils.getOrCreateTopic(sinkName).then(() => {
    return client.getSink({sinkName: formattedSinkName})
        .then(([existentSink]) => {
          console.log(
              `Find existent Sink[${existentSink.name}], update it now.`);
          const request = {
            sinkName: formattedSinkName,
            sink: sink,
            uniqueWriterIdentity: true,
          };
          return client.updateSink(request);
        })
        .catch((error) => {
          if (error.message.indexOf(`Sink ${sinkName} does not exist`)
              === -1) {
            throw error;
          }
          console.log(`Create Sink [${sinkName}] ...`);
          const request = {
            parent: client.projectPath(projectId),
            sink: sink,
            uniqueWriterIdentity: true,
          };
          return client.createSink(request);
        }).then(([sink]) => {
          const serviceAccount = sink.writerIdentity;
          console.log(`Create/Update Sink [${sink.name}]:`);
          console.log(`    service account: ${serviceAccount}`);
          console.log(`    destination: ${sink.destination}`);
          console.log(`    filter: ${sink.filter}`);
          return serviceAccount;
        })
        .catch(console.error);
  });
};

/**
 * Updates Sentinel Task Configs from a JSON object to Firestore or Datastore.
 *
 * @param {!ModuleConfigJSON} taskConfig The module configuration JSON
 *     object.
 * @return {!Promise<!Array<string>>} Results records.
 */
exports.uploadTaskConfig = (taskConfig) => {
  return isNativeMode().then((nativeMode) => {
    process.env['FIRESTORE_TYPE'] = (nativeMode) ? DataSource.FIRESTORE
        : DataSource.DATASTORE;
    const taskConfigDao = new TaskConfigDao();
    return Promise.all(Object.keys(taskConfig).map((key) => {
      const task = taskConfig[key];
      return taskConfigDao.update(task, key).then((id) => {
        console.log(
            `Update Task Config ${task.type}[${id}]->[${task.next
            || ''}] successfully.`);
        return (`Update Task Config [${id}] successfully.`);
      });
    }));
  });
};

exports.testTaskDirectly = (taskId, parameters = "{}") => {
  console.log('Task: ', taskId);
  console.log('Parameters: ', parameters);
  return isNativeMode().then((nativeMode) => {
    process.env['FIRESTORE_TYPE'] = (nativeMode) ? DataSource.FIRESTORE
        : DataSource.DATASTORE;
    const sentinel = new Sentinel();
    return sentinel.startJobForTask(taskId, JSON.parse(parameters));
  });
};

exports.testTaskThroughPubSub = (taskId, parameters = '{}', topicPrefix) => {
  console.log('Task: ', taskId);
  console.log('Parameters: ', parameters);
  console.log('Topic Prefix: ', topicPrefix);
  return PubSubUtils.publish(`${topicPrefix}-start`, parameters,
      {taskId: taskId});
};