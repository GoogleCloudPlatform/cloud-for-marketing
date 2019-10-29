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
 * @fileoverview Task Helper class.
 */

'use strict';
const {TaskConfigDao, TaskConfig, TaskType} = require(
    '../dao/task_config_dao.js');
const {LoadTask} = require('./load_task.js');
const {QueryTask} = require('./query_task.js');
const {PredictTask} = require('./predict_task.js');
const {ExportTask} = require('./export_task.js');
/**
 * Creates a task instance base on the taskConfig object.
 * @param {!TaskConfig} taskConfig
 */
const createTask = (taskConfig) => {
  switch (taskConfig.type) {
    case TaskType.LOAD:
      return new LoadTask(taskConfig);
    case TaskType.QUERY:
      return new QueryTask(taskConfig);
    case TaskType.PREDICT:
      return new PredictTask(taskConfig);
    case TaskType.EXPORT:
      return new ExportTask(taskConfig);
  }
};

const startTask = (taskId) => {
  const taskConfigDao = new TaskConfigDao();
  return taskConfigDao.load(taskId).then((taskConfig) => {
    let task;
    switch (taskConfig.type) {
      case TaskType.LOAD:

    }
  });
};