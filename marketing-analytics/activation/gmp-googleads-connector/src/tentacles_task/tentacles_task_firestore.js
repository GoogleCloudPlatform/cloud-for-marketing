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
 * @fileoverview Tentacles Task implementation class which is based on
 *     Firestore (native mode or Datastore mode).
 */

'use strict';
const {
  firestore: {DataSource, Entity, DataAccessObject},
  utils: {getLogger,}
} = require('@google-cloud/nodejs-common');
const {TaskStatus, TentaclesTask} = require('./tentacles_task.js');

/**
 * Returns updated Task entity based on the given one.
 * This function is used for Task changing to next status.
 * @typedef {function(!Entity):!Promise<!Entity|undefined>}
 */
let GetUpdatedTask;

/**
 * Tentacles Task data object Firestore native mode or Datastore mode.
 * @implements {TentaclesTask}
 */
class TentaclesTaskOnFirestore extends DataAccessObject {

  /**
   * Initializes TentaclesTask Dao instance.
   * @param {!DataSource} dataSource The data source type.
   * @param {string} namespace The namespace of the data.
   */
  constructor(dataSource, namespace = 'tentacles') {
    super('Task', namespace, dataSource);
    this.logger = getLogger('TentaclesTask');
  }

  /**
   * Parses the task id to proper type. Firestore Datastore mode auto generated
   * id is a number, however Pub/Sub attributes only support string values to be
   * passed. In order to use the id passed back from Pub/Sub, for Firestore
   * Datastore mode, the id value need to be parsed to number before using.
   * @param {string|number} id Id of the task.
   * @return {string|number} id of the task.
   * @private
   */
  parseTaskId_(id) {
    if (this.dataSource === DataSource.DATASTORE) {
      return isNaN(id) ? id : parseInt(id);
    }
    return id;
  }

  /**
   * Updates Task entity in a checked context. It checks the existence of the
   * 'Task' before updates it with the value that passed in function returns.
   * The function passed in will return new values of Task entity if its status
   * is correct, otherwise it returns undefined.
   * @param {string|number} id Tentacles Task ID.
   * @param {!GetUpdatedTask} fn The function that does the update work.
   * @return {!Promise<boolean>} Whether the update is successful.
   * @private
   */
  checkedSave_(id, fn) {
    if (!id) {
      console.log(`Empty Task ID (test mode), always returns TRUE.`);
      return Promise.resolve(true);
    }
    id = this.parseTaskId_(id);
    return this.load(id).then((task) => {
      if (!task) {
        console.warn(`Can't find the Task[${id}]. Quit.`);
        return false;
      }
      return Promise.resolve(fn(task))
          .then((task) => {
            if (!task) return false;
            return this.update(task, id).then(() => {
              this.logger.info(
                  JSON.stringify(Object.assign({id, action: fn.name}, task)));
              return true;
            });
          })
          .catch((error) => {
            console.error(error);
            return false;
          });
    });
  }

  /** @override */
  async createTask(entitySkeleton) {
    const entity = Object.assign(
        {}, entitySkeleton,
        {status: TaskStatus.QUEUING, createdAt: new Date()});
    const id = await this.create(entity);
    this.logger.info(
        JSON.stringify(Object.assign({action: 'createTask', id}, entity)));
    return id;
  }

  /** @override */
  updateTask(id, data) {
    const addMessageId = /** @type {!GetUpdatedTask} */(task) => {
      return Object.assign({}, task, data);
    };
    return this.checkedSave_(id, addMessageId);
  }

  /** @override */
  start(id) {
    const startTask = /** @type {!GetUpdatedTask} */(task) => {
      if (task.status === TaskStatus.QUEUING) {
        return Object.assign(
            {}, task, {status: TaskStatus.SENDING, startSending: new Date()});
      }
      console.warn(`Wrong status[${task.status}] to Start for Task[${id}].`);
    };
    return this.checkedSave_(id, startTask);
  }

  /** @override */
  finish(id, status) {
    const finishTask = /** @type {!GetUpdatedTask} */(task) => {
      if (task.status === TaskStatus.SENDING) {
        return Object.assign({}, task, {
          status: status ? TaskStatus.DONE : TaskStatus.FAILED,
          finishedTime: new Date(),
        });
      }
      console.warn(`Wrong status[${task.status}] to Finish for Task[${id}].`);
    };
    return this.checkedSave_(id, finishTask);
  }

  /** @override */
  logError(id, error) {
    const saveErrorMessage = /** @type {!GetUpdatedTask} */(task) => {
      console.warn(`Error in this task[${id}]: `, error);
      return Object.assign({}, task, {
        status: TaskStatus.ERROR,
        finishedTime: new Date(),
        error: error.toString(),
      });
    };
    return this.checkedSave_(id, saveErrorMessage);
  }
}

exports.TentaclesTaskOnFirestore = TentaclesTaskOnFirestore;
