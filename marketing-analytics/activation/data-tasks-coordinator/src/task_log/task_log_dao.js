// Copyright 2019 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this fileAccessObject except in compliance with the License.
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
 * @fileoverview Task Configuration implementation class which is based on
 *     Firestore (native mode or Datastore mode).
 */

'use strict';

const {
  firestore: {
    TransactionOperation,
    DataAccessObject,
  },
} = require('@google-cloud/nodejs-common');
const {TaskGroup} = require('../task_config/task_config_dao.js');

/**
 * Status of task logs.
 * @enum {string}
 */
const TaskLogStatus = {
  INITIAL: 'INITIAL',
  STARTED: 'STARTED',
  FINISHING: 'FINISHING',
  FINISHED: 'FINISHED',
  ERROR: 'ERROR',
};

/**
 * Special field names of task logs.
 * @enum {string}
 */
const FIELD_NAMES = {
  PARENT_ID: 'parentId',
  // This field indicates whether this TaskLog requires manual status check.
  // @see `../tasks/base_task.js` for the details.
  REGULAR_CHECK: 'needCheck',
  // This field is the tag for a group of embedded tasks.
  // @see `../tasks/knot_task.js`
  EMBEDDED_TAG: 'embeddedTag',
  // This field is the tag for multiple instances of a same task.
  // @see `../tasks/multiple_task.js`
  MULTIPLE_TAG: 'multipleTag',
  // How many times this task has been retried after error happens.
  RETRIED_TIMES: 'retriedTimes',
  // Although the task failed, Sentinel will continue its next task(s).
  // This field only valid when the TaskLog's status is ERROR. When TaskLog's
  // error is not retryable, or its retried times has exceeded the maximum, its
  // status will be updated to ERROR and at the same time this field will set
  // the value in the Task configuration.
  IGNORED_FAILURE: 'ignoredFailure',
}

/**
 * @typedef {{
 *   taskId:string,
 *   parameters:(string|undefined),
 *   status:!TaskLogStatus,
 *   needCheck:boolean,
 *   parentId:(string|undefined),
 *   embeddedTag:(string|undefined),
 *   createTime:!Date,
 *   startTime:(!Date|undefined),
 *   prefinishTime:(!Date|undefined),
 *   finishTime:(!Date|undefined),
 *   error:(string|undefined),
 *   retriedTimes:(number|undefined),
 *   ignoredFailure:(boolean|undefined),
 *   jobId:(string|undefined),
 *   next:(TaskGroup|undefined),
 * }}
 */
let TaskLog;

/**
 * Base Task Log data access class on Firestore. The underlying transactions
 * in native mode and datastore mode are different, so there will be two sub-
 * classes for these two different modes.
 */
class TaskLogDao extends DataAccessObject {

  /**
   * Initializes TaskLog Dao instance.
   * @param {!DataSource} dataSource The data source type.
   * @param {string} namespace The namespace of the data.
   */
  constructor(dataSource, namespace = 'sentinel') {
    super('TaskLog', namespace, dataSource);
  }

  /**
   * Creates a new TaskLog entity in database as the start of the task.
   * To prevent create duplicated task for the same taskLogId, it will be
   * executed in a transaction.
   * @param {(string|number)} taskLogId
   * @param {!Entity=} taskLogEntity
   * @return {!Promise<boolean>} Whether successfully starts the task.
   */
  startTask(taskLogId, taskLogEntity = {}) {
    const toCreateEntity = {
      createTime: new Date(),
      status: TaskLogStatus.INITIAL,
      ...taskLogEntity,
    };
    return this.runTransaction(
        this.wrapInTransaction(taskLogId,
            this.getStartTaskOperation(taskLogId, toCreateEntity)))
        .catch((error) => {
          console.log(`Error in startTask ${taskLogId}. Reason:`, error);
          return false;
        });
  }

  /**
   * Merges properties from 'Task.start()' to TaskLog.
   * The whole process of staring a Task is invoked by Sentinel.startTask_() in
   * following steps:
   * 1. TaskLogDao.startTask() makes sure it's ready to start;
   * 2. Task.start() to start the job and returns the job information related
   *    properties;
   * 3. This function to save the properties to TaskLog.
   * In this process, this function is always invoked after
   * 'TaskLogDao.startTask()', so it doesn't need to be run in a transaction.
   *
   * @param {(string|number)} taskLogId
   * @param {!Entity=} taskLogEntity
   * @return {!Promise<string|number>} The ID of updated document/entity.
   */
  afterStart(taskLogId, taskLogEntity = {}) {
    return this.merge({
      startTime: new Date(),
      status: TaskLogStatus.STARTED,
      ...taskLogEntity,
    }, taskLogId);
  }

  /**
   * Finishes the task by updating the TaskLog entity with given data.
   * @param {(string|number)} taskLogId
   * @param {!Entity=} taskLogEntity
   * @return {Promise<boolean>}  Whether successfully finishes the task.
   */
  finishTask(taskLogId, taskLogEntity = {}) {
    const toFinishEntity = {
      prefinishTime: new Date(),
      status: TaskLogStatus.FINISHING,
      ...taskLogEntity,
    };
    return this.runTransaction(
        this.wrapInTransaction(taskLogId,
            this.getFinishTaskOperation(taskLogId, toFinishEntity)))
        .catch((error) => {
          console.log(`Error in finishTask ${taskLogId}. Reason:`, error);
          return false;
        });
  }

  /**
   * Merges properties from 'Task.finish()' to TaskLog.
   * The whole process of finishing a Task is invoked by Sentinel.finishTask_()
   * in following steps:
   * 1. TaskLogDao.finishTask() makes sure it's ready to finish;
   * 2. Task.finish() to finish the job and returns the job related properties
   *    if any;
   * 3. This function to save the properties to TaskLog.
   * In this process, this function is always invoked after
   * 'TaskLogDao.finishTask()', so it doesn't need to be run in a transaction.
   *
   * @param {(string|number)} taskLogId
   * @param {!Entity=} taskLogEntity
   * @return {!Promise<string|number>} The ID of updated document/entity.
   */
  afterFinish(taskLogId, taskLogEntity = {}) {
    return this.merge({
      finishTime: new Date(),
      status: TaskLogStatus.FINISHED,
      ...taskLogEntity,
    }, taskLogId);
  }

  /**
   * Saves the error status and message to TaskLog.
   * @param {(string|number)} taskLogId
   * @param {(!Error|object)} error
   * @param {(boolean)} ignored Whether this error can be ignored. Ignored task
   *     will continue its next tasks.
   * @return {!Promise<string|number>}
   */
  saveErrorMessage(taskLogId, error, ignored = false) {
    const errorMessage = error instanceof Error ? error.toString()
        : JSON.stringify(error);
    return this.merge({
      [FIELD_NAMES.REGULAR_CHECK]: false,
      finishTime: new Date(),
      status: TaskLogStatus.ERROR,
      error: errorMessage,
      [FIELD_NAMES.IGNORED_FAILURE]: ignored,
    }, taskLogId);
  }

  /**
   * Starts the task in a transaction. This function needs to be implemented in
   * subclasses for different Firestore mode.
   * @param {(string|number)} taskLogId
   * @param {!Entity} taskLogEntity
   * @return {!TransactionOperation}
   * @private
   */
  getStartTaskOperation(taskLogId, taskLogEntity) {
    return (documentSnapshot, documentReference, transaction) => {
      if (!documentSnapshot.exists) {
        console.log(`Create the TaskLog for [${taskLogId}].`);
        transaction.create(documentReference, taskLogEntity);
        return true;
      }
      console.log(`The TaskLog for [${taskLogId}] exist. Quit.`);
      return false;
    };
  }

  /**
   * Finishes the task in a transaction. This function needs to be implemented
   * in subclasses for different Firestore mode.
   * @param {(string|number)} taskLogId
   * @param {!Entity} taskLogEntity
   * @return {!TransactionOperation}
   * @private
   */
  getFinishTaskOperation(taskLogId, taskLogEntity) {
    return (documentSnapshot, documentReference, transaction) => {
      if (!documentSnapshot.exists) {
        console.log(`The TaskLog[${taskLogId}] doesn't exist. Quit.`);
        return false;
      }
      const status = documentSnapshot.get('status');
      if (status !== TaskLogStatus.STARTED) {
        console.log(`TaskLog[${taskLogId}]'s status is ${status}. Quit.`);
        return false;
      }
      console.log(`Finish the TaskLog for [${taskLogId}].`);
      transaction.update(documentReference, taskLogEntity);
      return true;
    };
  }
}

module.exports = {
  TaskLogStatus,
  TaskLog,
  TaskLogDao,
  FIELD_NAMES,
};
