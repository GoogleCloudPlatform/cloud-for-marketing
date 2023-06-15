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
  firestore: { DataSource, Entity, DataAccessObject, DatastoreDocumentFacade },
  utils: { getLogger }
} = require('@google-cloud/nodejs-common');
const {TaskStatus, TentaclesTask} = require('./tentacles_task.js');

/**
 * Returns an operation on the specified task.
 * This function is used for Task changing to next status.
 * @typedef {function(!DatastoreDocumentFacade):!Promise<!Entity|undefined>}
 */
let TaskOperation;

/** @const{number} Default process times. It will retry 3 times by default. */
const DEFAULT_PROCESS_TIMES = 4;

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
    // This logger is used to send data to BigQuery for the dashboard.
    this.loggerForDashboard = getLogger('TentaclesTask');
    // This logger is for regular logging.
    this.logger = getLogger('T.TASK');
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
   * Carries out the operation in a transaction and returns whether the
   * operation is successful. It checks the existence of the'Task' before
   * updating it with the value that is returned by the function `operationFn`.
   * @param {(string|number)} id Tentacles Task Id.
   * @param {!TaskOperation} operationFn
   * @return {!Promise<boolean>} Whether the operation is successful.
   * @private
   */
  async operateInTransaction_(id, operationFn) {
    if (!id) {
      this.logger.info('Empty Task ID (test mode), always returns TRUE.');
      return true;
    }
    try {
      const transactionOperation =
        (documentSnapshot, documentReference, transaction) => {
          if (!documentSnapshot.exists) {
            this.logger.warn(`Can't find the Task[${id}]. Quit.`);
            return false;
          }
          const taskEntity = operationFn(documentSnapshot);
          const status = documentSnapshot.get('status');
          if (!taskEntity) {
            this.logger.warn(
              `Fail to ${operationFn.name} Task[${id}] status[${status}].`);
            return false;
          }
          transaction.update(documentReference, taskEntity);
          this.logger.debug(`${operationFn.name} task[${id}] status[${status}].`);
          this.loggerForDashboard.info(JSON.stringify(
            Object.assign({ id, status, action: operationFn.name }, taskEntity)
          ));
          return true;
        };
      const transactionFunction = this.wrapInTransaction(id, transactionOperation);
      return await this.runTransaction(transactionFunction);
    } catch (error) {
      this.logger.error(error);
      return false;
    }
  }

  /** @override */
  async createTask(entitySkeleton) {
    const entity = Object.assign(
        {}, entitySkeleton,
        {status: TaskStatus.QUEUING, createdAt: new Date()});
    const id = await this.create(entity);
    this.loggerForDashboard.info(
        JSON.stringify(Object.assign({action: 'createTask', id}, entity)));
    return id;
  }

  /** @override */
  updateTask(id, data) {
    /** @type {!TaskOperation} */
    const appendInfo = () => data;
    return this.operateInTransaction_(id, appendInfo);
  }

  /** @override */
  transport(id) {
    /** @type {!TaskOperation} */
    const transport = (documentSnapshot) => {
      const status = documentSnapshot.get('status');
      if (status === TaskStatus.QUEUING) {
        return {
          status: TaskStatus.TRANSPORTED,
          transportedTime: new Date(),
        };
      }
    };
    return this.operateInTransaction_(id, transport);
  }

  /** @override */
  async start(id) {
    // Errors can't be passed out of a transaction, so use it to hold errors.
    const errors = [];
    /** @type {!TaskOperation} */
    const startTask = (documentSnapshot) => {
      const status = documentSnapshot.get('status');
      const processedTimes = documentSnapshot.get('processedTimes') || 0;
      if (status === TaskStatus.TRANSPORTED && processedTimes === 0) {
        return {
          status: TaskStatus.SENDING,
          startSending: new Date(),
          processedTimes: 1,
        };
      } else if (status === TaskStatus.SENDING) { // 'retry'
        if (processedTimes < DEFAULT_PROCESS_TIMES) {
          return {
            status: TaskStatus.SENDING,
            startSending: new Date(),
            processedTimes: processedTimes + 1,
          };
        }
        errors.push(
          `Task[${id}] has been processed ${DEFAULT_PROCESS_TIMES} times.`
        );
      }
    };
    const result = await this.operateInTransaction_(id, startTask);
    if (errors.length > 0) throw new Error(errors.join('|'));
    return result;
  }

  /** @override */
  finish(id, taskDone) {
    /** @type {!TaskOperation} */
    const finish = (documentSnapshot) => {
      const status = documentSnapshot.get('status');
      if (status === TaskStatus.SENDING) {
        return {
          status: taskDone ? TaskStatus.DONE : TaskStatus.FAILED,
          finishedTime: new Date(),
        };
      }
    };
    return this.operateInTransaction_(id, finish);
  }

  /** @override */
  logError(id, error) {
    /** @type {!TaskOperation} */
    const saveErrorMessage = (task) => {
      this.logger.warn(`Error in this task[${id}]: `, error);
      return {
        status: TaskStatus.ERROR,
        finishedTime: new Date(),
        error: error.toString(),
      };
    };
    return this.operateInTransaction_(id, saveErrorMessage);
  }
}

exports.TentaclesTaskOnFirestore = TentaclesTaskOnFirestore;
