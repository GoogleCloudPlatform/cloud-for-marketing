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
  firestore: {
    Database,
    DataSource,
    Entity,
    DataAccessObject,
    DatastoreDocumentFacade,
  },
  utils: { getLogger }
} = require('@google-cloud/nodejs-common');

/**
 * Status of Tentacles Task.
 * @enum {string}
 */
const TentaclesTaskStatus = {
  QUEUING: 'queuing',
  TRANSPORTED: 'transported',
  SENDING: 'sending',
  DONE: 'done',
  FAILED: 'failed',
  ERROR: 'error',
};

/**
 * @typedef {{
 *   api:string,
 *   apiMessageId:(string|undefined),
 *   config:string,
 *   createdAt:(!Date|undefined),
 *   dataMessageId:(string|undefined),
 *   dryRun:string,
 *   end:(string|undefined),
 *   error:(string|undefined),
 *   fileId:(string|number),
 *   finishedTime:(!Date|undefined),
 *   gcs:(string|undefined),
 *   size:(string|undefined),
 *   slicedFile:(string|undefined),
 *   start:(string|undefined),
 *   startSending:(!Date|undefined),
 *   status:(!TentaclesTaskStatus|undefined),
 *   topic:(string|undefined),
 * }}
 */
let TentaclesTask;

/**
 * Returns an operation on the specified task.
 * This function is used for Task changing to next status.
 * @typedef {function(!DatastoreDocumentFacade):!Promise<!Entity|undefined>}
 */
let TaskOperation;

/** @const{number} Default process times. It will retry 3 times by default. */
const DEFAULT_PROCESS_TIMES = 4;

/**
 * Tentacles Task data access object.
 */
class TentaclesTaskDao extends DataAccessObject {

  /**
   * Initializes TentaclesTask Dao instance.
   * @param {!Database} database The database.
   * @param {string} namespace The namespace of the data.
   */
  constructor(database, namespace = 'tentacles') {
    super('Task', namespace, database);
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

  /**
   * Creates the task Entity based on the given information.
   * @param {!TentaclesTask} entitySkeleton Task information.
   * @return {!Promise<string|number>} Created Task ID.
   */
  async createTask(entitySkeleton) {
    const entity = Object.assign(
      {}, entitySkeleton,
      { status: TentaclesTaskStatus.QUEUING, createdAt: new Date() });
    const id = await this.create(entity);
    this.loggerForDashboard.info(
      JSON.stringify(Object.assign({ action: 'createTask', id }, entity)));
    return id;
  }

  /**
   * Saves the Pub/Sub message ID in Tentacles Task.
   * @param {string|number|undefined} id Tentacles Task ID.
   * @param {!Object<string,string>} taskData Task data that will be updated
   *     into the Task.
   * @return {!Promise<boolean>} Whether updates successfully.
   */
  updateTask(id, data) {
    /** @type {!TaskOperation} */
    const appendInfo = () => data;
    return this.operateInTransaction_(id, appendInfo);
  }

  /**
   * Checks whether this message has been transported or not. If not, transport
   * it and changes its status to 'TRANSPORTED' and returns true.
   * @param {string|number|undefined} id Tentacles Task ID.
   * @return {!Promise<boolean>} Whether this task is transported successfully.
   */
  transport(id) {
    /** @type {!TaskOperation} */
    const transport = (documentSnapshot) => {
      const status = documentSnapshot.get('status');
      if (status === TentaclesTaskStatus.QUEUING) {
        return {
          status: TentaclesTaskStatus.TRANSPORTED,
          transportedTime: new Date(),
        };
      }
    };
    return this.operateInTransaction_(id, transport);
  }

  /**
   * Checks whether this message has been handled or not. If not, updates its
   * status to 'SENDING' and returns true. Pub/Sub occasionally sends duplicated
   * messages. If it happened, the status of the duplicated Task will be wrong
   * and fail the check here, hence no task will be executed repeatedly.
   * @param {string|number|undefined} id Tentacles Task ID.
   * @return {!Promise<boolean>} Whether this task started successfully.
   */
  async start(id) {
    // Errors can't be passed out of a transaction, so use it to hold errors.
    const errors = [];
    /** @type {!TaskOperation} */
    const startTask = (documentSnapshot) => {
      const status = documentSnapshot.get('status');
      const processedTimes = documentSnapshot.get('processedTimes') || 0;
      if (status === TentaclesTaskStatus.TRANSPORTED && processedTimes === 0) {
        return {
          status: TentaclesTaskStatus.SENDING,
          startSending: new Date(),
          processedTimes: 1,
        };
      } else if (status === TentaclesTaskStatus.SENDING) { // 'retry'
        if (processedTimes < DEFAULT_PROCESS_TIMES) {
          return {
            status: TentaclesTaskStatus.SENDING,
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

  /**
   * Finishes a task by updating the status 'DONE'.
   * @param {string|number|undefined} id Tentacles Task ID.
   * @param {boolean} taskDone Whether this task succeeded.
   * @return {!Promise<boolean>} Whether this task finished successfully.
   */
  finish(id, taskDone) {
    /** @type {!TaskOperation} */
    const finish = (documentSnapshot) => {
      const status = documentSnapshot.get('status');
      if (status === TentaclesTaskStatus.SENDING) {
        return {
          status: taskDone ? TentaclesTaskStatus.DONE : TentaclesTaskStatus.FAILED,
          finishedTime: new Date(),
        };
      }
    };
    return this.operateInTransaction_(id, finish);
  }

  /**
   * Logs error message in the Task.
   * @param {string|number|undefined} id Tentacles Task ID.
   * @param {!Error} error Error which is happened in processing the task.
   * @return {!Promise<boolean>} Whether this error is logged successfully.
   */
  logError(id, error) {
    /** @type {!TaskOperation} */
    const saveErrorMessage = (task) => {
      this.logger.warn(`Error in this task[${id}]: `, error);
      return {
        status: TentaclesTaskStatus.ERROR,
        finishedTime: new Date(),
        error: error.toString(),
      };
    };
    return this.operateInTransaction_(id, saveErrorMessage);
  }
}

module.exports = {
  TentaclesTaskStatus,
  TentaclesTask,
  TentaclesTaskDao,
};
