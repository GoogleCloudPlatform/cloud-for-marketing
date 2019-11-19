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
 * @fileoverview Task Log implementation class on Firestore Native mode.
 */

'use strict';

const {FirestoreAccessBase: {Transaction}} = require('nodejs-common');
const {TaskLogDao, TaskLogStatus} = require('./task_log_dao.js');

/**
 * Task Log data access object on Firestore Native mode.
 */
class TaskLogDatastore extends TaskLogDao {
  /** @override */
  startTask(taskLogId, taskLogEntity) {
    /**
     * Starts the task based on coming message in a transaction mode to avoid
     * duplicated messages.
     * @param {!Transaction} transaction Transaction object which passed to a
     *     transaction's updateFunction provides the methods to read and write
     *     data within the transaction context.
     * @return {!Promise<boolean>} Whether successfully starts the task.
     */
    const startTaskTransaction = (transaction) => {
      console.log(`Start to get TaskLog for ${taskLogId}`);
      const key = this.accessObject.getKey(taskLogId);
      return transaction.get(key).then(([taskLog]) => {
        const needToCreate = !taskLog;
        if (needToCreate) {
          transaction.save([{key: key, data: taskLogEntity}]);
        } else {
          // Transaction in Datastore needs 'commit' to quit properly. So here
          // can't return directly.
          console.log(`There is already a task exists for [${taskLogId}].`);
        }
        return transaction.commit()
            .then(() => {
              console.log(`Create the TaskLog for [${taskLogId}].`);
              return needToCreate;
            })
            .catch((error) => {
              console.log(
                  `Error in starting the TaskLog [${taskLogId}]. Reason:`,
                  error.message);
              return false;
            });
      });
    };
    return this.accessObject.runTransaction(startTaskTransaction);
  }

  /** @override */
  finishTask(taskLogId, taskLogEntity) {
    /**
     * Finishes the task based on coming message in a transaction mode to avoid
     * duplicated messages.
     * @param {!Transaction} transaction Transaction object which passed to a
     *     transaction's updateFunction provides the methods to read and write
     *     data within the transaction context.
     * @return {!Promise<boolean>} Whether successfully finishes the task.
     */
    const finishTaskTransaction = (transaction) => {
      console.log(`Start to get TaskLog for ${taskLogId}`);
      const key = this.accessObject.getKey(taskLogId);
      return transaction.get(key).then(([taskLog]) => {
        const readyToFinish = taskLog && taskLog.status === TaskLogStatus.STARTED;
        if (readyToFinish) {
          transaction.save([{key: key, data: taskLogEntity}]);
        } else {
          console.log(`The TaskLog can't be finished. Quit.`, taskLog);
        }
        return transaction.commit()
            .then(() => {
              console.log(`Finish the TaskLog for [${taskLogId}].`);
              return readyToFinish;
            })
            .catch((error) => {
              console.log(
                  `Error in finishing the TaskLog [${taskLogId}]. Reason:`,
                  error.message);
              return false;
            });
      })
    };
    return this.accessObject.runTransaction(finishTaskTransaction);
  }
}

exports.TaskLogDatastore = TaskLogDatastore;