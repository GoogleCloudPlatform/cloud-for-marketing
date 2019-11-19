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
 * @fileoverview Task Log implementation class on Firestore native mode.
 */

'use strict';

const {TaskLogDao, TaskLogStatus} = require('./task_log_dao.js');

/**
 * Task Log data access object on Firestore.
 */
class TaskLogFirestore extends TaskLogDao {

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
      let documentReference = this.accessObject.getDocumentReference(taskLogId);
      return transaction.get(documentReference)
          .then((documentSnapshot) => {
            if (!documentSnapshot.exists) {
              console.log(`Create the TaskLog for [${taskLogId}].`);
              transaction.create(documentReference, taskLogEntity);
              return true;
            } else {
              console.log(`The TaskLog for [${taskLogId}] exist. Quit.`);
              return false;
            }
          })
          .catch((error) => {
            console.log(
                `Error in creating the lock [${taskLogId}]. Reason:`, error);
            return false;
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
      let documentReference = this.accessObject.getDocumentReference(taskLogId);
      return transaction.get(documentReference)
          .then((documentSnapshot) => {
            if (documentSnapshot.exists) {
              const currentStatus = documentSnapshot.get('status');
              if (currentStatus === TaskLogStatus.STARTED) {
                console.log(`Finish the TaskLog for [${taskLogId}].`);
                transaction.update(documentReference, taskLogEntity);
                return true;
              } else {
                console.log(`TaskLog[${taskLogId}]'s status is ${currentStatus}. Quit.`);
              }
            } else {
              console.log(`The TaskLog[${taskLogId}] doesn't exist. Quit.`);
            }
            return false;
          })
          .catch((error) => {
            console.log(
                `Error in finish the lock [${taskLogId}]. Reason:`, error);
            return false;
          });
    };
    return this.accessObject.runTransaction(finishTaskTransaction);
  }
}

exports.TaskLogFirestore = TaskLogFirestore;
