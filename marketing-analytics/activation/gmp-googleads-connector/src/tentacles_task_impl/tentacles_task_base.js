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
 * @fileoverview Tentacles Task base class.
 */

'use strict';

/**
 * Status of Tentacles Task.
 * @enum {string}
 */
const TaskStatus = {
  QUEUING: 'queuing',
  SENDING: 'sending',
  DONE: 'done',
  FAILED: 'failed',
  ERROR: 'error',
};

exports.TaskStatus = TaskStatus;

/**
 * @typedef{{
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
 *   status:(!TaskStatus|undefined),
 *   topic:(string|undefined),
 * }}
 */
let TentaclesTaskEntity;

exports.TentaclesTaskEntity = TentaclesTaskEntity;

/**
 * Tentacles Task implementation base class. Without external data source, it
 * just writes log messages.
 */
class TentaclesTaskBase {
  /**
   * Creates the task Entity based on the given information.
   * @param {!TentaclesTaskEntity} entitySkeleton Task information.
   * @return {!Promise<string|number>} Created Task ID.
   */
  create(entitySkeleton) {
    console.log('[Dummy Task saver]: ', entitySkeleton);
    return Promise.resolve(`NOT_SAVED_TASK`);
  }

  /**
   * Saves the Pub/Sub message ID in Tentacles Task.
   * @param {string|number|undefined} id Tentacles Task ID.
   * @param {!Object<string,string>} taskData Task data that will be updated
   *     into the Task.
   * @return {!Promise<boolean>} Whether updates successfully.
   */
  update(id, taskData) {
    return this.getPromisedTrueForDummy_(
        `Update Task[${id}] with ${JSON.stringify((taskData))}.`);
  }

  /**
   * Checks whether this message has been handled or not. If not, updates its
   * status to 'SENDING' and returns true. Pub/Sub occasionally sends duplicated
   * messages. If it happened, the status of the duplicated Task will be wrong
   * and fail the check here, hence no task will be executed repeatedly.
   * @param {string|number|undefined} id Tentacles Task ID.
   * @return {!Promise<boolean>} Whether this task started successfully.
   */
  start(id) {
    return this.getPromisedTrueForDummy_(`Start Task[${id}].`);
  }

  /**
   * Finishes a task by updating the status 'DONE'.
   * @param {string|number|undefined} id Tentacles Task ID.
   * @param {boolean} status Whether this task succeeded.
   * @return {!Promise<boolean>} Whether this task finished successfully.
   */
  finish(id, status) {
    return this.getPromisedTrueForDummy_(`Finish Task[${id}].`);
  }

  /**
   * Logs error message in the Task.
   * @param {string|number|undefined} id Tentacles Task ID.
   * @param {!Error} error Error which is happened in processing the task.
   * @return {!Promise<boolean>} Whether this error is logged successfully.
   */
  logError(id, error) {
    return this.getPromisedTrueForDummy_(
        `Error in Task[${id}]: ${JSON.stringify((error))}.`);
  }

  /**
   * Returns a Promise<true> for the fallback implementation.
   * In other implementation with a real database, some of those operations will
   * return false due to real situation. For example, duplicated messages will
   * trigger the same task for more than once and a database-enabled task
   * implementation can handle that.
   * But if you prefer a simple deployment (no database required) and don't care
   * about the possible duplication (e.g. in uploading audience list,
   * duplication won't hurt anything), the solution can use this kinda of
   * 'dummy' implementation which just output the logs and returns 'true' to all
   * kinds of operations.
   * @param {string} message A message string for log.
   * @return {!Promise<boolean>}
   * @private
   */
  getPromisedTrueForDummy_(message) {
    console.log(`[Dummy Task saver] ${message}: always returns TRUE.`);
    return Promise.resolve(true);
  }
}

exports.TentaclesTaskBase = TentaclesTaskBase;
