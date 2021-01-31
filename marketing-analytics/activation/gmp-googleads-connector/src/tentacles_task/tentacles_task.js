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
 * @fileoverview Interface for operations of Tentacles Task.
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
 *   status:(!TaskStatus|undefined),
 *   topic:(string|undefined),
 * }}
 */
let TentaclesTaskEntity;

/**
 * Tentacles Task interface to log tasks.
 * @interface
 */
class TentaclesTask {
  /**
   * Creates the task Entity based on the given information.
   * @param {!TentaclesTaskEntity} entitySkeleton Task information.
   * @return {!Promise<string|number>} Created Task ID.
   */
  createTask(entitySkeleton) {}

  /**
   * Saves the Pub/Sub message ID in Tentacles Task.
   * @param {string|number|undefined} id Tentacles Task ID.
   * @param {!Object<string,string>} taskData Task data that will be updated
   *     into the Task.
   * @return {!Promise<boolean>} Whether updates successfully.
   */
  updateTask(id, taskData) {}

  /**
   * Checks whether this message has been handled or not. If not, updates its
   * status to 'SENDING' and returns true. Pub/Sub occasionally sends duplicated
   * messages. If it happened, the status of the duplicated Task will be wrong
   * and fail the check here, hence no task will be executed repeatedly.
   * @param {string|number|undefined} id Tentacles Task ID.
   * @return {!Promise<boolean>} Whether this task started successfully.
   */
  start(id) {}

  /**
   * Finishes a task by updating the status 'DONE'.
   * @param {string|number|undefined} id Tentacles Task ID.
   * @param {boolean} status Whether this task succeeded.
   * @return {!Promise<boolean>} Whether this task finished successfully.
   */
  finish(id, status) {}

  /**
   * Logs error message in the Task.
   * @param {string|number|undefined} id Tentacles Task ID.
   * @param {!Error} error Error which is happened in processing the task.
   * @return {!Promise<boolean>} Whether this error is logged successfully.
   */
  logError(id, error) {}
}

module.exports = {
  TaskStatus,
  TentaclesTaskEntity,
  TentaclesTask,
};
