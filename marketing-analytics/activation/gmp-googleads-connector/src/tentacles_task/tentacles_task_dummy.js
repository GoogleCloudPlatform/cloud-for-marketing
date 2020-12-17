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
 * @fileoverview Tentacles Task empty implementation.
 */

'use strict';

const {TentaclesTask} = require('./tentacles_task.js');

/**
 * Tentacles Task implementation with no extra data source, only prints logs.
 * @implements {TentaclesTask}
 */
class TentaclesTaskDummy {

  /** @override */
  createTask(entitySkeleton) {
    console.log('[Dummy Task saver]: ', entitySkeleton);
    return Promise.resolve(`NOT_SAVED_TASK`);
  }

  /** @override */
  updateTask(id, taskData) {
    return this.getPromisedTrueForDummy_(
        `Update Task[${id}] with ${JSON.stringify((taskData))}.`);
  }

  /** @override */
  start(id) {
    return this.getPromisedTrueForDummy_(`Start Task[${id}].`);
  }

  /** @override */
  finish(id, status) {
    return this.getPromisedTrueForDummy_(`Finish Task[${id}].`);
  }

  /** @override */
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
   * But if the user prefer a simple deployment (no database required) and don't
   * care about the possible duplication (e.g. in uploading audience list,
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

module.exports = {TentaclesTaskDummy};
