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

const {base: {BaseDao}} = require('nodejs-common');

/**
 * Status of task logs.
 * @enum {string}
 */
const TaskLogStatus = {
  INITIAL: 'INITIAL',
  STARTED: 'STARTED',
  FINISHED: 'FINISHED',
  ERROR: 'ERROR',
};
exports.TaskLogStatus = TaskLogStatus;

/**
 * @typedef {{
 *   taskId:string,
 *   jobId:(string|undefined),
 *   parameters:(string|undefined),
 *   previousJobId:(string|undefined),
 *   status:!TaskLogStatus,
 *   createTime:!Date,
 *   startTime:(!Date|undefined),
 *   finishTime:(!Date|undefined),
 *   error:(string|undefined),
 * }}
 */
let TaskLog;
exports.TaskLog = TaskLog;

/**
 * Task Log data access object on Firestore.
 */
class TaskLogDao extends BaseDao {

  constructor() {
    super('TaskLog', 'sentinel');
  }
}

exports.TaskLogDao = TaskLogDao;
