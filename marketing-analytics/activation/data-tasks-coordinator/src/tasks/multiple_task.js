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

/** @fileoverview Multiple Task class file. */

'use strict';
const {nanoid} = require('nanoid');
const {
  utils: {apiSpeedControl, getProperValue,},
  storage: {StorageFile},
} = require('@google-cloud/nodejs-common');
const {
  TaskType,
  TaskGroup,
  StorageFileConfig,
  ErrorOptions,
} = require('../task_config/task_config_dao.js');
const {FIELD_NAMES} = require('../task_log/task_log_dao.js');
const {KnotTask} = require('./knot_task.js');

/**
 * `estimateRunningTime` Number of minutes to wait before doing the first task
 * status check for the embedded task group.
 * `dueTime` Task execution timeout time in minutes. If task group times out, it
 * will be marked with 'timeout' error code.
 * @typedef {{
 *   type:TaskType.MULTIPLE,
 *   appendedParameters:(Object<string,string>|undefined),
 *   source:{
 *     csv:{header:string, records:string,},
 *   }|{
 *     file:!StorageFileConfig,
 *   },
 *   destination:{
 *     taskId:string,
 *     target:'pubsub',
 *     qps:number|undefined,
 *   }|{
 *     taskId:string,
 *     target:'gcs',
 *     file:!StorageFileConfig,
 *   },
 *   multiple:{
 *     estimateRunningTime:(number|undefined),
 *     dueTime:(number|undefined),
 *   }|undefined,
 *   errorOptions:(!ErrorOptions|undefined),
 *   next:(!TaskGroup|undefined),
 * }}
 */
let MultipleTaskConfig;

/**
 * Multiple Task acts as a holder of multiple same tasks (with different
 * parameters). Some tasks will generate multiple instances of next tasks, e.g.
 * a Google Ads Report task will generate report tasks for each of the Ads
 * accounts.
 * There are two ways (different values for 'destination.target' in
 * `MultipleTaskConfig`) to start those multiple tasks:
 *  1. Directly send start task messages to Pub/Sub. This is a simple and
 *     straightforward approach. However due to Cloud Functions' execution time,
 *     the distribution time of multiple tasks couldn't be longer than 9
 *     minutes.
 *  2. Through Tentacles' Pub/Sub integration to send the start messages in a
 *     more manageable manner. This will require extra settings in Tentacles.
 *
 * Either way, after all these start task messages are sent, this task will act
 * as the holder of all those multiple tasks and check the status of them.
 */
class MultipleTask extends KnotTask {

  /** @override */
  isManualAsynchronous() {
    return this.parameters.numberOfTasks > 0;
  }

  /** @override */
  async doTask() {
    let records;
    if (this.config.source.file) {
      records = await this.getRecordsFromFile_(this.config.source.file);
    } else if (this.config.source.csv) {
      records = this.getRecordsFromCsv_(this.config.source.csv);
    } else {
      console.error('Unknown source for Multiple Task', this.config.source);
      throw new Error('Unknown source for Multiple Task');
    }
    console.log(records);
    const numberOfTasks = records.length;
    if (numberOfTasks === 0) {
      return {parameters: this.appendParameter({numberOfTasks})};
    }
    const multipleTag = nanoid();
    if (this.config.destination.target === 'pubsub') {
      await this.startThroughPubSub_(multipleTag, records);
    } else {
      await this.startThroughStorage_(multipleTag, records);
    }
    return {
      parameters: this.appendParameter({
        [FIELD_NAMES.MULTIPLE_TAG]: multipleTag,
        numberOfTasks,
        startTime: Date.now(),
      }),
    };
  }

  /**
   * Starts multiple tasks by sending out task start messages.
   * @param {string} tag The multiple task tag to mark task instances.
   * @param {!Array<string>} records Array of multiple task instances data. Each
   *     element is a JSON string of the 'appendedParameters' object for an task
   *     instance.
   * @return {!Promise<boolean>} Whether messages are all sent out successfully.
   * @private
   */
  startThroughPubSub_(tag, records) {
    const qps = getProperValue(this.config.destination.qps, 1, false);
    const managedSend = apiSpeedControl(1, 1, qps);
    const sendSingleMessage = async (lines, batchId) => {
      if (lines.length !== 1) {
        throw Error('Wrong number of Pub/Sub messages.');
      }
      try {
        await this.taskManager.startTasks(
            this.config.destination.taskId, {[FIELD_NAMES.MULTIPLE_TAG]: tag},
            JSON.stringify(
                Object.assign({}, this.parameters, JSON.parse(lines[0]))));
        return true;
      } catch (error) {
        console.error(`Pub/Sub message[${batchId}] failed.`, error);
        return false;
      }
    };
    return managedSend(sendSingleMessage, records, `multiple_task_${tag}`);
  }

  /**
   * Get records from a given Cloud Storage file.
   * @param {!StorageFileConfig} sourceFile
   * @return {Promise<!Array<string>>} Array of record (JSON string).
   * @private
   */
  async getRecordsFromFile_(sourceFile) {
    /** @const {StorageFile} */
    const storageFile = StorageFile.getInstance(
        sourceFile.bucket, sourceFile.name,
        {projectId: this.getCloudProject(sourceFile)});
    const records = (await storageFile.loadContent()).split('\n')
        .filter((record) => !!record); // filter out empty lines.
    return records;
  }

  /**
   * Get records from a given Csv setting.
   * @param {{
   *       header:string,
   *       records:string,
   *     }} csv setting
   * @return {Array<string>} Array of record (JSON string).
   * @private
   */
  getRecordsFromCsv_({header, records}) {
    const fields = header.split(',').map((field) => field.trim());
    return records.split('\n').map((line) => {
      const record = {}
      line.split(',').forEach((value, index) => {
        record[fields[index]] = value.trim();
      });
      return JSON.stringify(record);
    });
  }

  /**
   * Writes out a file for Tentacles to start multiple tasks.
   * @param {string} tag The multiple task tag to mark task instances.
   * @param {!Array<string>} records Array of multiple task instances data. Each
   *     element is a JSON string of the 'appendedParameters' object for an task
   *     instance.
   * @private
   */
  async startThroughStorage_(tag, records) {
    const content = records.map((record) => {
      return JSON.stringify(Object.assign(
          {},
          this.parameters,
          JSON.parse(record),
          {
            taskId: this.config.destination.taskId,
            [FIELD_NAMES.MULTIPLE_TAG]: tag,
          }));
    }).join('\n');
    /** @type {StorageFileConfig} */
    const destination = this.config.destination.file;
    const outputFile = StorageFile.getInstance(
        destination.bucket, destination.name, {
          projectId: destination.projectId,
          keyFilename: destination.keyFilename,
        });
    await outputFile.getFile().save(content);
  }

  /**
   * Return true if the instance number is 0.
   * Otherwise return false if there is a 'estimateRunningTime' and the time is
   * not over due; throw an out of time error if there is a 'dueTime' and the
   * time is over due; or check the status of multiple tasks.
   *
   * @override
   */
  async isDone() {
    if (this.parameters.numberOfTasks === 0) return true;
    const multipleConfig = this.config.multiple || {};
    const shouldCheck = this.shouldCheckTaskStatus(multipleConfig);
    const filter = [{
      property: FIELD_NAMES.MULTIPLE_TAG,
      value: this.parameters[FIELD_NAMES.MULTIPLE_TAG],
    }];
    return shouldCheck
        ? this.areTasksFinished(filter, this.parameters.numberOfTasks)
        : false;
  }
}

module.exports = {
  MultipleTaskConfig,
  MultipleTask,
}
