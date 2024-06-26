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
const lodash = require('lodash');
const {nanoid} = require('nanoid');
const {
  utils: { apiSpeedControl, getProperValue, replaceParameters, requestWithRetry },
  storage: {StorageFile},
} = require('@google-cloud/nodejs-common');
const {
  TaskType,
  TaskGroup,
  StorageFileConfig,
  ErrorOptions,
} = require('../task_config/task_config_dao.js');
const {FIELD_NAMES} = require('../task_log/task_log_dao.js');
const { KnotTask } = require('./knot_task.js');
const {
  TentaclesFileOption,
  getRequestOption,
  getFileUrl,
} = require('./internal/speed_controlled_task.js');

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
 *     recordSize: number|undefined,
 *     message: object|undefined,
 *   }|{
 *     taskId:string,
 *     target:'gcs',
 *     recordSize: number|undefined,
 *     file:(!StorageFileConfig|undefined),
 *     http:(!TentaclesFileOption|undefined),
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

  /**
   * @override
   * This task can have un-replaced parameters as it will start other tasks, so
   * it is possible that those parameters will be replaced with given data for
   * the target tasks..
   */
  setConfig_() {
    this.config = JSON.parse(
      replaceParameters(this.originalConfigString, this.parameters, true));
  }

  /** @override */
  async doTask() {
    let records;
    if (this.config.source.file) {
      records = await this.getRecordsFromFile_(this.config.source.file);
    } else if (this.config.source.csv) {
      records = this.getRecordsFromCsv_(this.config.source.csv);
    } else {
      this.logger.error('Unknown source for Multiple Task', this.config.source);
      throw new Error('Unknown source for Multiple Task');
    }
    if (records.length === 0) {
      return { parameters: this.appendParameter({ numberOfTasks: 0 }) };
    }
    this.recordSize = getProperValue(this.config.destination.recordSize, 1, false);
    let numberOfTasks;
    const multipleTag = nanoid();
    if (this.config.destination.target === 'pubsub') {
      numberOfTasks = await this.startThroughPubSub_(multipleTag, records);
    } else if (this.config.destination.target === 'gcs') {
      if (this.config.destination.file) {
        numberOfTasks = await this.startThroughStorage_(multipleTag, records);
      } else if (this.config.destination.http) {
        numberOfTasks = await this.startThroughHttp_(multipleTag, records);
      }
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
   * @return {!Promise<number>} Number of multiple tasks.
   * @private
   */
  async startThroughPubSub_(tag, records) {
    const qps = getProperValue(this.config.destination.qps, 1, false);
    const { message: configedMessage } = this.config.destination;
    const managedSend = apiSpeedControl(
      this.recordSize, 1, qps, (batchResult) => batchResult);
    const sendSingleMessage = async (lines, batchId) => {
      if (this.recordSize === 1 && lines.length !== 1) {
        throw Error('Wrong number of Pub/Sub messages.');
      }
      let extraParameters;
      extraParameters = JSON.parse(lines[0]);
      if (this.recordSize > 1) {
        extraParameters.records = lines.join('\n');
      }
      const finalParameters =
        Object.assign({}, this.parameters, extraParameters);
      const message = configedMessage
        ? replaceParameters(JSON.stringify(configedMessage), finalParameters)
        : JSON.stringify(finalParameters);
      try {
        await this.taskManager.startTasks(
          this.config.destination.taskId,
          { [FIELD_NAMES.MULTIPLE_TAG]: tag },
          message
        );
        return true;
      } catch (error) {
        this.logger.error(`Pub/Sub message[${batchId}] failed.`, error);
        return false;
      }
    };
    const results =
      await managedSend(sendSingleMessage, records, `multiple_task_${tag}`);
    return results.length;
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
   * @return {!Promise<number>} Number of multiple tasks.
   * @private
   */
  async startThroughStorage_(tag, records) {
    const tasks = [];
    for (let i = 0; i < records.length; i += this.recordSize) {
      const chunk = records.slice(i, i + this.recordSize);
      const task = Object.assign(
        {},
        this.parameters,
        JSON.parse(chunk[0]),
        {
          taskId: this.config.destination.taskId,
          [FIELD_NAMES.MULTIPLE_TAG]: tag,
        });
      if (this.recordSize > 1) {
        task.records = chunk.join('\n');
      }
      tasks.push(JSON.stringify(task));
    }
    const content = tasks.join('\n');

    /** @type {StorageFileConfig} */
    const destination = this.config.destination.file;
    const outputFile = StorageFile.getInstance(
        destination.bucket, destination.name, {
          projectId: destination.projectId,
          keyFilename: destination.keyFilename,
        });
    await outputFile.getFile().save(content);
    return tasks.length;
  }

  /**
   * Sends a file to Tentacles HTTP Cloud Functions to start the multiple task.
   * In this way, the Tentacles config can be embedded here and not required to
   * be uploaded to Tentacles' Firestore. It also brings the flexibility to
   * modify the config items based on the file to be processed.
   * @param {string} tag The multiple task tag to mark task instances.
   * @param {!Array<string>} records Array of multiple task instances data. Each
   *     element is a JSON string of the 'appendedParameters' object for an task
   *     instance.
   * @return {!Promise<number>} Number of multiple tasks.
   * @private
   */
  async startThroughHttp_(tag, records) {
    const tasks = [];
    const tentaclesFile = {};
    // Generate a new file if there are more than one record for one task.
    if (this.recordSize > 1) {
      for (let i = 0; i < records.length; i += this.recordSize) {
        const chunk = records.slice(i, i + this.recordSize);
        tasks.push(JSON.stringify({ records: chunk.join('\n') }));
      }
      const sourceFile = this.config.source.file;
      tentaclesFile.file = {
        bucket: sourceFile.bucket,
        name: sourceFile.name + '_for_multiple_task',
      };
      const outputFile = StorageFile.getInstance(
        tentaclesFile.file.bucket, tentaclesFile.file.name, {
        projectId: sourceFile.projectId,
        keyFilename: sourceFile.keyFilename,
      });
      await outputFile.getFile().save(tasks.join('\n'));
    } else {
      tasks.push(...records);
      tentaclesFile.file = this.config.source.file;
    }
    // To keep the TaskConfig simple, fill default fields here.
    const tentaclesFileOption = lodash.merge({
      service: {
        projectId: '${projectId}',
        locationId: '${locationId}',
        namespace: '${namespace}',
      },
      attributes: { api: 'PB' },
      config: {
        topic: this.taskManager.getMonitorTopicName(),
        attributes: {
          taskId: this.config.destination.taskId,
          [FIELD_NAMES.MULTIPLE_TAG]: tag,
        }
      }
    }, this.config.destination.http);
    const finalOption = JSON.parse(replaceParameters(
      JSON.stringify(tentaclesFileOption), this.parameters, true));
    const options = await getRequestOption(finalOption);
    const file = getFileUrl(tentaclesFile);
    const { attributes, config } = finalOption;
    options.data = {
      file,
      attributes,
      config,
    };
    const result = await requestWithRetry(options, this.logger);
    const { fileId } = result;
    this.parameters.tentaclesFileId = fileId;
    return tasks.length;
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
