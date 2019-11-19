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
 * @fileoverview Sentinel main class.
 */

'use strict';
const {
  CloudFunctionsUtils: {
    ValidatedStorageFile,
    PubsubMessage,
    CloudFunction,
    adaptNode6,
    validatedStorageTrigger,
  },
  PubSubUtils: {publish},
  utils: {getLogger, wait, replaceParameters},
} = require('nodejs-common');
const {TaskConfigDao, TaskType, BigQueryTableConfig} = require(
    './dao/task_config_dao.js');
const {TaskLogStatus} = require('./task_log_impl/task_log_dao.js');
const {getTaskLogDao} = require('./task_log.js');
const {LoadTask} = require('./tasks/load_task.js');
const {QueryTask} = require('./tasks/query_task.js');
const {PredictTask} = require('./tasks/predict_task.js');
const {ExportTask} = require('./tasks/export_task.js');
const {AutoMlService} = require('./automl_service.js');

/**
 * Sentinel is a Cloud Functions based solution to coordinate BigQuery related
 * jobs(tasks), e.g. loading file from Cloud Storage, querying or exporting data
 * to Cloud Storage. Besides these jobs, it also support AutoML Tables batch
 * prediction job.
 * TODO: update following doc.
 * 1. It uses the name of ingested file to figure out the configurations, e.g.
 * load the data in to which BigQuery table, the schema, the query SQL, etc.
 * All these configurations are grouped to 'Module'. Module configuration can
 * be stored in Firestore/Datastore.
 * 2. It offers standard implementation to 'load the data into BigQuery' and
 * 'run SQL ane export results to Google Cloud Storage(GCS)' based on the
 * configurations.
 */
class Sentinel {
  constructor(topicPrefix = 'sentinel') {
    /** @const {string} */ this.topicPrefix = topicPrefix;
    /** @const {!Logger} */
    this.logger = getLogger('S.MAIN');
    this.taskConfigDao = new TaskConfigDao();
    const dataSource = process.env['FIRESTORE_TYPE'];
    this.taskLogDao = getTaskLogDao(dataSource);
  }

  getStorageMonitor(inbound) {
    /**
     * Loads data from Google Cloud Storage (GCS) file and slices into pieces,
     * then sends pieces as messages to Pub/sub. After that, sends a 'nudge'
     * message to start the process to send out data to target API system.
     * @param {!ValidatedStorageFile} file Validated Storage file information
     *     from the function 'validatedStorageTrigger'..
     * @return {!Promise<string>} ID of the 'nudge' message.
     */
    const monitorStorage = (file) => {
      return this.getTaskIdByFile(file.name).then((taskIds) => {
        return Promise.all(taskIds.map((taskId) => {
          const attributes = {taskId: taskId};
          const parameters = JSON.stringify({
            file: file,
            partitionDay: getDatePartition(file.name),
          });
          this.logger.debug(`Trigger Load task: ${taskId}`);
          return publish(this.getStartTopicName_(), parameters, attributes);
        })).catch((error) => {
          console.error(`Error in handling file: ${JSON.stringify(file)} `,
              error);
        });
      });
    };
    return validatedStorageTrigger(monitorStorage, inbound);
  }

  /**
   * Gets the task config Id for the given file.
   * @param {string} fileName Name of ingested file.
   * @return {!Promise<!Array<string>>}
   */
  getTaskIdByFile(fileName) {
    const regex = /task\[([\w-]*)]/i;
    const task = fileName.match(regex);
    if (task) {
      return Promise.resolve([task[1]]);
    }
    return this.taskConfigDao.list(
        [{property: 'type', value: TaskType.LOAD}]).then((configs) => {
      const found = configs.filter((config) => {
        const taskConfig = config.entity;
        if (taskConfig.source && taskConfig.source.fileNamePattern) {
          const regex = new RegExp(taskConfig.source.fileNamePattern);
          return regex.test(fileName);
        }
        return false;
      });
      if (found.length === 0) {
        throw new Error(`Can't find Load Task for file: '${fileName}'`);
      }
      if (found.length > 1) {
        console.warn(`Find more than one Task for file: '${fileName}'`,
            found.map(config => config.id));
      }
      return found.map((task) => task.id);
    });
  }

  /**
   * Gets the Cloud Functions 'BigQuery Monitor' which is triggered by a log
   * event (Pub/Sub message). The Cloud Functions will load TaskLog based on the
   * jobId and kick off next task(s) if there are any.
   * @return {!CloudFunction}
   */
  getBigQueryMonitor() {
    /** @type {!CloudFunctionNode8} */
    const monitorBigQuery = (message, context) => {
      const payload = JSON.parse(
          Buffer.from(message.data, 'base64').toString());
      this.logger.debug(payload);
      if (payload.resource.type === 'bigquery_resource' &&
          payload.protoPayload.methodName === 'jobservice.jobcompleted') {
        const event = payload.protoPayload.serviceData.jobCompletedEvent;
        return this.handleResourceEvent(event);
      } else {
        throw new Error(`Unknown message: ${JSON.stringify(payload)}`);
      }
    };
    return adaptNode6(monitorBigQuery);
  }

  handleResourceEvent(event) {
    const job = event.job;
    const eventName = event.eventName;
    const jobStatus = job.jobStatus.state;
    const jobId = job.jobName.jobId;
    this.logger.debug(`Task JobId[${jobId}] [${eventName}] [${jobStatus}]`);
    const filter = {property: 'jobId', value: jobId};
    return this.taskLogDao.list([filter]).then((logs) => {
      if (!logs || logs.length === 0) {
        // If we need to support external query, then put the logic
        // here. It's similar to map to task for Load task.
        console.log(`Can't find the job: ${jobId}, it may be predicts.`);
        if (eventName === 'load_job_completed') {
          const destinationTable = job.jobConfiguration.load.destinationTable;
          if (destinationTable.tableId === 'predictions') {
            console.log(`Try to match this job to a prediction result...`,
                destinationTable);
            const filters = [{
              property: 'status',
              value: TaskLogStatus.STARTED,
            }];
            return this.taskLogDao.list(filters).then((jobs) => {
              const predictJobs = jobs.filter(
                  (job) => job.entity.jobId.startsWith('projects'));
              if (predictJobs.length === 0) {
                console.log('There is no running prediction tasks. Quit');
                return;
              }
              return this.matchPredictTask(predictJobs, destinationTable);
            });
          }
        }
      } else {
        /** @type {!TaskLog} */ const taskLog = logs[0].entity;
        const taskLogId = logs[0].id;
        if (jobStatus === 'DONE') {
          return this.completeTaskJob(taskLog, taskLogId);
        }
        console.log(`Job Status is not DONE: `, payload);
        return this.taskLogDao.merge({
          finishTime: new Date(),
          status: TaskLogStatus.ERROR,
          error: job.jobStatus.error,
        }, taskLogId);
      }
    }).catch((error) => void console.error(error));
  }

  /**
   * Checks the AutoML prediction job status for the one that matches the
   * destination table. If there is matched one, completes that job; otherwise,
   * waits for 10 seconds and tries again with pending jobs until there is no
   * available pending jobs.
   * @param {!Array<{id:(string|number),entity:TaskLog}>} jobs Unfinished
   *     prediction tasks to be checked for status and whether matches the
   *     destination table.
   * @param {!BigQueryTableConfig} destinationTable BigQuery table information
   *     from a BigQuery load job completed event.
   * @return {Promise<(!Array<TaskLog>|TaskLog|undefined)[] | never>}
   */
  matchPredictTask(jobs, destinationTable) {
    return this.updatePendingTasks(jobs, destinationTable).then(
        ([matchedJob, pendingJobs]) => {
          if (matchedJob) {
            const taskJob = matchedJob.entity;
            const taskJobId = matchedJob.id;
            taskJob.parameters = JSON.stringify(
                Object.assign(JSON.parse(taskJob.parameters),
                    {outputBiqQuery: destinationTable}));
            return this.completeTaskJob(taskJob, taskJobId);
          }
          if (pendingJobs && pendingJobs.length > 0) {
            //Due to the delay between 'predictions' results exported and a
            // 'predict' job is marked as done, wait some time before continue.
            console.log(`There are pending jobs, wait 10s before next round.`);
            return wait(10000).then(
                () => this.matchPredictTask(pendingJobs, destinationTable));
          }
          console.log('There is no pending prediction job. Quit.');
        });
  }

  /**
   * Checks the AutoML prediction job status for the one that matches the
   * destination table. If there is not matched job but not finished jobs,
   * returns those jobs list as the 'pending' jobs to be checked again later.
   * @param {!Array<{id:(string|number),entity:TaskLog}>} jobs Unfinished
   *     prediction tasks to be checked for status and whether matches the
   *     destination table.
   * @param {!BigQueryTableConfig} destinationTable BigQuery table information
   *     from a BigQuery load job completed event.
   * @return {!Promise<[
   *       ({id:(string|number),entity:TaskLog}|undefined),
   *       (!Array<{id:(string|number),entity:TaskLog}>|undefined),
   *     ]>} The first element(if exists) is the matched task log. The second
   *     element is the unfinished tasks.
   */
  updatePendingTasks(jobs, destinationTable) {
    const autoMl = new AutoMlService();
    const loadDestination = `bq://${destinationTable.projectId}.${destinationTable.datasetId}`;
    console.log(`Start to match prediction output: ${loadDestination}`);
    const promises = jobs.map((job) => {
      const taskJob = job.entity;
      const taskJobId = job.id;
      return autoMl.getOperation(taskJob.jobId).then((result) => {
        this.logger.debug(`Start to load Predict job: ${taskJob.jobId}`);
        this.logger.debug(result);
        if (result.error) {
          console.error(`Predict Task[${taskJobId}] error: `, result.error);
          return this.taskLogDao.merge({
            finishTime: new Date(),
            status: TaskLogStatus.ERROR,
            error: result.error.message,
          }, taskJobId).then(() => 'SKIPPED');
        }
        if (!result.done) {
          console.log(`Predict Task[${taskJobId}] not finished.`,
              JSON.stringify(result));
          return 'PENDING';
        }
        const output = result.metadata.batchPredictDetails.outputInfo.bigqueryOutputDataset;
        if (output !== loadDestination) {
          console.log(`This event's output is ${loadDestination},
                 not the same as predict task's ${output}`);
          return 'SKIPPED';
        }
        console.log(`Got ${taskJobId} matched. Start to finish it.`);
        return 'MATCHED';
      });
    });
    return Promise.all(promises).then((results) => {
      const matched = results.indexOf('MATCHED');
      if (matched > -1) return [jobs[matched]];
      return [undefined,
        jobs.filter((job, index) => (results[index] === 'PENDING'))];
    });
  }

  /**
   * Updates the taskLog's status ('FINISHED') and finishTime. If there are next
   * tasks, sends out the messages to Pub/Sub topic named 'Task to Run' to start.
   * @param {!TaskLog} taskLog
   * @param {(number|string)} taskLogId
   * @return {!Promise<(!Array<string>|undefined)>} If the completed task has
   *    followed tasks, returns the message Id of the next tasks.
   */
  completeTaskJob(taskLog, taskLogId) {
    this.logger.debug(`Start to finish task ${taskLogId}`);
    const mergedTaskLog = Object.assign({}, taskLog, {
      finishTime: new Date(),
      status: TaskLogStatus.FINISHED,
    });
    return this.taskLogDao.finishTask(taskLogId, mergedTaskLog).then(
        (finishResult) => {
          if (!finishResult) {
            console.log(`Fail to finish the taskLog [${taskLogId}]. Quit.`);
            return;
          }
          return this.taskConfigDao.load(taskLog.taskId).then((taskConfig) => {
            if (taskConfig.next) {
              const nextTasks = (typeof taskConfig.next === 'string') ?
                  taskConfig.next.split(',') : taskConfig.next;
              return Promise.all(nextTasks.map((taskId) => {
                const attributes = {
                  taskId: taskId.trim(),
                  previousJobId: taskLogId,
                };
                const parameters = taskLog.parameters || "{}";
                console.log(`Trigger next: ${taskId}`);
                return publish(this.getStartTopicName_(), parameters,
                    attributes);
              }));
            }
          });
        });
  }

  /**
   * Gets the Cloud Functions 'Task Runner' which is triggered by a Pub/Sub
   * message. The Cloud Functions will load Task Config based on the taskId
   * and start it.
   * @return {!CloudFunction}
   */
  getTaskRunner() {
    /** @type {!CloudFunctionNode8} */
    const startTask = (message, context) => {
      const attributes = message.attributes || {};
      const messageId = context.eventId;
      let parametersStr = Buffer.from(message.data, 'base64').toString();
      this.logger.debug('message id:', messageId);
      this.logger.debug('message.data decoded:', parametersStr);
      if (parametersStr.indexOf('${') > -1) {
        console.log('Need to preprocess parameters.', parametersStr);
        parametersStr = replaceParameters(parametersStr, getDefaultParameters());
        console.log('Get init parameters: ', parametersStr);
      }
      const parameters = parametersStr ? JSON.parse(parametersStr) : {};
      const newTaskLog = Object.assign({
        createTime: new Date(),
        status: TaskLogStatus.INITIAL,
        parameters: parametersStr,
      }, attributes);
      const newTaskId = messageId;
      return this.taskLogDao.startTask(newTaskId, newTaskLog).then((started) => {
        if (!started) {
          console.warn(`Task Log ${newTaskId} exists. Duplicated? Quit.`);
          return;
        }
        return this.startJobForTask(attributes.taskId, parameters).then(
            (jobId) => {
              console.log(`Job ID: ${jobId}`);
              return this.taskLogDao.merge({
                jobId: jobId,
                startTime: new Date(),
                status: TaskLogStatus.STARTED,
              }, newTaskId);
            }).catch((error) => {
          return this.taskLogDao.merge({
            finishTime: new Date(),
            status: TaskLogStatus.ERROR,
            error: error.message,
          }, newTaskId);
        });
      }).catch((error) => {
        console.log(error);
      });
    };
    return adaptNode6(startTask);
  }

  /**
   * Loads the task config and starts.
   * @param {string} taskConfigId
   * @param {object} parameters
   * @return {!Promise<string>} Task job Id.
   */
  startJobForTask(taskConfigId, parameters = {}) {
    return this.taskConfigDao.load(taskConfigId).then((taskConfig) => {
      if (!taskConfig) {
        console.log(`Fail to load Task ${taskConfigId}. Quit.`);
        throw new Error(`Fail to load Task ${taskConfigId}`);
      }
      const task = this.createTask_(taskConfig, parameters);
      return task.start();
    });
  }

  createTask_(taskConfig, parameters) {
    switch (taskConfig.type) {
      case TaskType.LOAD:
        return new LoadTask(taskConfig, parameters);
      case TaskType.QUERY:
        return new QueryTask(taskConfig, parameters);
      case TaskType.PREDICT:
        return new PredictTask(taskConfig, parameters);
      case TaskType.EXPORT:
        return new ExportTask(taskConfig, parameters);
    }
    throw new Error(`Unknown task type: ${taskConfig.type}`);
  }

  getStartTopicName_() {
    return `${this.topicPrefix}-start`;
  }

}

/**
 * Extracts the date information from the file name. If there is no date
 * recognized, will return today's date.
 * @param {string} filename. It is supposed to contain date information in
 *     format 'YYYY-MM-DD' or 'YYYYMMDD'.
 * @return {string} Date string in format "YYYYMMDD".
 */
const getDatePartition = (filename) => {
  const reg = /\d{4}-?\d{2}-?\d{2}/;
  const date = reg.exec(filename);
  let partition;
  if (date) {
    partition = date[0];
  } else {
    partition = (new Date()).toISOString().split('T')[0];
    console.log(
        `No date find in file: ${filename}. Use current date: ${partition}.`);
  }
  return partition.replace(/-/g, '');
};

/**
 * Returns the default parameter object. Current, it contains:
 *   today
 *   yesterday
 * @return {{string: string}}
 */
const getDefaultParameters = () => {
  const now = new Date();
  const result = {};
  result.now = now.toISOString();
  result.today = (now.toISOString().split('T')[0]).replace(/-/g, '');
  now.setDate(now.getDate() - 1);
  result.yesterday = now.toISOString().split('T')[0].replace(/-/g, '');
  return result;
};

exports.getDefaultParameters = getDefaultParameters;

exports.Sentinel = Sentinel;
