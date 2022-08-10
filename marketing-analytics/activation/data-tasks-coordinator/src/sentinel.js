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

/** @fileoverview Sentinel main class. */

'use strict';

const {DateTime} = require('luxon');
const {
  firestore: {DataSource, FirestoreAccessBase,},
  cloudfunctions: {
    ValidatedStorageFile,
    CloudFunction,
    adaptNode6,
    validatedStorageTrigger,
  },
  pubsub: {EnhancedPubSub, getMessage,},
  utils: {getLogger, replaceParameters,},
} = require('@google-cloud/nodejs-common');
const {
  TaskConfigDao,
  TaskType,
  ErrorOptions,
} = require('./task_config/task_config_dao.js');
const {
  TaskLog,
  TaskLogDao,
  FIELD_NAMES,
  TaskLogStatus,
} = require('./task_log/task_log_dao.js');
const {
  ErrorHandledStatus,
  buildTask,
  BaseTask,
  RetryableError,
} = require('./tasks/index.js');
const {buildReport} = require('./tasks/report/index.js');
const {
  resumeStatusCheck,
  pauseStatusCheck,
} = require('./utils/cronjob_helper.js');
const {StatusCheckTask} = require('./tasks/internal/status_check_task.js');
const {
  TaskManagerOptions,
  TaskManager,
} = require('./task_manager.js');

/**
 * String value of BigQuery Data Transfer job status 'SUCCEEDED'
 * in BigQuery Data Transfer pubsub messages.
 * @const {string}
 */
const DATATRANSFER_JOB_STATUS_DONE = 'SUCCEEDED';

/**
 * Types of internal (intrinsic) tasks.
 * @enum {string}
 */
const INTERNAL_TASK_TYPE = Object.freeze({
  STATUS_CHECK: 'status_check',
});

/**
 * Sentinel is a Cloud Functions based solution to coordinate BigQuery related
 * jobs(tasks), e.g. loading file from Cloud Storage, querying or exporting data
 * to Cloud Storage. Besides these jobs, it also support AutoML Tables batch
 * prediction job.
 */
class Sentinel {

  /**
   * Initializes the Sentinel instance.
   * @param {!TaskManagerOptions} options
   * @param {function(Object<string,*>,Object<string,*>):!BaseTask} buildTaskFn
   *     Function to build a task instance based on  given configuration and
   *     parameters.
   */
  constructor(options, buildTaskFn = buildTask) {
    /** @const {!TaskManager} */ this.taskManager = new TaskManager(options);
    /** @const {!TaskManagerOptions} */ this.options = options;
    /** @const {!TaskConfigDao} */ this.taskConfigDao = options.taskConfigDao;
    /** @const {!TaskLogDao} */ this.taskLogDao = options.taskLogDao;
    /** @const {function(Object<string,*>,Object<string,*>):!BaseTask} */
    this.buildTask = buildTaskFn;
    /** @const {!Logger} */ this.logger = getLogger('S.MAIN');
  }

  /**
   * Gets the Cloud Functions 'Storage Monitor' which will send out starting
   * related task(s) messages based on the the name of the file added to the
   * monitored directory.
   * It match Tasks in following order. It will skip second step if Task Id is
   * found in the first step:
   * 1. If there is `task[TASK_CONFIG_ID]` in the file name, just starts the
   * task with TaskConfig Id as `TASK_CONFIG_ID`;
   * 2. Tries to match the file name with all LOAD tasks' configuration
   * `fileNamePattern` and starts all matching Tasks.
   * @param {string} inbound The folder that this function monitors.
   * @return {!CloudFunction}
   */
  getStorageMonitor(inbound) {
    /**
     * Monitors the Cloud Storage for new files. Sends 'start load task' message
     * if it identifies any new files related to Load tasks.
     * @param {!ValidatedStorageFile} file Validated Storage file information
     *     from the function 'validatedStorageTrigger'.
     * @return {!Promise<(!Array<string>|undefined)>} IDs of the 'start load
     *     task' messages.
     */
    const monitorStorage = (file) => {
      return this.getTaskIdByFile_(file.name).then((taskIds) => {
        if (taskIds.length === 0) {
          throw new Error(`Can't find Load Task for file: '${file.name}'`);
        }
        this.logger.debug(`Find ${taskIds.length} Task for [${file.name}]`);
        return Promise.all(taskIds.map((taskId) => {
          const parameters = JSON.stringify({
            file,
            partitionDay: getDatePartition(file.name),
          });
          this.logger.debug(`Trigger Load task: ${taskId}.`);
          return this.taskManager.sendTaskMessage(parameters, {taskId});
        }));
      }).catch((error) => {
        console.error(`Error in handling file: ${file.name}`, error);
        throw error;
      });
    };
    return this.options.validatedStorageTrigger(monitorStorage, inbound);
  }

  /**
   * Returns collection of task IDs which are triggered by the given file name.
   * @param {string} fileName Name of ingested file.
   * @return {!Promise<!Array<string>>}
   * @private
   */
  getTaskIdByFile_(fileName) {
    const regex = /task\[([\w-]*)]/i;
    const task = fileName.match(regex);
    if (task) return Promise.resolve([task[1]]);
    const hasFileNamePattern = (config) => {
      const sourceConfig = config.source;
      return sourceConfig && sourceConfig.fileNamePattern;
    };
    const matchesFileNamePattern =
        (config) => new RegExp(config.source.fileNamePattern).test(fileName);
    return this.taskConfigDao.list(
        [{property: 'type', value: TaskType.LOAD}]).then((configs) => {
      return configs
          .filter(({entity: config}) => hasFileNamePattern(config))
          .filter(({entity: config}) => matchesFileNamePattern(config))
          .map((config) => config.id);
    });
  }

  /**
   * Gets the Cloud Functions 'Task Coordinator' which is triggered by a Pub/Sub
   * message. There are four different kinds of messages:
   * 1. Starting a task. Message attributes (only string typed value) have
   *    following keys:
   *      a. taskId (TaskConfig Id) determines the TaskConfig of the task;
   *      b. parentId (TaskLog Id) Id of the TaskLog that triggered this one;
   *    Message data is a JSON string for the dynamic parameter values, e.g.
   *    'today'. It supports type other than string values.
   * 2. Finishing a task. Message attributes (only string typed value) have
   *    following keys:
   *      a. taskLogId (TaskLog Id) determines the TaskConfig of the task;
   * 3. Logs from Cloud Logging system of subscribed events, e.g. BigQuery job
   *    complete event. This function will complete the matched Task.
   * 4. Pubsub message from BigQuery Data Transfer after a Transfer Run job is
   *    finished. The message attributes only have two keys:
   *      a. eventType: The type of event that has just occurred.
   *         TRANSFER_RUN_FINISHED is the only possible value.
   *      b. payloadFormat: The format of the object payload.
   *         JSON_API_V1 is the only possible value
   *    Go https://cloud.google.com/bigquery-transfer/docs/transfer-run-notifications#format
   *    to have more information of the format.
   */
  getTaskCoordinator() {
    /**
     * @see method 'finishTask_()'
     * @see method 'startTask_()'
     * @see method 'handleResourceEvent_()'
     * @type {!CloudFunctionNode8}
     */
    const coordinateTask = (message, context) => {
      if (this.isStartTaskMessage_(message)) {
        return this.startTaskByMessage_(message, context);
      }
      if (this.isFinishTaskMessage_(message)) {
        return this.finishTaskByMessage_(message);
      }
      if (this.isBigQueryLoggingMessage_(message)) {
        return this.finishBigQueryTask_(message);
      }
      if (this.isBigQueryDataTransferMessage_(message)) {
        return this.finishBigQueryDataTransferTask_(message);
      }
      throw new Error(`Unknown message: ${getMessage(message)}`);
    };
    return adaptNode6(coordinateTask);
  }

  /** Returns whether this is a message to start a new task. */
  isStartTaskMessage_(message) {
    const attributes = message.attributes || {};
    return !!attributes.taskId;
  }

  /** Starts the task based on a Pub/sub message. */
  startTaskByMessage_(message, context) {
    const attributes = message.attributes || {};
    const data = getMessage(message);
    this.logger.debug('message.data decoded:', data);
    const messageId = context.eventId; // New TaskLog Id.
    this.logger.debug('message id:', messageId);
    let parametersStr;
    if (!data) {
      parametersStr = '{}';
    } else if (data.indexOf('${') === -1) { // No placeholder in parameters.
      parametersStr = data;
    } else { // There are placeholders in parameters. Need default values.
      const regex = /\${([^}]*)}/g;
      const parameters = data.match(regex).map((match) => {
        return match.substring(2, match.length - 1);
      });
      const {timezone} = JSON.parse(data);
      parametersStr = replaceParameters(data,
          getDefaultParameters(parameters, timezone));
    }
    const taskLog = {
      parameters: parametersStr,
      ...attributes,
    };
    return this.startTask_(messageId, taskLog);
  }

  /** Returns whether this is a message to finish a task. */
  isFinishTaskMessage_(message) {
    const attributes = message.attributes || {};
    return !!attributes.taskLogId;
  }

  /** Finishes the task based on a Pub/sub message. */
  finishTaskByMessage_(message) {
    const attributes = message.attributes || {};
    this.logger.debug(`Complete task ${attributes.taskLogId}`);
    return this.finishTask_(attributes.taskLogId);
  }

  /**
   * Returns whether this is a message from BigQuery Logging of a completed job.
   */
  isBigQueryLoggingMessage_(message) {
    const data = getMessage(message);
    try {
      const payload = JSON.parse(data);
      return payload.resource
          && payload.resource.type === 'bigquery_resource'
          && payload.protoPayload
          && payload.protoPayload.methodName === 'jobservice.jobcompleted';
    } catch (error) {
      this.logger.error('Checking whether the message is from BigQuery', error);
      return false;
    }
  }

  /** Finishes the task (if any) related to the completed job in the message. */
  finishBigQueryTask_(message) {
    const data = getMessage(message);
    const payload = JSON.parse(data);
    const event = payload.protoPayload.serviceData.jobCompletedEvent;
    return this.handleBigQueryJobCompletedEvent_(event);
  }

  /**
   * Returns whether this is a message from BigQuery Data Transfer of a completed run job.
   */
  isBigQueryDataTransferMessage_(message) {
    try {
      const attributes = message.attributes || {};
      return attributes.eventType === 'TRANSFER_RUN_FINISHED'
          && attributes.payloadFormat === 'JSON_API_V1';
    } catch (error) {
      this.logger.error(
        'Checking whether the message is from BigQuery Data Transfer',
          error
      );
      return false;
    }
  }

  /** Finishes the task (if any) related to the completed job in the message. */
  finishBigQueryDataTransferTask_(message) {
    const data = getMessage(message);
    const payload = JSON.parse(data);
    return this.handleBigQueryDataTransferTask_(payload);
  }

  /**
   * Starts a task in a 'duplicated message proof' way by using
   * 'TaskLogDao.startTask' to check the status.
   * @param {(string|number)} taskLogId
   * @param {!TaskLog} taskLog
   * @return {!Promise<(string | number)>} taskLogId
   * @private
   */
  async startTask_(taskLogId, taskLog) {
    // Get the 'lock' to start this task to prevent duplicate messages.
    const started = await this.taskLogDao.startTask(taskLogId, taskLog);
    if (started) return this.startTaskJob_(taskLogId, taskLog);
    this.logger.warn(`TaskLog ${taskLogId} exists. Duplicated? Quit.`);
  }

  /**
   * Starts a task job:
   *  1. Using TaskConfigId and parameters to create the instance of Task, then
   *     run 'Task.start()' to start the real job in the task;
   *  2. Using 'TaskLogDao.afterStart' to merge the output of 'Task.start()' to
   *     TaskLog, e.g. jobId or other job identity from external systems.
   *  3. Checking whether this task is already done (returns true when this is
   *     a synchronous task). If yes, continue to finish the task.
   * @param {(string|number)} taskLogId
   * @param {!TaskLog} taskLog
   * @return {!Promise<(string | number)>} taskLogId
   * @private
   */
  async startTaskJob_(taskLogId, taskLog) {
    try {
      const parameters = JSON.parse(taskLog.parameters);
      const task = await this.prepareTask(taskLog.taskId, parameters);
      const updatesToTaskLog = await task.start();
      this.logger.debug('Task started with:', JSON.stringify(updatesToTaskLog));
      const needEnableCheckCronJob = task.isManualAsynchronous()
        && !taskLog[FIELD_NAMES.MULTIPLE_TAG] && !taskLog[FIELD_NAMES.EMBEDDED_TAG];
      if (needEnableCheckCronJob) {
        this.logger.info(
          `Asynchronous tasks started. Resume the Status Check Task.`);
        await this.options.statusCheckCronJob.resume();
      }
      await this.taskLogDao.afterStart(taskLogId, updatesToTaskLog);
      const done = await task.isDone();
      if (done) {
        await this.finishTask_(taskLogId);
      }
      return taskLogId;
    } catch (error) {
      console.error(error);
      return this.taskLogDao.saveErrorMessage(taskLogId, error);
    }
  }

  /**
   * Returns the task object based on TaskConfigId and parameters, with
   * TaskManagerOptions injected.
   * @param {string} taskConfigId
   * @param {object} parameters Parameters for this Task. There is a special
   *     property named 'intrinsic' in parameters to indicate this task is
   *     offered by Sentinel and doesn't exists in TaskConfig.
   *     The purpose is to let those intrinsic(internal) tasks to reuse
   *     Sentinel's framework of managing the lifecycle of a task.
   *     Using the 'intrinsic' to avoid the possible clash of parameter names.
   * @return {!Promise<!BaseTask>} Task instance.
   */
  async prepareTask(taskConfigId, parameters = {}) {
    /** @const {!BaseTask} */
    const task = parameters.intrinsic
        ? this.prepareInternalTask_(parameters.intrinsic, parameters)
        : await this.prepareExternalTask_(taskConfigId, parameters);
    return task;
  }

  /**
   * Returns an 'internal' task instance based on the task type and parameters.
   *
   * @param {!INTERNAL_TASK_TYPE} type Internal task type.
   * @param {Object} parameters
   * @return {!BaseTask}
   * @private
   */
  prepareInternalTask_(type, parameters) {
    if (type === INTERNAL_TASK_TYPE.STATUS_CHECK) {
      const task = new StatusCheckTask({}, parameters);
      task.setPrepareExternalTaskFn(this.prepareExternalTask_.bind(this));
      task.injectContext(this.options);
      return task;
    }
    throw new Error(`Unsupported internal task: ${type}`);
  }

  /**
   * Returns an external task instance based on the TaskConfig Id and parameters
   * with context (TaskManagerOptions) injected.
   * When Sentinel starts or finishes an external task, the task information is
   * passed as a TaskConfig Id and an object of parameters. This function will
   * use TaskConfigDao to get the TaskConfig(config), then use the config and
   * parameters to get the Task object.
   * This function will also be passed into Status Check Task to initialize
   * other external tasks.
   * @param {string} taskConfigId
   * @param {Object} parameters
   * @return {!Promise<!BaseTask>}
   * @private
   */
  async prepareExternalTask_(taskConfigId, parameters) {
    const taskConfig = await this.taskConfigDao.load(taskConfigId);
    if (!taskConfig) throw new Error(`Fail to load Task ${taskConfigId}`);
    const task = this.buildTask(taskConfig, parameters);
    task.injectContext(this.options);
    return task;
  };

  /**
   * Finishes a task in a 'duplicated message proof' way by using
   * 'TaskLogDao.finishTask'  to check the status.
   * @param  {(string|number)} taskLogId
   * @return {!Promise<(!Array<string>|undefined)>} Ids of messages to trigger
   *     next task(s).
   * @private
   */
  async finishTask_(taskLogId) {
    this.logger.debug(`Start to finish task ${taskLogId}`);
    const taskLog = await this.taskLogDao.load(taskLogId);
    const finished = await this.taskLogDao.finishTask(taskLogId);
    if (finished) return this.finishTaskJob_(taskLogId, taskLog);
    this.logger.warn(`Fail to finish the taskLog [${taskLogId}]. Quit.`);
  }

  /**
   * Finishes a task job:
   *  1. Using TaskConfigId and parameters to create the instance of Task, then
   *     run 'Task.finish()' to finish the job in the task, e.g. download files
   *     from external system;
   *  2. Using 'TaskLogDao.afterFinish' to merge the output of 'Task.finish()'
   *     to TaskLog;
   *  3. Triggering next Tasks if there are.
   * @param  {(string|number)} taskLogId
   * @param {!TaskLog} taskLog
   * @return {!Promise<(!Array<string>|undefined)>} Ids of messages to trigger
   *     next task(s).
   * @private
   */
  async finishTaskJob_(taskLogId, taskLog) {
    const parameters = JSON.parse(taskLog.parameters);
    let updatedParameterStr = taskLog.parameters;
    const task = await this.prepareTask(taskLog.taskId, parameters);
    try {
      const updatesToTaskLog = await task.finish();
      this.logger.debug('Task finished with', JSON.stringify(updatesToTaskLog));
      if (updatesToTaskLog.parameters) {
        updatedParameterStr = updatesToTaskLog.parameters;
      }
      // 'afterFinish' won't throw 'RetryableError' which can trigger retry.
      await this.taskLogDao.afterFinish(taskLogId, updatesToTaskLog);
    } catch (error) {
      this.logger.error(`Task[${taskLogId}] error:`, error);
      const [errorHandledStatus, result] =
        await this.taskManager.handleFailedTask(taskLogId, taskLog, error);
      if (errorHandledStatus !== ErrorHandledStatus.IGNORED) {
        return result;
      }
    }
    return this.taskManager.startTasks(
      taskLog.next,
      { [FIELD_NAMES.PARENT_ID]: taskLogId },
      updatedParameterStr
    );
  }

  /**
   * Based on the incoming message, updates the TaskLog and triggers next tasks
   * if there is any in TaskConfig of the current finished task.
   * For Load/Query/Export tasks, the taskLog saves 'job Id' generated when the
   * job starts at the beginning. When these kinds of job is done, the log event
   * will be sent here with 'job Id'. So we can match to TaskLogs in database
   * waiting for the job is done.
   * @param event
   * @return {!Promise<(!Array<string>|number|undefined)>} The message Id array
   *     of the next tasks and an empty Array if there is no followed task.
   *     Returns taskLogId (number) when an error occurs.
   *     Returns undefined if there is no related taskLog.
   * @private
   */
  async handleBigQueryJobCompletedEvent_(event) {
    const job = event.job;
    const eventName = event.eventName;
    const jobId = job.jobName.jobId;
    const jobStatus = job.jobStatus.state;
    this.logger.debug(`Task JobId[${jobId}] [${eventName}] [${jobStatus}]`);
    const filter = {property: 'jobId', value: jobId};
    const taskLogs = await this.taskLogDao.list([filter]);
    if (taskLogs.length > 1) {
      throw new Error(`Find more than one task with Job Id: ${jobId}`);
    }
    if (taskLogs.length === 1) {
      return this.finishTask_(taskLogs[0].id);
    }
    this.logger.debug(`BigQuery JobId[${jobId}] is not a Sentinel Job.`);
  }

  /**
   * Based on the incoming message, updates the TaskLog and triggers next tasks
   * if there is any in TaskConfig of the current finished task.
   * For Data Transfer tasks, the taskLog saves the 'name' of run job
   * as 'job id' which is generated when the job starts at the beginning.
   * When the job is done, the datatransfer job will be sent here with the run
   * job 'name'. So we can match to TaskLogs in database
   * waiting for the job is done.
   * @param payload
   * @return {!Promise<(!Array<string>|number|undefined)>} The message Id array
   *     of the next tasks and an empty Array if there is no followed task.
   *     Returns taskLogId (number) when an error occurs.
   *     Returns undefined if there is no related taskLog.
   * @private
   */
  async handleBigQueryDataTransferTask_(payload) {
    const jobId = payload.name;
    const jobStatus = payload.state;
    this.logger.debug(`Data Transfer job[${jobId}] status: ${jobStatus}`);
    const filter = { property: 'jobId', value: jobId };
    const taskLogs = await this.taskLogDao.list([filter]);
    if (taskLogs.length > 1) {
      throw new Error(`Find more than one task with Job Id: ${jobId}`);
    }
    if (taskLogs.length === 1) {
      return this.finishTask_(taskLogs[0].id);
    }
    this.logger.debug(
      `BigQuery Data Transfer JobId[${jobId}] is not a Sentinel Job.`
    );
  }
}

/**
 * Extracts the date information from the file name. If there is no date
 * recognized, will return today's date.
 * @param {string} filename. It is supposed to contain date information in
 *     format 'YYYY-MM-DD' or 'YYYYMMDD'. Though it will also take 'YYYY-MMDD'
 *     or 'YYYYMM-DD'.
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
        `No date find in file: ${filename}. Using current date: ${partition}.`);
  }
  return partition.replace(/-/g, '');
};

/**
 * Returns the default parameter object. Currently, it support following rules:
 *   now - Date ISO String
 *   today - format 'YYYYMMDD'
 *   today_set_X - set the day as X based on today's date, format 'YYYYMMDD'
 *   today_sub_X - sub the X days based on today's date, format 'YYYYMMDD'
 *   today_add_X - add the X days based on today's date, format 'YYYYMMDD'
 *   Y_hyphenated - 'Y' could be any of previous date, format 'YYYY-MM-DD'
 *   Y_timestamp_ms - 'Unix milliseconds timestamp of the start of  date 'Y'
 *   yesterday - quick access as 'today_sub_1'. It can has follow ups as well,
 *       e.g. yesterday_sub_X, yesterday_hyphenated, etc.
 * Parameters get values ignoring their cases status (lower or upper).
 * @param {Array<string>} parameters Names of default parameter.
 * @param {string=} timezone Default value is UTC.
 * @param {number=} unixMillis Unix timestamps in milliseconds. Default value is
 *     now. Used for test.
 * @return {{string: string}}
 */
const getDefaultParameters = (parameters, timezone = 'UTC',
    unixMillis = Date.now()) => {
  /**
   * Returns the value based on the given parameter name.
   * @param {string=} parameter
   * @return {string|number}
   */
  const getDefaultValue = (parameter) => {
    let realParameter = parameter.toLocaleLowerCase();
    const now = DateTime.fromMillis(unixMillis, {zone: timezone});
    if (realParameter === 'now') return now.toISO(); // 'now' is a Date ISO String.
    if (realParameter === 'today') return now.toFormat('yyyyMMdd');
    realParameter = realParameter.replace(/^yesterday/, 'today_sub_1');
    if (!realParameter.startsWith('today')) {
      throw new Error(`Unknown default parameter: ${parameter}`);
    }
    const suffixes = realParameter.split('_');
    let date = now;
    for (let index = 1; index < suffixes.length; index++) {
      if (suffixes[index] === 'hyphenated') return date.toISODate();
      if (suffixes[index] === 'timestamp' && suffixes[index + 1] === 'ms') {
        return date.startOf('days').toMillis();
      }
      const operator = suffixes[index];
      let operationOfLib;
      switch (operator) {
        case 'add':
          operationOfLib = 'plus';
          break;
        case 'set':
          operationOfLib = 'set';
          break;
        case 'sub':
          operationOfLib = 'minus';
          break;
        default:
          throw new Error(
              `Unknown operator in default parameter: ${parameter}`);
      }
      const day = suffixes[++index];
      if (typeof day === "undefined") {
        throw new Error(`Malformed of default parameter: ${parameter}`);
      }
      date = date[operationOfLib]({days: day});
    }
    return date.toFormat('yyyyMMdd');
  }

  const result = {};
  parameters.forEach((parameter) => {
    result[parameter] = getDefaultValue(parameter);
  })
  return result;
};

/**
 * Returns a Sentinel instance based on the parameters.
 * Sentinel works on several components which depend on the configuration. This
 * factory function will seal the details in product environment and let the
 * Sentinel class be more friendly to test.
 *
 * @param {string} namespace The `namespace` of this instance, e.g. prefix of
 *     the topics, Firestore root collection name, Datastore namespace, etc.
 * @param {!DataSource} datasource The underlying datasource type.
 * @return {!Sentinel} The Sentinel instance.
 */
const getSentinel = (namespace, datasource) => {
  /** @type {TaskManagerOptions} */
  const options = {
    namespace,
    taskConfigDao: new TaskConfigDao(datasource, namespace),
    taskLogDao: new TaskLogDao(datasource, namespace),
    pubsub: EnhancedPubSub.getInstance(),
    buildReport,
    statusCheckCronJob: {
      pause: pauseStatusCheck,
      resume: resumeStatusCheck,
    },
    validatedStorageTrigger,
  };
  console.log(
      `Init Sentinel for namespace[${namespace}], Datasource[${datasource}]`);
  return new Sentinel(options);
};

/**
 * Probes the Google Cloud Project's Firestore mode (Native or Datastore), then
 * uses it to create an instance of Sentinel.
 * @param {(string|undefined)=} namespace
 * @return {!Promise<!Sentinel>}
 */
const guessSentinel = (namespace = process.env['PROJECT_NAMESPACE']) => {
  if (!namespace) {
    console.warn(
        'Fail to find ENV variables PROJECT_NAMESPACE, will set as `sentinel`');
    namespace = 'sentinel';
  }
  return FirestoreAccessBase.isNativeMode().then((isNative) => {
    const dataSource = isNative ? DataSource.FIRESTORE : DataSource.DATASTORE;
    return getSentinel(namespace, dataSource);
  });
};

module.exports = {
  Sentinel,
  getSentinel,
  guessSentinel,
  getDatePartition,
  getDefaultParameters,
};
