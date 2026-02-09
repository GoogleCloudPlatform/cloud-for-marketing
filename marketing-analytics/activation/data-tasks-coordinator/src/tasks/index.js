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
 * @fileoverview Hosts all kinds of tasks.
 */

'use strict';
const { TaskType } = require('../task_config/task_config_dao.js');
const { ErrorHandledStatus } = require('./error/error_handled_status.js');
const { RetryableError } = require('./error/retryable_error.js');
const { BaseTask } = require('./base_task.js');
const { LoadTask } = require('./bigquery/load_task.js');
const { ExportTask } = require('./bigquery/export_task.js');
const { QueryTask } = require('./bigquery/query_task.js');
const { ExportSchemaTask } = require('./bigquery/export_schema_task.js');
const { DeleteBigQueryTask } = require('./bigquery/delete_bigquery_task.js');
const { DataTransferTask } = require('./bigquery/data_transfer_task.js');
const { CreateExternalTableTask } = require('./bigquery/create_external_table.js');
const { CopyGcsTask } = require('./gcp/copy_gcs_task.js');
const { ExecuteNotebookTask } = require('./gcp/execute_notebook_task.js');
const { InvokeCloudFunctionTask } = require('./gcp/invoke_cloud_function_task.js');
const { GmcXmlFeedToJsonlTask } = require('./gmc/gmc_xml_feed_to_jsonl_task.js');
const { GmcWebpageFetch } = require('./gmc/gmc_webpage_fetcher.js');
const { DownloadTask } = require('./external/download_task.js');
const { PredictTask } = require('./gcp/predict_task.js');
const { QueryAdhTask } = require('./query_adh_task.js');
const { ReportTask } = require('./report_task.js');
const { KnotTask } = require('./internal/knot_task.js');
const { SpeedControlledTask } = require('./internal/speed_controlled_task.js');
const { MultipleTask } = require('./internal/multiple_task.js');
const { NotifyTask } = require('./external/notify_task.js');

/**
 * All tasks Sentinel supports.
 * @type {!Object<string,!BaseTask>}
 */
const ALL_TASKS = Object.freeze({
  [TaskType.LOAD]: LoadTask,
  [TaskType.EXPORT]: ExportTask,
  [TaskType.QUERY]: QueryTask,
  [TaskType.EXPORT_SCHEMA]: ExportSchemaTask,
  [TaskType.DELETE_BIGQUERY]: DeleteBigQueryTask,
  [TaskType.DATA_TRANSFER]: DataTransferTask,
  [TaskType.CREATE_EXTERNAL]: CreateExternalTableTask,
  [TaskType.COPY_GCS]: CopyGcsTask,
  [TaskType.EXECUTE_NOTEBOOK]: ExecuteNotebookTask,
  [TaskType.INVOKE_CLOUD_FUNCTION]: InvokeCloudFunctionTask,
  [TaskType.DOWNLOAD]: DownloadTask,
  [TaskType.GMC_XML_FEED_TO_JSONL]: GmcXmlFeedToJsonlTask,
  [TaskType.GMC_WEBPAGE_FETCHER]: GmcWebpageFetch,
  [TaskType.PREDICT]: PredictTask,
  [TaskType.QUERY_ADH]: QueryAdhTask,
  [TaskType.REPORT]: ReportTask,
  [TaskType.KNOT]: KnotTask,
  [TaskType.MULTIPLE]: MultipleTask,
  [TaskType.SPEED_CONTROLLED]: SpeedControlledTask,
  [TaskType.NOTIFY]: NotifyTask,
});

/**
 * Returns a task instance based on the given configuration and parameters.
 * @param {Object<string,string>} taskConfig
 * @param {Object<string,string>} parameters
 * @return {!BaseTask}
 */
const buildTask = (taskConfig, parameters = {}) => {
  const taskClass = ALL_TASKS[taskConfig.type];
  if (!taskClass) throw new Error(`Unknown task type: ${taskConfig.type}`);
  const defaultProject = process.env['GCP_PROJECT'];
  return new taskClass(taskConfig, parameters, defaultProject);
};

module.exports = {
  ErrorHandledStatus,
  BaseTask,
  buildTask,
  RetryableError,
};
