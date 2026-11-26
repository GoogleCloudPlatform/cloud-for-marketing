// Copyright 2021 Google Inc.
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

/** @fileoverview Sentinel task config sheet.*/

/** Grouped Sentinel task types for JSON editor. */
const SENTINEL_GROUPED_TASK_TYPES = {
  BigQuery: {
    query: 'Query on BigQuery',
    load: 'Load data file(s) to BigQuery table',
    export: 'Export a BigQuery table to Cloud Storage',
    create_external: 'Create a BigQuery external table from Google Sheets',
    delete_bigquery: 'Delete a BigQuery dataset or table',
    export_schema: 'Export a BigQuery table schema to Cloud Storage',
  },
  Data: {
    report: 'Download data from Google Ads/SA360/CM360/DV360/YouTube',
    query_adh: 'Run an ADH query and export result to a BigQuery table',
    data_transfer: 'Run a BigQuery Data Transfer job',
  },
  'Task Organize': {
    knot: 'Trigger one or multiple tasks',
    multiple: 'Trigger one task multiple times with different parameters',
    speed_controlled: 'Invoke a Pupa Cloud Function in a batch mode',
  },
  'ML/AI': {
    predict: 'Batch prediction on Vertex AI',
    execute_notebook: 'Execute a Notebook',
  },
  Misc: {
    execute_run_job: 'Execute a Cloud Run Job',
    invoke_cloud_function: 'Invoke a Pupa Cloud Function',
    notify: 'Send out an email',
    download: 'Download a URL and save to Cloud Storage',
    copy_gcs: 'Copy files between Cloud Storage buckets',
  },
};

/** Default 'Task Id' background color based on the task type. */
const TASK_BACKGROUND_COLOR = Object.freeze({
  create_external: '#fde293',
  load: '#fde293',
  query: '#aecbfa',
  export: '#a8dab5',
  report: '#d7aefb',
  knot: '#a1e4f2',
  multiple: '#a1e4f2',
  speed_controlled: '#f6aea9',
  execute_notebook: '#fba9d6',
  execute_run_job: '#fba9d6',
  invoke_cloud_function: '#fba9d6',
  notify: '#fdc69c',
});

/**
 * Gets the JSON string of the Sentinel task.
 * @param {string} instanceName
 * @param {string} targetType
 * @param {string} targetTaskId
 * @return {string} The JSON string stands for the configuration.
 */
function getSentinelTask(instanceName, targetType, targetTaskId) {
  const result = PlainSheet.instances[instanceName]
    .getArrayOfRowEntity()
    .filter(({taskId}) => taskId === targetTaskId);
  if (result.length === 0) {
    throw new Error(`Fail to find Sentinel task for ${targetTaskId}.`);
  }
  return result[0].taskConfig;
}

/**
 * Updates or insert the Sentinel task.
 * @param {string} instanceName
 * @param {string} type
 * @param {string} targetTaskId
 * @param {string} configContent
 */
function saveSentinelTask(instanceName, type, targetTaskId, configContent) {
  const plainSheet = PlainSheet.instances[instanceName];
  const taskConfig = Object.assign(JSON.parse(configContent), {type});
  const formattedJSON = JSON.stringify(taskConfig, null, 2);
  const values = [[targetTaskId, formattedJSON]];
  let updatedRow = 0;
  plainSheet.getArrayOfRowEntity().forEach(({taskId, taskConfig}, index) => {
    if (taskId === targetTaskId) {
      updatedRow = index + ROW_INDEX_SHIFT;
      console.log('find existing line', updatedRow);
      const range = plainSheet
        .getEnhancedSheet()
        .sheet.getRange(index + ROW_INDEX_SHIFT, 1, 1, 2);
      range.setValues(values);
      range.setFontColor('black');
      range.clearNote();
    }
  });
  if (updatedRow === 0) {
    console.log('insert new config', updatedRow);
    plainSheet.getEnhancedSheet().append(values);
  }
}

/**
 * Type of the object that each row of the sheet can be mapped to.
 *
 * @typedef {{
 *   taskId: string,
 *   taskConfig: string,
 * }}
 */
let SentinelConfigRowEntity;

/**
 * Constructs a Pub/Sub message payload to trigger a Sentinel task. The
 * message includes the topic name, attributes and data.
 *
 * @param {string} taskId
 * @param {string} testParameters - A string containing a JSON object of
 *   parameters that are used to trigger the task.
 * @return {!Object} An object containing the topic name, attributes and data.
 * @property {string} topicName - The name of the Pub/sub topic.
 * @property {!Object} attributes - Message attributes.
 * @property {string} data - The test parameters JSON string.
 */
function buildSentinelTaskMessage(taskId, testParameters) {
  const properties = getDocumentProperties();
  const topicName = getDocumentProperty('sentinelTopic');
  const data = replaceVariables(testParameters, properties);
  const regex = /"#(\w+)#"/gm;
  const missed = [];
  let match;
  while ((match = regex.exec(data)) !== null) {
    missed.push(match[1]);
  }
  if (missed.length > 0) {
    throw new Error(`Fail to find value for #variable#: ${missed.join()}`);
  }
  return {
    topicName,
    attributes: {taskId},
    data,
  };
}

/**
 * Publishes the message to trigger a Sentinel Task.
 * @param {!Object} messageMetadata
 * @return {string}
 */
function triggerSentinelTask(messageMetadata) {
  const {topicName, attributes, data} = messageMetadata;
  const message = {
    attributes,
    data: Utilities.base64Encode(data),
  };
  const projectId = getDocumentProperty('projectId');
  const pubsub = new PubSub(projectId);
  const {messageIds, error} = pubsub.publish(topicName, message);
  if (error) {
    throw error.message;
  }
  return `Message was sent with Id: ${messageIds[0]}.`;
}

// /**
//  * Sends the message to trigger a Sentinel Task through the Cloud Functions
//  * delegator. This function is used by users have limited permission to the
//  * GCP project (usually only as `Cloud Functions Developer`), the users don't
//  * need to install the solution to run the workflow.
//  * @see DELEGATOR_CLOUD_FUNCTIONS
//  * @param {object} message
//  * @return {string}
//  */
// function delegateSentinelTask(message) {
//   const projectId = getDocumentProperty('projectId');
//   const locationId = getDocumentProperty('locationId');
//   const cloudFunctions = new CloudFunctionsInvoker(projectId, locationId);
//   const {functionName} = DELEGATOR_CLOUD_FUNCTIONS['Pub/Sub Publisher'];
//   const {error, messageId} = cloudFunctions.invoke(functionName, message);
//   if (error) {
//     throw error.message || error.toString();
//   }
//   return (
//     `Delegated message was sent with Id: ${messageId} for task ` +
//     `${message.attributes.taskId}.`
//   );
// }

/**
 * Open a modal dialog and show the Sentinel workflow.
 * @param {string} taskConfigId - The Id of the starting task of the workflow.
 * @param {boolean=} lastRun - Whether or not to show the latest execution when
 *     there is no taskLogId; if this is false and there is no taskLogId, it
 *     will show the definition of the workflow.
 * @param {string?} taskLogId - The Id of the starting taskLog of the execution.
 */
function showSentinelWorkflow(taskConfigId, lastRun = false, taskLogId) {
  const initialWorkflowCode = getSentinelWorkflow(
    taskConfigId,
    lastRun,
    taskLogId,
  );
  const template = HtmlService.createTemplateFromFile('pages/workflow_diagram');
  template.taskConfigId = taskConfigId;
  template.lastRun = lastRun;
  template.taskLogId = taskLogId;
  template.initialWorkflowCode = JSON.stringify(initialWorkflowCode);
  SpreadsheetApp.getUi().showModalDialog(
    template.evaluate().setWidth(1000).setHeight(800),
    'Workflow Diagram',
  );
  console.log('Opening workflow diagram...');
}

/**
 * Gets the Mermaid flow chart code of a Sentinel workflow. This is invoked by
 * the open dialog in the spreadsheet.
 * @see showSentinelWorkflow for the params.
 * @param {string} taskConfigId
 * @param {boolean} lastRun
 * @param {string?} taskLogId
 * @return {string} The Mermaid flow chart code
 */
function getSentinelWorkflow(taskConfigId, lastRun, taskLogId) {
  const properties = getDocumentProperties();
  const {projectId, locationId, timeZone, sentinelReport} = properties;
  const payload = {
    taskLogId,
    taskConfigId,
    timeZone,
    lastRun,
    output: {target: 'mermaid', format: 'dev'},
    responseContent: 'json',
  };
  const cloudRunService = new CloudRunService(projectId, locationId);
  const invoker = new CloudRunInvoker(cloudRunService, sentinelReport);
  const response = invoker.invoke(payload);
  const code = response.result;
  return code;
}

/**
 * The sheet stores Sentinel task configurations which will be uploaded to
 * Firestore.
 */
class SentinelConfig extends PlainSheet {
  /**
   * In the parameter `options`, the property named 'tasks' is for initial data.
   * @return {!Array<!Array<string>>}
   */
  get initialData() {
    const {tasks = {}} = this.options || {};
    return Object.keys(tasks).map(key => {
      return [key, JSON.stringify(tasks[key], null, 2)];
    });
  }

  /**
   * Returns the default sheet name.
   * @return {string}
   */
  get defaultSheetName() {
    return 'Task Config';
  }

  /**
   * Returns the column configuration for the sheet.
   * @return {!Array<!Object>}
   */
  get columnConfiguration() {
    return [
      {
        name: 'Task Id',
        width: 250,
        format: COLUMN_STYLES.ALIGN_MIDDLE,
        idColumn: true,
      },
      {
        name: 'Task Config',
        width: 600,
        format: COLUMN_STYLES.WRAP_TEXT,
        jsonColumn: true,
        defaultNote: true,
      },
      {name: 'Test Parameters', width: 400, jsonColumn: true},
      {
        name: COLUMN_NAME_FOR_DEFAULT_CONFIG,
        width: 100,
        format: [COLUMN_STYLES.MONO_FONT, COLUMN_STYLES.ALIGN_MIDDLE],
      },
    ];
  }

  /**
   * Returns the content to be downloaded.
   * @return {string}
   */
  get downloadContent() {
    const entities = this.getArrayOfRowEntity();
    const result = {};
    entities.forEach(({taskId, taskConfig}) => {
      result[taskId] = JSON.parse(taskConfig);
    });
    return JSON.stringify(result);
  }

  /**
   * Returns the default filename for downloading.
   * @return {string}
   */
  get downloadFilename() {
    const tag = this.options.sheetName
      ? `_${serpentine(this.options.sheetName)}`
      : '';
    return `config_task${tag}.json`;
  }

  /**
   * Returns the inherent menu items for the sheet.
   * @return {!Array<!Object>}
   */
  get inherentMenuItems() {
    return [
      {
        name: 'Open JSON Editor',
        method: 'openJsonEditor',
      },
      {separator: true},
      {
        name: 'Update the selected config to Firestore',
        method: 'operateSingleRow',
      },
      {
        name: 'Trigger the selected task with test parameters',
        method: 'triggerTask',
      },
      // {
      //   name: '[Delegate] Trigger the selected task with test parameters',
      //   method: 'delegateTask',
      // },
      {separator: true},
      {
        name: 'Show the triggered execution of the selected task',
        method: 'showTriggeredExecution',
      },
      {
        name: 'Show the latest execution in system of the selected task',
        method: 'showLatestExecution',
      },
      {
        name: 'Show workflow starts with the selected task',
        method: 'showWorkflow',
      },
      {separator: true},
      {name: 'Update all configs to Firestore', method: 'operateAllRows'},
      {
        name: 'Append all configs from Firestore to Sheets',
        method: 'loadFromFirestore',
      },
      {name: 'Download the current sheet as a JSON file', method: 'export'},
      {separator: true},
      {name: 'Reset sheet (will lose modification)', method: 'initialize'},
    ];
  }

  /** @override */
  initialize() {
    super.initialize();
    // Status column conditional format
    const sheet = this.getEnhancedSheet().sheet;
    const taskIdIndex = this.columnName.indexOf('Task Id') + 1;
    const taskIdColumn = sheet.getRange(
      ROW_INDEX_SHIFT,
      taskIdIndex,
      sheet.getMaxRows(),
      1,
    );
    const taskIdRules = Object.keys(TASK_BACKGROUND_COLOR).map(key => {
      const formula = `=IFERROR(FIND("""type"": ""${key}""", B2) > 0, FALSE)`;
      return SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied(formula)
        .setBackground(TASK_BACKGROUND_COLOR[key])
        .setRanges([taskIdColumn])
        .build();
    });
    sheet.setConditionalFormatRules(taskIdRules);
  }

  /**
   * Returns a map of Sentinel tasks to be saved to Firestore. The keys are
   * Firestore entity Ids and the values are Firestore entities.
   *
   * @param {!Array<!SentinelConfigRowEntity>} tasks
   * @return {!Object<string, !Object>}
   * @private
   */
  getEntities_(tasks) {
    const updatedConfigs = JSON.parse(
      replaceVariables(JSON.stringify(tasks), getDocumentProperties()),
    );
    const entities = {};
    updatedConfigs.forEach(({taskId, taskConfig}) => {
      entities[taskId] = JSON.parse(taskConfig);
    });
    return entities;
  }

  /**
   * Updates Sentinel tasks to Firestore.
   * @override
   * @param {!Array<!SentinelConfigRowEntity>} tasks
   * @return {!Array<string>}
   */
  processResources(tasks) {
    const namespace = getDocumentProperty('namespace');
    const entities = this.getEntities_(tasks);
    const results = MojoFunctions.saveEntitiesToFirestore(
      'TaskConfig',
      entities,
      `${namespace}/database`,
    );
    return results.map(
      ({updateTime}) => `Has been updated to Database at ${updateTime}`,
    );
  }

  /**
   * Returns the Pub/Sub message to trigger the selected task.
   * @return {!Object}
   * @private
   */
  getSelectedTaskMessage() {
    const rowIndex = this.getSelectedRow();
    const {taskId, testParameters} =
      this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    return buildSentinelTaskMessage(taskId, testParameters);
  }

  /**
   * Sends the 'test parameter' and 'task id' to Sentinel Pub/Sub topic to
   * trigger the corresponding task.
   */
  triggerTask() {
    const message = this.getSelectedTaskMessage();
    const rowIndex = this.getSelectedRow();
    const note = triggerSentinelTask(message);
    this.showOperationResult(rowIndex, note, 'black', 'Test Parameters');
  }

  // /**
  //  * Sends the 'test parameter' and 'task id' to a delegate Cloud Functions
  //  to
  //  * trigger the corresponding task. In this way, the user only need
  //  permission
  //  * of 'Cloud Functions Developer'.
  //  */
  // delegateTask() {
  //   const message = this.getSelectedTaskMessage();
  //   const rowIndex = this.getSelectedRow();
  //   // const note = delegateSentinelTask(message);
  //   // TODO: change to HTTP request based way?
  //   this.showOperationResult(rowIndex, note, 'black', 'Test Parameters');
  // }

  /** Loads Api Configs from Firestore database to this Google Sheets. */
  loadFromFirestore() {
    const namespace = getDocumentProperty('namespace');
    const results = MojoFunctions.loadEntitiesFromFirestore(
      'TaskConfig',
      `${namespace}/database`,
    );
    const rows = results
      .filter(entity => this.entityFilter(entity))
      .map(({id, json}) => [id, JSON.stringify(json, null, 2)]);
    this.getEnhancedSheet().append(rows);
  }

  /**
   * Shows workflow of the latest execution triggered in this sheet.
   * The TaskLogId will be fetched from the 'note' of the 'Test Parameter' cell.
   * If it was missing, it will show the latest execution available.
   */
  showTriggeredExecution() {
    const rowIndex = this.getSelectedRow();
    const columnIndex = this.columnName.indexOf('Test Parameters') + 1;
    const note = this.getEnhancedSheet()
      .sheet.getRange(rowIndex, columnIndex, 1, 1)
      .getNote();
    let taskLogId;
    if (note) {
      const match = note.match(/Id: (\d+)\.?/);
      if (match && match[1]) {
        taskLogId = match[1];
      }
    }
    this.showWorkflow(true, taskLogId);
  }

  /** Shows workflow of the latest execution in the system. */
  showLatestExecution() {
    this.showWorkflow(true);
  }

  /** Shows workflow definition. */
  showWorkflow(lastRun = false, taskLogId) {
    const rowIndex = this.getSelectedRow();
    const {taskId} = this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    showSentinelWorkflow(taskId, lastRun, taskLogId);
  }

  /** Opens a dialog as Sentinel Task config editor. */
  openJsonEditor() {
    // Default value based on selection
    let defaultType, defaultTaskId;
    try {
      const rowIndex = this.getSelectedRow();
      const rowEntity = this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
      defaultTaskId = rowEntity.taskId;
      defaultType = JSON.parse(rowEntity.taskConfig).type;
    } catch (e) {} // No selected row is allowed here.
    const template = HtmlService.createTemplateFromFile('pages/json_editor');
    template.hostName = this.menuObjectName;
    template.defaultCategory = defaultType || '';
    template.defaultItem = defaultTaskId || '';
    // Customization of the JSON editor dialog
    template.categoryName = 'Task Type';
    template.itemName = 'Task Id';
    template.groupedCategories = SENTINEL_GROUPED_TASK_TYPES;
    template.categorySchemasStr = getSentinelTaskSchema();
    template.getSchemaFunction = 'getSentinelTaskSchema';
    template.getItemFunction = 'getSentinelTask';
    template.saveItemFunction = 'saveSentinelTask';
    const entities = this.getArrayOfRowEntity();
    const itemNames = {};
    const itemConfigs = {};
    entities.forEach(({taskId, taskConfig}, index) => {
      try {
        const task = JSON.parse(taskConfig);
        const type = task.type;
        if (!itemNames[type]) itemNames[type] = [];
        if (!itemConfigs[type]) itemConfigs[type] = {};
        itemNames[type].push(taskId);
        itemConfigs[type][taskId] = task;
      } catch (error) {
        SpreadsheetApp.getUi().alert(
          `Row[${index + 2}]/Task Id[${taskId}] has invalid content ${
            taskConfig
          }`,
        );
      }
    });
    // XXX Some report tasks with 'ancestor' do not have the report type
    //  which is used to determine the JSON schema. In this case, the report
    //  type will be put back to the report task.
    this._fixReportTaskType(itemConfigs.report);
    template.itemNamesStr = JSON.stringify(itemNames);
    template.itemConfigsStr = JSON.stringify(itemConfigs);
    // Open the dialog
    SpreadsheetApp.getUi().showModalDialog(
      template.evaluate().setWidth(1000).setHeight(800),
      'Sentinel Task Editor',
    );
  }

  /**
   * Temporary solution to solve the missing report type for some tasks.
   * It doesn't matter if this 'target' is written back to task config.
   */
  _fixReportTaskType(reports = {}) {
    const getReportType = taskId => {
      const report = reports[taskId];
      if (!report) throw new Error(`Fail to find task: ${taskId}`);
      if (!report.source || !report.source.target) {
        return getReportType(report.ancestor);
      }
      return report.source.target;
    };
    Object.keys(reports).forEach(key => {
      const report = reports[key];
      if (!report.ancestor || (report.source && report.source.target)) return;
      if (!report.source) report.source = {};
      report.source.target = getReportType(key);
    });
  }
}
