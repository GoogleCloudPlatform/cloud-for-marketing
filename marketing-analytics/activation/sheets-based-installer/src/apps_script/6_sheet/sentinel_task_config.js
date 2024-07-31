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

/**
 * Type of the object that each row of the sheet can be mapped to.
 *
 * @typedef {{
 *   taskId: string,
 *   taksConfig: string,
 * }}
 */
let SentinelConfigRowEntity;

/**
 * The sheet stores Sentinel task configurations which will be uploaded to
 * Firestore.
 */
class SentinelConfig extends PlainSheet {

  /**
   * In the parameter `options`, the property named 'tasks' is for intial data.
   * @param {!Object<string,!Object>} tasks A map of tasks, the 'key' is the
   *   task Id, the 'value' is the task definition JSON object.
   */
  get initialData() {
    const { tasks = {} } = this.options || {};
    return Object.keys(tasks).map((key) => {
      return [key, JSON.stringify(tasks[key], null, 2)];
    });
  }

  get defaultSheetName() {
    return 'Task Config';
  }

  get columnConfiguration() {
    return [
      { name: 'Task Id', width: 150, format: COLUMN_STYLES.ALIGN_MIDDLE },
      { name: 'Task Config', width: 600, jsonColumn: true, defaultNote: true },
      { name: 'Test Parameters', width: 400, jsonColumn: true },
      {
        name: COLUMN_NAME_FOR_DEFAULT_CONFIG, width: 100,
        format: COLUMN_STYLES.MONO_FONT,
      },
    ];
  }

  get downloadContent() {
    const entities = this.getArrayOfRowEntity();
    const result = {};
    entities.forEach(({ taskId, taskConfig }) => {
      result[taskId] = JSON.parse(taskConfig);
    });
    return JSON.stringify(result);
  }

  get downloadFilename() {
    const tag = this.options.sheetName
      ? `_${snakeize(this.options.sheetName)}` : '';
    return `config_task${tag}.json`;
  }

  get inherentMenuItems() {
    return [
      {
        name: 'Update selected config to Firestore',
        method: 'operateSingleRow',
      },
      {
        name: 'Trigger selected task with test parameters',
        method: 'triggerTask',
      },
      { separator: true },
      {
        name: 'Show workflow with latest execution of selected task',
        method: 'showLatestExecution',
      },
      {
        name: 'Show workflow starts with selected task',
        method: 'showWorkflow',
      },
      {
        name: 'Show workflow with latest execution of selected task (debug mode)',
        method: 'debugLatestExecution',
      },
      {
        name: 'Show workflow starts with selected task (debug mode)',
        method: 'debugWorkflow',
      },
      { separator: true },
      { name: 'Update all configs to Firestore', method: 'operateAllRows' },
      {
        name: 'Append all configs from Firestore to Sheets',
        method: 'loadFromFirestore',
      },
      { name: 'Download current sheet as a JSON file', method: 'export' },
      { separator: true },
      { name: 'Reset sheet (will lose monification)', method: 'initialize' },
    ];
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
    const upldatedConfigs = JSON.parse(
      replaceVariables(JSON.stringify(tasks), getDocumentProperties()));
    const entities = {};
    upldatedConfigs.forEach(({ taskId, taskConfig }) => {
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
    const entities = this.getEntities_(tasks);
    const results = gcloud.saveEntitiesToFirestore('TaskConfig', entities);
    return results.map(
      ({ updateTime }) => `Has been updated to Database at ${updateTime}`
    );
  }

  /**
   * Sends the 'test parameter' and 'task id' to Sentinel Pub/Sub topic to
   * trigger the corresponding task.
   */
  triggerTask() {
    const properties = getDocumentProperties();
    const rowIndex = this.getSelectedRow();
    const { taskId, testParameters } =
      this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    const projectId = getDocumentProperty('projectId');
    const namespace = getDocumentProperty('namespace');
    const topicName = sentinel.getMonitorTopicName(namespace);
    const message = {
      attributes: { taskId },
      data: Utilities.base64Encode(replaceVariables(testParameters, properties)),
    };
    const pubsub = new PubSub(projectId);
    const response = pubsub.publish(topicName, message);
    const note = `Message was sent with Id: ${response.messageIds[0]}.`;
    this.showOperationResult(rowIndex, note, 'black', 'Test Parameters');
  }

  /** Loads Api Configs from Firestore database to this Google Sheets. */
  loadFromFirestore() {
    const results = gcloud.loadEntitiesToFirestore('TaskConfig');
    const rows = results.filter((entity) => this.entityFilter(entity))
      .map(({ id, json }) => [id, JSON.stringify(json, null, 2)]);
    this.getEnhancedSheet().append(rows);
  }

  /** Shows the flowchart of the workflow in an auto openned new tab. */
  showWorkflow(lastRun = false, format = 'link') {
    const properties = getDocumentProperties();
    const rowIndex = this.getSelectedRow();
    const { taskId } = this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    const realData = {
      taskConfigId: taskId,
      lastRun,
      output: { target: 'mermaid', format, },
      responseContent: 'json',
    };
    const { namespace, projectId, locationId } = properties;
    const proxy = new CloudFunctionsInvoker(projectId, locationId);
    const response = proxy.invoke(`${namespace}_report`, realData);
    const content = response.result;
    if (format === 'link') {
      const html =
        `<script>window.open('${content}', '_blank');google.script.host.close();</script>`;
      const ui = HtmlService.createHtmlOutput(html)
        .setWidth(10)
        .setHeight(10);
      SpreadsheetApp.getUi().showModalDialog(ui, "Opening Diagram ...");
    } else {
      const html = `
<p>Open <a href='https://mermaid.live/' target='_blank'>Mermaid Live Editor</a>
 and paste following code into it.</p>
 <textarea cols='106' rows='33'>${content}</textarea>`;
      const ui = HtmlService.createHtmlOutput(html)
        .setWidth(900)
        .setHeight(600);
      SpreadsheetApp.getUi().showModalDialog(ui, 'Debug information');
    }
  }

  /**
   * Shows the flowchart of the latest execution of the workflow in an auto
   * openned new tab.
   */
  showLatestExecution() {
    this.showWorkflow(true);
  }

  /**
   * Shows the Mermaid graph code in the dialog window, so users can copy and
   * paste it into a Mermaid editor to see and debug the workflow.
   */
  debugWorkflow() {
    this.showWorkflow(false, 'dev');
  }

  /**
   * Shows the Mermaid graph code in the dialog window, so users can copy and
   * paste it into a Mermaid editor to see and debug the latest execution of the
   * workflow.
   */
  debugLatestExecution() {
    this.showWorkflow(true, 'dev');
  }
}
