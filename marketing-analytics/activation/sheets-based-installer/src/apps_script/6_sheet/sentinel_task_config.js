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
   * @constructor
   * @param {!Object<string,!Object>} tasks A map of tasks, the 'key' is the
   *   task Id, the 'value' is the task definition JSON object.
   */
  constructor(tasks = {}) {
    super();
    this.sheetName = 'Task Config';
    this.columnName = [
      'Task Id',
      'Task Config',
      'Test Parameters',
    ];
    this.fields = this.columnName.map(camelize);
    this.columnWidth = {
      'Task Id': 150,
      'Task Config': 600,
      'Test Parameters': 400,
      default_: 100,
    };
    this.columnFormat = {
      'Task Id': COLUMN_STYLES.ALIGN_MIDDLE,
      default_: { fn: 'setFontFamily', format: 'Consolas' },
    };
    // Register columns contains a JSON string to `JSON_COLUMNS` for
    // auto-checking.
    JSON_COLUMNS.push(`${this.sheetName}.Task Config`);
    JSON_COLUMNS.push(`${this.sheetName}.Test Parameters`);
    this.defaultNoteColumn = 'Task Config';
    // Menu items
    this.menuItem = [
      {
        name: 'Update selected config to Firestore',
        method: `${this.menuFunctionHolder}.operateSingleRow`,
      },
      {
        name: 'Trigger selected task with test parameters',
        method: `${this.menuFunctionHolder}.triggerTask`,
      },
      { seperateLine: true },
      {
        name: 'Update all configs to Firestore',
        method: `${this.menuFunctionHolder}.operateAllRows`,
      },
      {
        name: 'Reset sheet (will lose monification)',
        method: `${this.menuFunctionHolder}.initialize`,
      },
    ];
    // Initialize data
    this.initialData = Object.keys(tasks).map((key) => {
      return [key, JSON.stringify(tasks[key], null, 2)];
    });
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
    const rowIndex = this.getSelectedRow();
    const { taskId, testParameters } =
      this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    const projectId = getDocumentProperty('projectId');
    const namespace = getDocumentProperty('namespace');
    const topicName = sentinel.getMonitorTopicName(namespace);
    const message = {
      attributes: { taskId },
      data: Utilities.base64Encode(testParameters),
    };
    const pubsub = new PubSub(projectId);
    const response = pubsub.publish(topicName, message);
    const note = `Message was sent with Id: ${response.messageIds[0]}.`;
    this.showOperationResult(rowIndex, note, 'black', 'Test Parameters');
  }
}
