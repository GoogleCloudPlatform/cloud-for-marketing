// Copyright 2023 Google Inc.
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
 * @fileoverview A loader class for TaskConfig.
 */
const lodash = require('lodash');
const {
  firestore: { DEFAULT_DATABASE, Database, getFirestoreDatabase },
  utils: { replaceParameters },
} = require('@google-cloud/nodejs-common');
const { TaskConfigDao, TaskType }
  = require('../../task_config/task_config_dao.js');
const { TaskLogStatus } = require('../../task_log/task_log_dao.js');
const { getTaskArray } = require('../../task_manager.js');
const { Node, getLinkFunction } = require('./task_log_node_loader.js');

/**
 * @const {number} DEFAULT_LEVEL The default value of a node level. In a graph
 * of a work flow. The task has a less (-1) level value comparing to tasks it
 * triggers. 'Trigger' here means 'next' tasks, 'embedded' tasks or 'multiple'
 * tasks.
 */
const DEFAULT_LEVEL = 10;

/**
 * @const {number} MAX_LEVEL To prevent a loop in a workflow, the loading
 * process will stop and insert a node with error message when it reaches the
 * maximum level.
 */
const MAX_LEVEL = 100;

/**
 * @const {Array<string>} TASK_CONFIG_PROPERTIES The array of properties that
 * will be extracted from TaskConfig entities. These properties will be used to
 * form information of a node.
 */
const TASK_CONFIG_PROPERTIES = [
  'type',
  'destination',
  'embedded',
  'next',
  'appendedParameters',
];

/**
 * This class loads task configs of a workflow and convert them into a sorted
 * array of nodes which can be converted to other formats for different
 * virtualization solutions.
 */
class TaskConfigNodeLoader {

  /**
   * @constructor
   * @param {!TaskConfigDao} taskConfigDao
   * @param {string=} projectId
   */
  constructor(taskConfigDao, projectId = process.env['GCP_PROJECT']) {
    this.taskConfigDao = taskConfigDao;
    this.projectId = projectId;
    this.namespace = this.taskConfigDao.namespace;
    this.dataSource = this.taskConfigDao.dataSource;
    this.databaseId = this.taskConfigDao.databaseId || DEFAULT_DATABASE;
    // There is no 'logId' for TaskConfig, so use a counter to generate 'Ids'.
    this.taskCounter = 1;
    this.parameters = {};
    this.getLink = getLinkFunction('TaskConfig',
      this.dataSource, this.databaseId, this.namespace, this.projectId);
  }

  /**
   * Returns an array of nodes present a workflow sorted by the level of node.
   * @param {string} taskConfigId
   * @return {!Array<!Node>}
   */
  async getWorkFlow(taskConfigId) {
    const nodeMap = await this.loadNodes(taskConfigId);
    return [...nodeMap.values()].sort(({ level: a }, { level: b }) => a - b);
  }

  /**
   * Returns a map of auto generated counter as the key and Node as the value
   * for a workflow.
   * The given taskConfigId is expected to be the start taskConfig of a work
   * flow.
   * Besides the given taskConfig, this function it will also load following
   * other taskConfigs in a recursion way:
   * 1. Its multiple task if this is a multiple task.
   * 2. all embedded tasks if this is an embedded task.
   * 3. all its next tasks.
   *
   * @param {string|number} taskConfigId
   * @param {number=} level
   * @param {!Map=} nodeMap
   * @param {!Object=} options Default properties and values for nodes. This
   *   object is used to add extra fields to let a TaskConfig node can reuse
   *   the same logic of a TaskLog Node, e.g. parentId, embeddedTag, so the
   *   taskConfigs can be connected as a workflow like taskLogs.
   * @param {Object|undefined} parameters Parameters for the task.
   * @return {!Map<string|number, Node>}
   */
  async loadNodes(taskConfigId, level = DEFAULT_LEVEL, nodeMap = new Map(),
    options = {}, parameters = this.parameters) {
    if (level > MAX_LEVEL) {
      const errorNode = {
        id: this.taskCounter++,
        level,
        taskId: `Error_Max_level_reached ${taskConfigId}`,
        status: TaskLogStatus.ERROR,
      };
      nodeMap.set(errorNode.id, errorNode);
      return nodeMap;
    }
    const currentNode = Object.assign(
      await this.getNode(taskConfigId, parameters), options);
    nodeMap.set(currentNode.id, currentNode);
    currentNode.link = this.getLink(taskConfigId);
    currentNode.level = level;
    const nextParameters = Object.assign(
      {}, parameters, currentNode.appendedParameters);
    // A helper function to load a group of taskConfigs, e.g. next tasks or
    // embedded tasks.
    const loadTaskGroup = async (taskGroup, options) => {
      const tasks = getTaskArray(taskGroup);
      await Promise.all(tasks.map((task) => {
        const taskConfigId = task.taskId ? task.taskId : task;
        // Skip dynamic next tasks for TaskConfig. TaskLogs can connect this.
        if (taskConfigId.indexOf('${') > -1) return;
        const taskParameters =
          Object.assign({}, nextParameters, task.appendedParameters);
        return this.loadNodes(
          taskConfigId, level + 1, nodeMap, options, taskParameters);
      }));
    }
    // Load its multiple sub task flow if this is a multiple task.
    if (currentNode.type === TaskType.MULTIPLE) {
      currentNode.tagHeld = `group${currentNode.id}`; // assign a multiple tag
      currentNode.numberOfTasks = 'Runtime';
      const multipleTag = currentNode.tagHeld;
      await this.loadNodes(currentNode.destination.taskId, level + 1, nodeMap,
        { multipleTag }, nextParameters);
    }
    // Load its all embedded tasks if this is an embedded task.
    if (currentNode.type === TaskType.KNOT && currentNode.embedded) {
      currentNode.tagHeld = `group${currentNode.id}`;// assign an embedded tag
      const embeddedTag = currentNode.tagHeld;
      await loadTaskGroup(currentNode.embedded.tasks, { embeddedTag });
    }
    // Load its children(next) tasks.
    if (currentNode.next) {
      const parentId = currentNode.id;
      await loadTaskGroup(currentNode.next, { parentId });
    }
    return nodeMap;
  }

  /**
   * Gets a Node from a TaskConfig with the given TaskConfig Id.
   * @param {string|number} taskConfigId
   * @param {object} parameters
   * @return {!Node}
   */
  async getNode(taskConfigId, parameters) {
    const entity = await this.taskConfigDao.load(taskConfigId);
    if (entity) {
      const config = lodash.merge(lodash.pick(entity, TASK_CONFIG_PROPERTIES),
        {
          id: this.taskCounter++,
          taskId: taskConfigId,
        });
      return JSON.parse(
        replaceParameters(JSON.stringify(config), parameters, true));
    } else {
      console.warn(`TaskConfig ${taskConfigId} does not exist`);
      return {
        id: this.taskCounter++,
        taskId: `Error_Not_found ${taskConfigId}`,
        status: TaskLogStatus.ERROR,
      };
    }
  }
}

/**
 * Returns a TaskConfigNodeLoader instance based on the given GCP project and
 * namespace.
 * @param {string=} namespace
 * @param {string=} projectId
 * @param {string=} databaseId
 * @return {!TaskConfigNodeLoader}
 */
async function prepareTaskConfigNodeLoader(namespace = 'sentinel',
  projectId = process.env['GCP_PROJECT'],
  databaseId = process.env['DATABASE_ID']) {
  /** @type {!Database} */
  const database = await getFirestoreDatabase(projectId, databaseId);
  const taskConfigDao = new TaskConfigDao(database, namespace, projectId);
  return new TaskConfigNodeLoader(taskConfigDao, projectId);
}

module.exports = {
  TaskConfigNodeLoader,
  prepareTaskConfigNodeLoader,
};
