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
 * @fileoverview A loader class for TaskLog.
 */
const lodash = require('lodash');
const {
  firestore: { DEFAULT_DATABASE, DataSource, Database, getFirestoreDatabase },
} = require('@google-cloud/nodejs-common');
const { TaskConfigDao, TaskType } = require('../../task_config/task_config_dao');
const { TaskLogDao } = require('../../task_log/task_log_dao');

/**
 * @const {number} DEFAULT_LEVEL The default value of a node level. In a graph
 * of a work flow. The task has a less (-1) level value comparing to tasks it
 * triggers. 'Trigger' here means 'next' tasks, 'embedded' tasks or 'multiple'
 * tasks.
 */
const DEFAULT_LEVEL = 10;

/**
 * The type for a node of a work flow. Different visualization solutions can
 * convert an array of nodes into their own code to render it as a graph.
 *
 *  `id` is the TaskLog Id.
 *  `level` is the relative relationship of nodes in the work flow.
 *  `status` the status of TaskLog.
 *  `parameters` the JSON string of this TaskLog's parameter object. There are
 *     multipleTag, embeddedTag, etc. available in parameter which will be used
 *     to connect tasks.
 *  `link` the link to GCP console for the TaskLog entity.
 *
 *  `type` the type of TaskConfig.
 *  `taskId` is the TaskConfig Id.
 *
 *  `parentId` is the Id of the TaskLog that triggered this TaskLog.
 *  `tagHeld`, a knot(embedded) task or multiple task has a tag to be used to
 *     connect to all tasks that it triggers. These tags are called `Embedded
 *     Tag` or `Multiple Tag`. For simplicity, those tags are saved as the
 *     `tagHeld` of the owner node.
 *
 *  `numberOfTasks` is the number of its multiple tasks.
 *
 *  `embeddedTag` the tag of a task that is embedded in other task.
 *  `multipleTag` the tag of a task that belongs to a multiple task.
 *
 * @typedef {{
 *   id: string|number,
 *   level: number,
 *   status: string,
 *   parameters: string,
 *   link: string|undefined,
 *   type: string,
 *   taskId: string,
 *   parentId: string|number|undefined,
 *   tagHeld: string|number,
 *   numberOfTasks: number|undefined,
 *   embeddedTag: string|undefined,
 *   multipleTag: string|undefined,
 * }}
 */
let Node;

/**
 * @const {Array<string>} TASK_LOG_PROPERTIES The array of properties that will
 * be extracted from TaskLog entities. These properties will be used to generate
 * information of a node.
 */
const TASK_LOG_PROPERTIES = [
  'taskId',
  'parentId',
  'status',
  'parameters',
  'embeddedTag',
  'multipleTag',
];

/**
 * Gets a node from a TaskLog entity.
 * @param {{id:string|number, entity:object}} param0, id is the TaskLog Id.
 *     entity is the TaskLog entity.
 * @return {!Node}
 */
function getNodeFromTaskLog({ id, entity }) {
  if (entity) {
    return lodash.merge({ id }, lodash.pick(entity, TASK_LOG_PROPERTIES));
  }
  throw new Error('TaskLog does not exist');
}

/**
 * Gets the function to generate a link to the Firestore document (TaskLog or
 * TaskConfig) directly with a specified Id.
 * Only Firestore (native mode) supports this feature.
 * @param {string} kind 'TaskLog' or 'TaskConfig'.
 * @param {!DataSource} dataSource 'firestore' or 'datastore'.
 * @param {string} databaseId Firestore database Id, default value '(default)'.
 * @param {string} namespace
 * @param {string} projectId
 * @return {function(string|number):(string|undefined)}
 */
function getLinkFunction(kind, dataSource, databaseId, namespace, projectId) {
  /**
   * Gets the link to Google Cloud Console of the Firestore entity with given Id.
   * @param {string|number} id
   * @return {string|undefined}
   */
  return (id) => {
    // Datastore doesn't support a direct link to an entity.
    if (dataSource === DataSource.FIRESTORE) {
      const database =
        databaseId === DEFAULT_DATABASE ? '-default-' : databaseId;
      const linkSegments = [
        'https://console.cloud.google.com',
        `firestore/databases/${database}`,
        `data/panel/${namespace}/database/${kind}`,
        `${id}?project=${projectId}`,
      ];
      return linkSegments.join('/');
    }
  }
}

/**
 * This class loads task logs of a workflow and convert them into a sorted
 * array of nodes which can be converted to other formats for different
 * virtualization solutions.
 */
class TaskLogNodeLoader {

  /**
   * @constructor
   * @param {!TaskLogDao} taskLogDao
   * @param {!TaskConfigDao} taskConfigDao
   * @param {string=} projectId
   */
  constructor(taskLogDao, taskConfigDao,
    projectId = process.env['GCP_PROJECT']) {
    this.taskLogDao = taskLogDao;
    this.taskConfigDao = taskConfigDao;
    this.projectId = projectId;
    this.namespace = this.taskLogDao.namespace;
    this.dataSource = this.taskLogDao.dataSource;
    this.databaseId = this.taskLogDao.databaseId || DEFAULT_DATABASE;
    // A cache map for tasks' type. A TaskLog entity only has 'taskId' which can
    // be used to load the TaskConfig which has the task 'type'. Use this map
    // to cache the tasks' types.
    this.taskConfigTypeCacheMap = new Map();
    this.getLink = getLinkFunction('TaskLog',
      this.dataSource, this.databaseId, this.namespace, this.projectId);
  }

  /**
   * Returns an array of nodes present a workflow sorted by the level of node.
   * @param {string|number} taskLogId
   * @return {!Array<!Node>}
   */
  async getWorkFlow(taskLogId) {
    const nodeMap = await this.loadNodes(taskLogId);
    return [...nodeMap.values()].sort(({ level: a }, { level: b }) => a - b);
  }

  /**
   * Returns a map of TaskLogId as the key and Node as the value for a workflow.
   * This is the main function to load the whole work flow. The given taskLogId
   * is expected to be one of the tasks on 'main' workflow which means it should
   * not belong to any multiple tasks or embedded tasks.
   * Besides the given taskLog, this function it will also load following other
   * TaskLogs in a recursion way:
   * 1. its multiple sub taskLogs if this is a multiple task. It will only
   *    load ONE sub taskLog because there might be too many instances.
   * 2. all embedded taskLogs if this is an embedded task.
   * 3. all its children taskLogs.
   * 4. its parent taskLog if there is a parentId.
   *
   * @param {string|number} taskLogId
   * @param {number=} level
   * @param {!Map=} nodeMap
   * @return {!Map<string|number, Node>}
   */
  async loadNodes(taskLogId, level = DEFAULT_LEVEL, nodeMap = new Map()) {
    //To prevent loop, quit if the node has been loaded.
    if (nodeMap.has(taskLogId)) return;
    // Current node
    const currentNode = await this.getNode(taskLogId);
    nodeMap.set(taskLogId, currentNode);
    currentNode.type = await this.getTaskType(currentNode.taskId);
    currentNode.link = this.getLink(taskLogId);
    currentNode.level = level;
    // Load its multiple sub task flow if this is a multiple task.
    const { multipleTag, numberOfTasks, embeddedTag }
      = JSON.parse(currentNode.parameters || '{}');
    if (currentNode.type === TaskType.MULTIPLE && multipleTag) {
      currentNode.tagHeld = multipleTag;
      currentNode.numberOfTasks = numberOfTasks;
      if (numberOfTasks > 0) {
        const { taskId, id: multipleTaskLogId } =
          (await this.getNodesFromMultipleTask(multipleTag))[0];
        await this.loadNodes(multipleTaskLogId, level + 1, nodeMap);
      } else {
        console.warn('0 multiple task log.');
      }
    }
    // Load its all embedded taskLogs if this is an embedded task.
    if (currentNode.type === TaskType.KNOT && embeddedTag) {
      currentNode.tagHeld = embeddedTag;
      const embeddedTasks = await this.getNodesFromEmbeddedTask(embeddedTag);
      await Promise.all(
        embeddedTasks.map(({ id }) => this.loadNodes(id, level + 1, nodeMap))
      );
    }
    // Load its children(next) taskLogs.
    const children = await this.getNodesFromChildren(taskLogId);
    if (children.length > 0) {
      await Promise.all(
        children.map(({ id }) => this.loadNodes(id, level + 1, nodeMap))
      );
    }
    // Load its parent taskLog if there is a parentId.
    if (currentNode.parentId) {
      await this.loadNodes(currentNode.parentId, level - 1, nodeMap);
    }
    return nodeMap;
  }

  /**
   * Gets the type of a task with the given task Id.
   * @param {string} taskId
   * @return {string}
   */
  async getTaskType(taskId) {
    if (!this.taskConfigTypeCacheMap.has(taskId)) {
      const config = await this.taskConfigDao.load(taskId);
      this.taskConfigTypeCacheMap.set(taskId,
        config ? config.type : `unknown_task_${taskId}`);
    }
    return this.taskConfigTypeCacheMap.get(taskId);
  }

  /**
   * Gets a Node from a TaskLog with the given TaskLog Id.
   * @param {string|number} taskLogId
   * @return {!Node}
   */
  async getNode(taskLogId) {
    const node = await this.taskLogDao.load(taskLogId);
    return getNodeFromTaskLog({ id: taskLogId, entity: node });
  }

  /**
   * Gets an array of nodes from the multiple tasks with a given multiple tag.
   * By default, it only get one multiple tasks as an example.
   * @param {string} tag
   * @param {number=} limit The number of nodes that would be returned.
   * @return {!Array<Node>}
   */
  async getNodesFromMultipleTask(tag, limit = 1) {
    const nodes = await this.taskLogDao.list(
      [{ property: 'multipleTag', value: tag }], undefined, limit);
    return nodes.map(getNodeFromTaskLog);
  }

  /**
   * Gets an array of nodes from the embedded tasks with a given embedded tag.
   * @param {string} tag
   * @return {!Array<Node>}
   */
  async getNodesFromEmbeddedTask(tag) {
    const nodes =
      await this.taskLogDao.list([{ property: 'embeddedTag', value: tag }]);
    return nodes.map(getNodeFromTaskLog);
  }

  /**
   * Gets an array of nodes with a given parent TaskLog Id.
   * @param {string|number} parentId
   * @return {!Array<Node>}
   */
  async getNodesFromChildren(parentId) {
    const nodes =
      await this.taskLogDao.list([{ property: 'parentId', value: parentId }]);
    return nodes.map(getNodeFromTaskLog);
  }

}

/**
 * Returns a TaskLogNodeLoader instance based on the given GCP project and
 * namespace.
 * @param {string=} namespace
 * @param {string=} projectId
 * @param {string=} databaseId
 * @return {!TaskLogNodeLoader}
 */
async function prepareTaskLogNodeLoader(namespace = 'sentinel',
  projectId = process.env['GCP_PROJECT'],
  databaseId = process.env['DATABASE_ID']) {
  /** @type {!Database} */
  const database = await getFirestoreDatabase(projectId, databaseId);
  const taskLogDao = new TaskLogDao(database, namespace, projectId);
  const taskConfigDao = new TaskConfigDao(database, namespace, projectId);
  return new TaskLogNodeLoader(taskLogDao, taskConfigDao, projectId);
}

module.exports = {
  Node,
  TaskLogNodeLoader,
  prepareTaskLogNodeLoader,
  getLinkFunction,
};
