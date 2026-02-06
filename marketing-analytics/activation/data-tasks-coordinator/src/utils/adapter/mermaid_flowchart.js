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
 * @fileoverview Adapter for Mermaid Flowchart.
 * @see https://mermaid.js.org/intro/ for more information about Mermaid.
 * @see https://mermaid.js.org/syntax/flowchart.html for Mermaid Flowchart.
 */

const { TaskType } = require('../../task_config/task_config_dao.js');
const { Node } = require('../node_loader/task_log_node_loader.js');


/**
 * @const {string} MERMAID_INK_SERVER The url of Mermaid Ink server. This server
 * is used to show a Mermaid diagram with a url directly. All the Mermaid code
 * is base64 coded in the url, so there is a limit for how large the diagram
 * can be.
 */
const MERMAID_INK_SERVER = 'https://mermaid.ink/img';

/**
 * Different styles to render some parts of the graphic. In some cases, they
 * should be text, while in other cases, they can be fancy icons.
 */
const TEXT_STYLE = {
  STATUS_OK: 'O',
  STATUS_RUNNING: 'R',
  STATUS_ERROR: 'E',
  STATUS_RETRY: 'T',
  MULTIPLE: 'multiple',
  EMBED: 'embed',
};
const ICON_STYLE = {
  STATUS_OK: 'fa:fa-check-circle',
  STATUS_RUNNING: 'fa:fa-spinner',
  STATUS_ERROR: 'fa:fa-exclamation-triangle',
  STATUS_RETRY: 'fa:fa-question-circle',
  MULTIPLE: 'fa:fa-clone',
  EMBED: 'fa:fa-folder-open',
};

/** The default color of different Sentinel tasks. */
const SENTINEL_TASK_STYLE = {
  create_external: 'fill:#fde293',
  load: 'fill:#fde293',
  query: 'fill:#aecbfa',
  export: 'fill:#a8dab5',
  report: 'fill:#d7aefb',
  knot: 'fill:#a1e4f2',
  multiple: 'fill:#a1e4f2',
  speed_controlled: 'fill:#f6aea9',
  execute_notebook: 'fill:#fba9d6',
  invoke_cloud_function: 'fill:#fba9d6',
  notify: 'fill:#fdc69c',
  taskConfig: 'fill:#e8eaed,font-style:italic,stroke-dasharray: 5 5',
  error: 'fill:#f00,color:white,font-weight:bold,stroke:black',
  retry: 'fill:#ff0,color:black,font-weight:bold,stroke:red',
  running: 'fill:#0ff,color:black,font-weight:bold,stroke-width:2px,stroke:black',
  animate: 'stroke-dasharray: 9,5,stroke-dashoffset: 900,animation: dash 25s linear infinite',
};

/**
 * Options to control how to generate Mermaid FlowChart, include:
 *   1. direction, the direction of the Flowchart, e.g. 'TD' or 'LR'.
 *      By default, it is 'TD'.
 *      @see https://mermaid.js.org/syntax/flowchart.html#direction
 *   2. style, the style to render a node or a line of FlowChart as defined
 *      previously as 'TEXT_STYLE' or 'ICON_STYLE'.
 *   3. nolink, whether generates links to Firestore entity for nodes.
 *   4. showTitle, whether show a title if title is available.
 *   5. indentation, to make Mermaid Flowchart code more readable, 'indentation'
 *      can be added to lines of a sub components.
 *   6. classStyle, for Sentinel workflow, different node background colors are
 *      used for different Sentinel tasks.
 * @typedef {{
 *   direction: string|undefined,
 *   style:TEXT_STYLE|ICON_STYLE,
 *   nolink:boolean|undefined,
 *   showTitle:boolean|undefined,
 *   indentation:string,
 *   classStyle:object|undefined,
 * }}
 */
let MermaidOptions;

/**
 * A predefined options to render Mermaid flowchart for ink service which will
 * show a flow chart with a HTTP link with the chart information encoded in the
 * URL.
 * Note: a URL has a length limit. Although this is convenient, it will not work
 * for a big flowchart.
 * @const {MermaidOptions} OPTIONS_INK
 */
const OPTIONS_INK = {
  style: ICON_STYLE,
  nolink: true, // link is not useful in this case.
  showTitle: true,
  indentation: '', //no indentation id required in this case.
  classStyle: SENTINEL_TASK_STYLE,
};

/**
 * A predefined options to render Mermaid flowchart which will give the most
 * details in a Mermaid editor.
 * @see https://mermaid.live/edit
 * @const {MermaidOptions} OPTIONS_DEV
 */
const OPTIONS_DEV = {
  style: ICON_STYLE,
  nolink: false,
  showTitle: true,
  indentation: '  ',
  classStyle: SENTINEL_TASK_STYLE,
};

/**
 * A class stands for a Mermaid Flowchart.
 */
class MermaidFlowChart {

  /**
   * @constructor
   * @param {MermaidOptions} options
   */
  constructor(options = {}) {
    this.options = Object.assign(OPTIONS_DEV, options);
    // An array of strings. Each string is a line of Mermaid code.
    this.codes = [];
    // An array of MermaidSubGraph. Embedded tasks or multiple tasks will be
    // rendered in Mermaid as subgraphs.
    this.subGraphs = [];
  }

  /**
   * Returns the code of a Mermaid chart based on the given array of Nodes.
   *
   * @param {Array<!Node>=} nodes
   * @param {string|undefined} title
   * @return {string}
   */
  getChartFromNodes(nodes = [], title) {
    //Where there are more than 100 nodes, only keep links for error nodes.
    if (nodes.length > 100 && this.options.nolink !== true) {
      console.warn('Remove links for normal nodes as there are too many nodes:',
        nodes.length);
      this.options.nolink = true;
    }
    this.title = title;
    // Map for node Id to a sub graph it belongs to.
    const idBelongsToSubGraph = new Map();
    // Map for a tag (embedded tag or multiple tag) to a sub graph it belongs to.
    const tagToSubGraph = new Map();
    // Map for tag owner's Id and the related sub graph's Id.
    const ownerIdToSubGraphId = new Map();
    nodes.forEach((node) => {
      const {
        taskId, id, parentId,
        tagHeld,
        embeddedTag,
        multipleTag,
      } = node;
      // 'target' is the subgraph or the main graph that contains the current node
      let target;
      if (idBelongsToSubGraph.has(parentId)) {
        // belongs to the current node's parent's graph
        target = idBelongsToSubGraph.get(parentId);
        idBelongsToSubGraph.set(id, target);
      } else if (tagToSubGraph.has(embeddedTag || multipleTag)) {
        // if the current node is embedded or multiplied, use its holder's graph
        // The tag holder should have been proceeded as the nodes are sorted.
        target = tagToSubGraph.get(embeddedTag || multipleTag);
        idBelongsToSubGraph.set(id, target);
      } else {// the node belongs to the main graph
        target = this;
      }

      // A tag (multiple tag or embedded tag) means there needs a new subgraph.
      if (tagHeld) {
        const subgraphId = `${taskId}-${id}`;
        const subgraph = new MermaidSubGraph(this.options, subgraphId);
        tagToSubGraph.set(tagHeld, subgraph);
        ownerIdToSubGraphId.set(id, subgraphId);
        const lineLabel = target.getEdgeLabel(node);
        target.addCode(`${id} ---|${lineLabel}| ${subgraphId}`);
        target.addSubGraph(subgraph);
      }
      // Add Mermaid element for this node.
      const mermaidNode = target.getNodeCode(node);
      if (parentId) {
        // If parent has a sub graph, the link should from the sub graph not parent node
        const sourceId = ownerIdToSubGraphId.has(parentId)
          ? ownerIdToSubGraphId.get(parentId)
          : parentId;
        if (mermaidNode.endsWith('running')) {
          const edgeId = `e${node.id}`;
          target.addCode(`${sourceId} ${edgeId}@--> ${mermaidNode}`);
          target.addCode(`${edgeId}:::animate`);
        } else {
          target.addCode(`${sourceId} --> ${mermaidNode}`);
        }

      } else {
        target.addCode(mermaidNode);
      }
      // Add Firestore link (if possible) to this node.
      target.addLink(node);
    });
    return this.getChartCode().join('\n');
  }

  /**
   * Returns the URL with encoded graph information so it will show a workflow
   * chart when it is opened in a browser.
   * @param {!Array<!Node>} nodes
   * @param {string|undefined} title
   * @return {string}
   */
  getInkLinkFromNodes(nodes, title) {
    const code = this.getChartFromNodes(nodes, title);
    const base64String = Buffer.from(code, 'ascii').toString('base64');
    return `${MERMAID_INK_SERVER}/${base64String}`;
  }

  /**
   * Returns the direction of this chart.
   * Main chart has a default value of 'TD'.
   * @see https://mermaid.js.org/syntax/flowchart.html#direction
   * @return {string}
   */
  getDirection() {
    return this.options.direction || 'TD';
  }

  /**
   * Returns the array of strings that stands for the chart whole definition.
   * Comparing to the `getMainCode()`, the content from this function will have
   * the title and class style definition (if they are available).
   * @return {!Array<string>}
   */
  getChartCode() {
    const result = [];
    if (this.title && this.options.showTitle) {
      result.push(...['---', `title: ${this.title}`, '---']);
    }
    result.push(`flowchart ${this.getDirection()}`);
    const { classStyle, indentation = '' } = this.options;
    if (classStyle) {
      Object.keys(classStyle).forEach((key) => {
        result.push(`${indentation}classDef ${key} ${classStyle[key]}`);
      });
    }
    return result.concat(this.getMainCode());
  }

  /**
   * Returns the array of strings that stands for the chart main definition.
   * @return {!Array<string>}
   */
  getMainCode() {
    return this.codes
      .concat(this.subGraphs.map((subGraph) => subGraph.getChartCode())) //add sub
      .flat()
      .map((line) => `${this.options.indentation}${line}`);
  }

  /**
   * Returns code (a string) for a Node in Mermaid chart. It includes the id,
   * text, node shape and style.
   * @see https://mermaid.js.org/syntax/flowchart.html#node-shapes
   * @param {!Node} node
   * @return {string}
   */
  getNodeCode(node) {
    const { id, taskId, status, type, tagHeld, isTaskConfig } = node;
    const displayId = id;
    let statusTag, style = isTaskConfig ? 'taskConfig' : type;
    switch (status) {
      case 'FINISHED':
        statusTag = this.options.style.STATUS_OK;
        break;
      case 'ERROR':
        statusTag = this.options.style.STATUS_ERROR;
        style = 'error';
        break;
      case 'RETRY':
        statusTag = this.options.style.STATUS_RETRY;
        style = 'retry';
        break;
      case 'INITIAL':
      case 'STARTED':
      case 'FINISHING':
        statusTag = this.options.style.STATUS_RUNNING;
        style = 'running';
        break;
      default:
        statusTag = '';
    }
    const label = `${statusTag} ${taskId}@${displayId}`;
    let mermaidNode;
    if (type === TaskType.MULTIPLE) {
      mermaidNode = `{{${label}}}`;
    } else if (tagHeld) {
      mermaidNode = `[[${label}]]`;
    } else {
      mermaidNode = `[${label}]`;
    }
    return `${id}${mermaidNode}:::${style}`;
  }

  /**
   * Returns a string stands for an edge label in Mermaid chart.
   * @see https://mermaid.js.org/syntax/flowchart.html#links-between-nodes
   * @param {!Node} node
   * @return {string}
   */
  getEdgeLabel(node) {
    const { type, numberOfTasks } = node;
    return type === TaskType.MULTIPLE
      ? `${numberOfTasks} ${this.options.style.MULTIPLE}`
      : this.options.style.EMBED;
  }

  /**
   * Adds a line of code to the chart.
   * @param {string} code
   */
  addCode(code) {
    this.codes.push(code);
  }

  /**
   * Adds a sub graph to the chart.
   * @param {!MermaidSubGraph} subGraph
   */
  addSubGraph(subGraph) {
    this.subGraphs.push(subGraph);
  }

  /**
   * Appends link information to the chart.
   * @param {!Node} node
   */
  addLink(node) {
    const { id, link, status } = node;
    if (!link) return;
    if (this.options.nolink && status === 'FINISHED') return;
    this.addCode(`click ${id} href "${link}" _blank`);
  }
}

/**
 * A class stands for Mermaid sub graph.
 * This is used to present embedded tasks or multiple tasks so they can be
 * clearly grouped together.
 * @see https://mermaid.js.org/syntax/flowchart.html#subgraphs
 */
class MermaidSubGraph extends MermaidFlowChart {

  /**
   * @constructor
   * @param {MermaidOptions} options
   * @param {string} id Id of this sub graph.
   * @param {string=} name Display name of the sub graph. By default it is a
   *   'space' so there is no visible name.
   */
  constructor(options, id, name = ' ') {
    super(options);
    this.id = id;
    this.name = name ? `[${name}]` : '';
  }

  /**
   * It has a different head part (subgraph) comparing to the main chart (title,
   * style, etc.), as well as it has an 'end'.
   * @override
   */
  getChartCode() {
    const header = [
      `subgraph ${this.id} ${this.name}`,
      `${this.options.indentation}direction ${this.getDirection()}`,
    ];
    return header.concat(this.getMainCode()).concat('end');
  }

  /**
   * Sub chart has a default value of 'LR'.
   * @override
   * @see https://mermaid.js.org/syntax/flowchart.html#direction-in-subgraphs
   */
  getDirection() {
    return this.options.direction || 'LR';
  }
}

module.exports = {
  MermaidFlowChart,
  ICON_STYLE,
  TEXT_STYLE,
  OPTIONS_INK,
  OPTIONS_DEV,
  SENTINEL_TASK_STYLE,
};
