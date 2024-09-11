// Copyright 2023 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this fileAccessObject except in compliance with the License.
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
 * @fileoverview A class to send out notification of the data pipeline.
 */

'use strict';

const { api: {
  gmail: { Gmail, EmailOptions, },
  sendgrid: { SendGrid },
} } = require('@google-cloud/nodejs-common');
const { BaseTask } = require('../base_task.js');
const { TaskType } = require('../../task_config/task_config_dao.js');
const { TaskLogNodeLoader } =
  require('../../utils/node_loader/task_log_node_loader.js');
const {
  MermaidFlowChart,
  OPTIONS_DEV,
  OPTIONS_INK,
} = require('../../utils/adapter/mermaid_flowchart.js');

/**
 * The configuration of a notification task.
 * Currently it support two emails notification: gmail and sendgrid.
 * 'gmail' is the default and it is based on an OAuth token (a token file or a
 * secret in Secret Manager). So the sender is fixed to the owner of the OAuth
 * token (but it can have a specific name, e.g. "Notification <abc@xyz.com>"");
 * 'sendgrid' needs a 'apiKey' which can be set up in SendGrid.
 * @see https://cloud.google.com/security-command-center/docs/how-to-enable-real-time-notifications#setting_up_a_messaging_app
 *
 * @typedef {{
 *   type:TaskType.NOTIFY,
 *   method: {
 *     target: 'gmail'|'sendgrid'|undefined,
 *     secretName: string|undefined,
 *     apiKey: string|undefined,
 *   },
 *   message: !EmailOptions,
 *   appendedParameters:(Object<string,string>|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let NotifyTaskConfig;

/** @const{string} A link to a flow chart rendered by Mermaid ink. */
const TEMPLATE_IMGAE_LINK = '{WORKFLOW_MERMAID_INK_LINK}';

/** @const{string} A text for a Mermaid flow chart code. */
const TEMPLATE_MERMAID_CODE = '{WORKFLOW_MERMAID_DEV_CODE}';

/**
 * A notify task class.
 * Currently, it supports sending email through Gmail or SendGrid.
 * By default, it is Gmail.
 */
class NotifyTask extends BaseTask {

  async doTask() {
    const { target = 'gmail', secretName, apiKey } = this.config.method;
    const options = {};
    if (secretName) options.SECRET_NAME = secretName;
    const { message } = this.config;
    message.content = await this.getContent_(message.content);

    if (target === 'gmail') {
      const gmail = new Gmail(options);
      return gmail.sendSimpleEmail(message);
    }
    if (target === 'sendgrid') {
      const sendGrid = new SendGrid(apiKey);
      return sendGrid.sendMail(message);
    }
    throw new Error('Unknown notify method', this.config);
  }

  /** @override */
  async isDone() {
    return true;
  }

  /**
   * Gets the email content. If there are placeholders for work flow, the
   * placeholders will be replaced with real values.
   * @param {string} content
   * @return {string}
   */
  async getContent_(content) {
    let result = content;
    if (content.indexOf(TEMPLATE_IMGAE_LINK) > -1
      || content.indexOf(TEMPLATE_MERMAID_CODE) > -1) {
      const { taskLogId } = this.parameters;
      const { taskLogDao, taskConfigDao } = this.options;
      const loader =
        new TaskLogNodeLoader(taskLogDao, taskConfigDao, this.getCloudProject());
      const nodes = await loader.getWorkFlow(taskLogId);
      if (content.indexOf(TEMPLATE_IMGAE_LINK) > -1) {
        const mermaid = new MermaidFlowChart(OPTIONS_INK);
        const ink = mermaid.getInkLinkFromNodes(nodes);
        result = result.replace(TEMPLATE_IMGAE_LINK, ink);
      }
      if (content.indexOf(TEMPLATE_MERMAID_CODE) > -1) {
        const mermaid = new MermaidFlowChart(OPTIONS_DEV);
        const code = mermaid.getChartFromNodes(nodes);
        result = result.replace(TEMPLATE_MERMAID_CODE, code);
      }
    }
    return result;
  }

}

module.exports = {
  NotifyTaskConfig,
  NotifyTask,
};
