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

/**
 * @fileoverview Task to convert GMC XML feed to JSONL.
 */

'use strict';

const { xml2js } = require('xml-js');
const { Bucket, File } = require('@google-cloud/storage');
const { storage: { StorageFile } } = require('@google-cloud/nodejs-common');
const {
  TaskType,
  StorageFileConfig,
} = require('../../task_config/task_config_dao.js');

const { BaseTask } = require('../base_task.js');

/**
 *
 * @typedef {{
*   type:TaskType.GMC_XML_TO_JSONL,
*   source: !StorageFileConfig,
*   destination: !StorageFileConfig,
*   appendedParameters:(Object<string,string>|undefined),
*   next:(string|!Array<string>|undefined),
* }}
*/
let GmcXmlFeedToJsonlTaskConfig;

class GmcXmlFeedToJsonlTask extends BaseTask {

  /** @override */
  async doTask() {
    /** @type {StorageFileConfig} */
    const destination = this.config.destination;
    const { bucket, name } = destination;
    const storageFile = StorageFile.getInstance(
      bucket,
      name,
      {
        projectId: destination.projectId,
        keyFilename: destination.keyFilename,
      });
    /** @const {!Array<!File>} */
    const content = await this.getContent_();
    await storageFile.getFile().save(content);
    return { parameters: this.appendParameter({ destination }) };
  }

  /** @override */
  async isDone() {
    return true;
  }

  /**
   * @return {!Promise<!string>}
   * @private
   */
  async getContent_() {
    /** @const {StorageFileConfig} */
    const { bucket, name, projectId } = this.config.source;
    const xml = await StorageFile.getInstance(bucket, name,
      { projectId: this.getCloudProject() }).loadContent(0);
    const data = xml2js(xml, { compact: true });
    const items = data.rss.channel.item.map((item) => {
      const result = {};
      Object.keys(item).forEach((key) => {
        const keyFragments = key.split(':');
        const newKey = keyFragments.length > 1
          ? keyFragments[keyFragments.length - 1] : key;
        result[newKey] = item[key]._text;
      });
      return JSON.stringify(result);
    })
    return items.join('\n');
  }
}

module.exports = {
  GmcXmlFeedToJsonlTaskConfig,
  GmcXmlFeedToJsonlTask,
};
