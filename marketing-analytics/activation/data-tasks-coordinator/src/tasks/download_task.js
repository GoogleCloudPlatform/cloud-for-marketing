// Copyright 2019 Google Inc.
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
 * @fileoverview Download task class.
 */

'use strict';

const { request } = require('gaxios');
const { BaseTask } = require('./base_task.js');
const { storage: { StorageFile } } = require('@google-cloud/nodejs-common');

/**
 *
 * @typedef {{
*   type:TaskType.DOWNLOAD,
*   source: {
*     url: string,
*     userAgent: string|undefined,
*   },
*   destination: !StorageFileConfig,
*   appendedParameters:(Object<string,string>|undefined),
*   next:(string|!Array<string>|undefined),
* }}
*/
let DownloadTaskConfig;

/** Download task class. */
class DownloadTask extends BaseTask {

  async doTask() {
    const content = await this.getContent_(this.parameters);
    /** @const {StorageFileConfig} */
    const destination = this.config.destination;
    const { bucket, name } = destination;
    const storageFile = StorageFile.getInstance(
      bucket,
      name,
      {
        projectId: destination.projectId,
        keyFilename: destination.keyFilename,
      });
    await storageFile.getFile().save(content);
    return { parameters: this.appendParameter({ destination }) };
  }

  /** @override */
  async isDone() {
    return true;
  }

  /** @override */
  async getContent_(parameters) {
    const { url, userAgent } = this.config.source;
    const requestOptions = { url };
    if (userAgent) {
      requestOptions.headers = { 'User-Agent': userAgent };
    }
    const response = await request(requestOptions);
    return response.data;
  }

}

module.exports = {
  DownloadTaskConfig,
  DownloadTask,
};
