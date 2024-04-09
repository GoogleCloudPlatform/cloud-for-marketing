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
 * @fileoverview GMC Webpage fetcher class.
 */

'use strict';

const { JSDOM } = require("jsdom");
const { request } = require('gaxios');
const {
  utils: { apiSpeedControl, getProperValue },
  storage: { StorageFile },
} = require('@google-cloud/nodejs-common');
const { BaseTask } = require('../base_task.js');

const QUERIES_PER_SECOND = 20;
const NUMBER_OF_THREADS = 20;

/**
 *
 * @typedef {{
*   type:TaskType.GMC_WEBPAGE_FETECHER,
*   source: {
*     userAgency: string|undefined,
*     option: {
*       numberOfThreads:number|undefined,
*       qps::number|undefined,
*     }
*   },
*   destination: !StorageFileConfig,
*   appendedParameters:(Object<string,string>|undefined),
*   next:(string|!Array<string>|undefined),
* }}
*/
let GmcWebpageFetchTaskConfig;

/** GMC Webpage fetcher class. */
class GmcWebpageFetch extends BaseTask {

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
    return { destination };
  }

  /** @override */
  async isDone() {
    return true;
  }

  /** @override */
  async getContent_(parameters) {
    const records = parameters.records.split('\n');
    const { userAgent, option = {} } = this.config.source;
    const numberOfThreads =
      getProperValue(option.numberOfThreads, NUMBER_OF_THREADS, false);
    const qps = getProperValue(option.qps, QUERIES_PER_SECOND, false);
    const mergeBatchResults = (batchResults) => batchResults;
    const managedSend = apiSpeedControl(
      1, numberOfThreads, qps, mergeBatchResults);

    const fetchSinglePage = async (line, batchId) => {
      const { link, id } = JSON.parse(line);
      const requestOptions = { url: link };
      if (userAgent) {
        requestOptions.headers = { 'User-Agent': userAgent };
      }
      const result = { id, retry: 0, error: '' };
      let retried = false;
      while (true) {
        try {
          const response = await request(requestOptions);
          const dom = new JSDOM(response.data);
          const offer =
            dom.window.document.querySelector('[itemtype="http://schema.org/Offer"]');
          for (let i = 0; i < offer.children.length; i++) {
            const key = offer.children[i].getAttribute('itemprop');
            const value = offer.children[i].getAttribute('content').trim();
            result[key] = value;
          }
          break;
        } catch (error) {
          if (result.retry < 3) {
            result.retry += 1;
            continue;
          }
          result.error = error.message || JSON.stringify(error);
          this.logger.error(result);
          break;
        }
      }
      return JSON.stringify(result);
    };
    const results = await managedSend(fetchSinglePage, records, '');
    return results.join('\n');
  }
}

module.exports = {
  GmcWebpageFetchTaskConfig,
  GmcWebpageFetch,
};
