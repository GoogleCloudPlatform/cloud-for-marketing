// Copyright 2019 Google Inc.
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
 * @fileoverview Task base class.
 */

'use strict';
const {BigQuery} = require('@google-cloud/bigquery');
const {Storage} = require('@google-cloud/storage');
const {utils: {replaceParameters}} = require('nodejs-common');

class BaseTask {

  constructor(config, parameters = {}) {
    this.parameters = parameters;
    this.config = JSON.parse(replaceParameters(JSON.stringify(config), this.parameters));
  }

  start() {
    return this.doTask().then((jobId) => {
      return this.afterStart(jobId);
    });
  }

  /**
   * @return {!Promise<string|undefined>}
   * @abstract
   */
  doTask() {
    console.log('Base Task class, print task: ', this.config);
    return Promise.resolve(undefined);
  }

  afterStart(jobId) {
    if (jobId) return jobId;
    console.log('Empty Job Id for task:', this.config);
  }

  /**
   * Gets the Cloud Project Id from the the options with key 'projectId'. If it
   * doesn't exist, use the environment variable 'GCP_PROJECT'.
   * @param {Object<string,String>} options
   * @return {string} Cloud Project Id.
   */
  getCloudProject(options) {
    if (options && options.projectId) return options.projectId;
    return process.env['GCP_PROJECT'];
  }

  getBigQuery(options) {
    return new BigQuery({projectId: this.getCloudProject(options)});
  }

  getStorage(options) {
    return new Storage({projectId: this.getCloudProject(options)});
  }

}

exports.BaseTask = BaseTask;
