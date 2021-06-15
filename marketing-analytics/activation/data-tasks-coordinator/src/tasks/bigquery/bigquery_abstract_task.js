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
 * @fileoverview BigQuery abstract task class.
 */

'use strict';

const {BigQuery} = require('@google-cloud/bigquery');
const {BaseTask} = require('../base_task.js');

/**
 * String value of BigQuery job status 'done' in BigQuery logging messages.
 * @const {string}
 */
const BIGQUERY_JOB_STATUS_DONE = 'DONE';

/** BigQuery tasks abstract class. */
class BigQueryAbstractTask extends BaseTask {

  /**
   * Returns a default BigQuery instance based on detailed task.
   * @return {!BigQuery}
   */
  getBigQueryForTask() {
    throw new Error('Unimplemented function: getInstanceBigQuery');
  }

  /** @override */
  async isDone() {
    const bigquery = this.getBigQueryForTask();
    const {jobId} = this.jobReference;
    const [job] = await bigquery.job(jobId, this.jobReference).get();
    return job.metadata.status.state === BIGQUERY_JOB_STATUS_DONE;
  }

  /**
   * Gets a BigQuery client library instance.
   * @param {Object<string,string>} options
   * @param {boolean=} external Whether this BigQuery needs to access external
   *     table which is based on Google Sheet. If this is the case, extra API
   *     scopes needs to be set for the BigQuery instance.
   * @return {!BigQuery}
   */
  getBigQuery(options, external = false) {
    const authOptions = {
      projectId: this.getCloudProject(options),
      location: options.location,
    };
    if (external) {
      authOptions.scopes = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/bigquery',
      ];
    }
    return new BigQuery(authOptions);
  }
}

module.exports = {
  BigQueryAbstractTask,
  BIGQUERY_JOB_STATUS_DONE,
};
