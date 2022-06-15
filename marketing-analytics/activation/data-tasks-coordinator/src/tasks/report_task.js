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
 * @fileoverview Task to generate a report.
 */

'use strict';

const {utils: {wait}, storage: {StorageFile}} = require(
    '@google-cloud/nodejs-common');
const {ErrorOptions} = require('../task_config/task_config_dao.js');
const {Report, ReportConfig,} = require('./report/index.js');
const { RetryableError } = require('./error/retryable_error.js');
const { BaseTask } = require('./base_task.js');
const {
  TaskType,
  StorageFileConfig,
} = require('../task_config/task_config_dao.js');

/**
 * @typedef {{
 *   type:TaskType.REPORT,
 *   source:!ReportConfig,
 *   destination:!StorageFileConfig,
 *   appendedParameters:(Object<string,string>|undefined),
 *   errorOptions:(!ErrorOptions|undefined),
 *   next:(string|!Array<string>|undefined),
 * }}
 */
let ReportTaskConfig;

/**
 * To avoid the Cloud Functions hitting a real timeout (9 minutes at maximum),
 * set this run time for a report task. After this, task will actively throw a
 * timeout error.
 *
 */
const TIMEOUT_IN_MILLISECOND = 500000;

/** Generates a report from external system and downloads to Cloud Storage. */
class ReportTask extends BaseTask {

  /** @override */
  isManualAsynchronous() {
    return this.getReport().isAsynchronous();
  }

  /**
   * Starts to generate a report from external system.
   * @override
   */
  doTask() {
    return this.getReport().generate(this.parameters)
        .then((jobInfo) => ({parameters: this.appendParameter(jobInfo)}));
  }

  /** @override */
  isDone() {
    return this.getReport().isReady(this.parameters);
  }

  /** @override */
  async completeTask() {
    /** @type {StorageFileConfig} */
    const destination = this.config.destination;
    const {bucket, name} = destination;
    const report = this.getReport();
    try {
      const content = await report.getContent(this.parameters);
      this.logger.debug('Got result from report');
      const storageFile = StorageFile.getInstance(
          bucket,
          name,
          {
            projectId: destination.projectId,
            keyFilename: destination.keyFilename,
          });
      if (!content.pipe) {
        storageFile.getFile().save(content);
      } else {
        this.logger.debug('Start to output stream to gcs');
        const timeoutWatcher = wait(TIMEOUT_IN_MILLISECOND, false);
        const outputStream = storageFile.getFile().createWriteStream(
          { resumable: false }
        );
        const downloadReport = new Promise((resolve, reject) => {
          content
            .on('end', () => outputStream.emit('finish'))
            .on('error', (error) => outputStream.emit('error', error))
            .pipe(outputStream)
            .on('error', (error) => reject(error))
            .on('finish', () => {
              this.logger.debug('Uploaded to Cloud Storage');
              resolve(true);
            });
        }
        );
        const result = await Promise.race([timeoutWatcher, downloadReport]);
        if (!result) throw new Error('Timeout');
      }
      return {parameters: this.appendParameter({reportFile: {bucket, name,}})};
    } catch (error) {
      if (report.isFatalError(error.toString())) {
        this.logger.error(
          'Fail immediately without retry for ReportTask error: ',
          error.toString());
        throw error;
      } else {
        this.logger.error('Retry for ReportTask error: ', error.toString());
        throw new RetryableError(error.toString());
      }
    }
  }

  /**
   * Returns the Report object of this task.
   * @return {Report} Report instance.
   */
  getReport() {
    return this.options.buildReport(this.config.source);
  }
}

module.exports = {
  ReportConfig,
  ReportTaskConfig,
  ReportTask,
};
