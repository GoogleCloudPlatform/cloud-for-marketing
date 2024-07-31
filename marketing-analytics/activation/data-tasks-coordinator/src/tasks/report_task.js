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

const { createGzip, gzip } = require('zlib');
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
  async doTask() {
    const jobInfo = await this.getReport().generate(this.parameters);
    return { parameters: this.appendParameter(jobInfo) };
  }

  /** @override */
  async isDone() {
    const report = this.getReport();
    try {
      const result = await report.isReady(this.parameters);
      return result;
    } catch (error) {
      this.triageError_(error);
    }
  }

  /** @override */
  async completeTask() {
    /** @type {StorageFileConfig} */
    const destination = this.config.destination;
    const {bucket, name} = destination;
    const report = this.getReport();
    try {
      const content = await report.getContent(this.parameters);
      if (content === '') { // Empty report
        this.logger.warn('Got empty report', this.parameters);
        return {
          parameters: this.appendParameter({ reportFile: 'EMPTY_REPORT' }),
        };
      }
      this.logger.debug('Got result from report');
      const storageFile = StorageFile.getInstance(
          bucket,
          name,
          {
            projectId: destination.projectId,
            keyFilename: destination.keyFilename,
          });
      let downloadReport;
      if (!content.pipe) {
        this.logger.debug('Start to write string to gcs');
        if (this.needCompression_(name)) {
          downloadReport = new Promise((resolve, reject) => {
            gzip(content, ((error, buffer) => {
              if (error) reject(error);
              resolve(storageFile.getFile().save(buffer));
            }));
          });
        } else {
          downloadReport = storageFile.getFile().save(content);
        }
        downloadReport = downloadReport.then(() => true);
      } else {
        this.logger.debug('Start to output stream to gcs');
        const outputStream = storageFile.getFile().createWriteStream(
          { resumable: false }
        );
        const inputStream = this.needCompression_(name)
          ? content.pipe(createGzip())
          : content;
        downloadReport = new Promise((resolve, reject) => {
          inputStream
            .on('end', () => outputStream.end())
            .on('error', (error) => outputStream.emit('error', error))
            .pipe(outputStream)
            .on('error', (error) => reject(error))
            .on('finish', () => {
              this.logger.debug('Uploaded to Cloud Storage');
              resolve(true);
            });
        }
        );
      }
      const timeoutWatcher = wait(TIMEOUT_IN_MILLISECOND, false);
      const result = await Promise.race([timeoutWatcher, downloadReport]);
      if (!result) throw new Error('Timeout');
      return {parameters: this.appendParameter({reportFile: {bucket, name,}})};
    } catch (error) {
      this.triageError_(error);
    }
  }

  /**
   * Returns the Report object of this task.
   * @return {Report} Report instance.
   */
  getReport() {
    if (!this.report) {
      this.report = this.options.buildReport(this.config.source);
    }
    return this.report;
  }

  /**
   * Returns the schema of current report's data structure to help BigQuery load
   * the data into Table.
   * @return {!BqTableSchema} BigQuery load schema, see:
   *     https://cloud.google.com/bigquery/docs/schemas
   */
  generateSchema() {
    return this.getReport().generateSchema(this.parameters);
  }

  /**
   * Returns whether the report need to be compressed.
   * @param {string} name File name.
   * @return {boolean}
   * @private
   */
  needCompression_(name) {
    return name.toLowerCase().endsWith('.gz')
  }

  /**
   * If the error is not fatal (e.g. not an auth error), the task might worth a
   * retry. In this case, this function will throw a wrapped 'RetryableError'
   * error so the following codes will pick it up and retry; otherwise, the
   * normal error will be thrown out.
   * @param {Error} error
   * @private
   */
  triageError_(error) {
    const report = this.getReport();
    if (report.isFatalError(error.toString())) {
      this.logger.error(
        'Fail immediately without retry for ReportTask error: ', error);
      throw error;
    } else {
      this.logger.error('Retry for ReportTask error: ', error);
      throw new RetryableError(error.toString());
    }
  }
}

module.exports = {
  ReportConfig,
  ReportTaskConfig,
  ReportTask,
};
