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

/** @fileoverview Sentinel Cronjob sheet.*/

/**
 * Type of the object that each row of the sheet can be mapped to.
 *
 * @typedef {{
*   jobId: string,
*   description: string,
*   schedule: string,
*   taskId: string,
*   message: string,
* }}
*/
let SentinelCronJobRowEntity;

/**
 * The sheet stores Sentinel cronjob configurations which will be uploaded to
 * Cloud Scheduler.
 */
class SentinelCronJob extends PlainSheet {

  /**
   * In the parameter `options`, the property named 'jobs' is for intial data.
   * @param {!Array<Object<string,!Object>>} jobs An array of jobs. Each job
   *   is an object with column fields as the properties.
   */
  get initialData() {
    const { jobs = [] } = this.options || {};
    return jobs.map((job) => {
      return this.fields.map((key) => {
        const value = job[key];
        return typeof value === 'object' ?
          JSON.stringify(value, null, 2) : value;
      });
    });
  }

  get defaultSheetName() {
    return 'Sentinel CronJob';
  }

  get columnConfiguration() {
    return [
      { name: 'Job Id', defaultNote: true, format: COLUMN_STYLES.ALIGN_MIDDLE },
      {
        name: 'Description', width: 300,
        format: [COLUMN_STYLES.ALIGN_MIDDLE, { fn: 'setWrap', format: true }],
      },
      { name: 'Schedule', format: COLUMN_STYLES.ALIGN_MIDDLE },
      { name: 'Task Id', format: COLUMN_STYLES.ALIGN_MIDDLE },
      { name: 'Message', width: 400, jsonColumn: true },
      {
        name: COLUMN_NAME_FOR_DEFAULT_CONFIG, width: 200,
        format: COLUMN_STYLES.MONO_FONT,
      },
    ];
  }

  get inherentMenuItems() {
    return [
      {
        name: 'Update selected job to Cloud Scheduler',
        method: 'operateSingleRow',
      },
      { name: 'Force run selected selected job', method: 'runJob' },
      { separator: true },
      { name: 'Update all jobs to Cloud Scheduler', method: 'operateAllRows' },
      { name: 'Reset sheet (will lose monification)', method: 'initialize' },
    ];
  }

  /**
   * Uploads cronjobs to Cloud Scheduler.
   * @override
   * @param {!Array<!SentinelCronJobRowEntity>} jobs
   * @return {!Array<string>}
   */
  processResources(jobs) {
    const properties = getDocumentProperties();
    const { projectId, locationId, timeZone, namespace } = properties;
    const cloudScheduler = new CloudScheduler(projectId, locationId);
    const topicName = sentinel.getMonitorTopicName(namespace);
    return jobs
      .map(({ jobId: rawId, description, schedule, taskId, message }) => {
        const jobId = replaceVariables(rawId, properties);
        const pubsubTarget = {
          topicName: `projects/${projectId}/topics/${topicName}`,
          attributes: { taskId },
          data: Utilities.base64Encode(replaceVariables(message, properties)),
        };
        const options = Object.assign(
          { timeZone: 'Etc/UTC' },
          { description, schedule, timeZone, pubsubTarget });
        return cloudScheduler.createOrUpdateJob(jobId, options);
      })
      .map(({ userUpdateTime }) => {
        return `Has been updated to Cloud Scheduler at ${userUpdateTime}`;
      });
  }

  /**
   * Starts the selected CronJob.
   */
  runJob() {
    const rowIndex = this.getSelectedRow();
    const { jobId: rawId } = this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    const properties = getDocumentProperties();
    const projectId = getDocumentProperty('projectId');
    const locationId = getDocumentProperty('locationId');
    const jobId = replaceVariables(rawId, properties);
    const cloudScheduler = new CloudScheduler(projectId, locationId);
    const { error, lastAttemptTime } = cloudScheduler.runJob(jobId);
    const note = `Last attempt time: ${lastAttemptTime}`;
    const fontColor = error ? 'red' : 'black';
    this.showOperationResult(rowIndex, note, fontColor, 'Message');
  }

}
