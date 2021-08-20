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
 * @fileoverview Helper functions for Sentinel. All the functions in this file
 *     are supposed to be invoked by bash shell for installation or test.
 */

'use strict';

const fs = require('fs');
const {join} = require('path');
const {
  api: {googleads: {GoogleAds}},
  firestore: {DataSource, FirestoreAccessBase},
} = require('@google-cloud/nodejs-common');
const {getSchemaFields} = require('./tasks/report/googleads_report_helper.js');
const {TaskConfigDao} = require('./task_config/task_config_dao.js');
const {guessSentinel} = require('./sentinel.js');

/**
 * Updates Sentinel Task Configs from a JSON object to Firestore or Datastore.
 * TODO define TaskConfigJSON in src/tasks/index.js after 'Tasks' merged.
 * @param {!TaskConfigJSON} taskConfig The module configuration JSON
 *     object.
 * @param {Object<string,string>=} parameters The properties from the
 *     installation process and have been saved in the configuration file
 *     `config.json`. The name of properties could be: `PROJECT_ID`,
 *     `GCS_BUCKET` and `OUTBOUND`, etc. Properties can be used in task configs
 *     as `#PROJECT_ID#` format.
 * @param {string} namespace Namespace for the Firestore.
 * @return {!Promise<!Array<{string:boolean}>>} Results records.
 */
exports.uploadTaskConfig = (taskConfig, parameters = {},
    namespace = process.env['PROJECT_NAMESPACE']) => {
  const tasks = JSON.parse(Object.keys(parameters).reduce(
      (previousValue, key) => {
        const regExp = new RegExp(`#${key}#`, 'g');
        return previousValue.replace(regExp, parameters[key]);
      },
      JSON.stringify(taskConfig)));
  return FirestoreAccessBase.isNativeMode().then((isNative) => {
    const dataSource = (isNative) ? DataSource.FIRESTORE : DataSource.DATASTORE;
    const taskConfigDao = new TaskConfigDao(dataSource, namespace);
    const reduceFn = (previousResults, key) => {
      return previousResults.then((results) => {
        const task = tasks[key];
        return taskConfigDao.update(task, key)
            .then((id) => {
              const taskInfo = `${task.type}[${id}]->[${task.next || ''}]`;
              console.log(`Update Task Config ${taskInfo} successfully.`);
              return true;
            }).catch((error) => {
              console.error(`Failed to update ${task.type}[${id}]`, error);
              return false;
            }).then((thisResult) => results.concat({[key]: thisResult}));
      });
    };
    return Object.keys(tasks).reduce(reduceFn, Promise.resolve([]));
  });
};
/**
 * Checks whether Sentinel can generate BigQuery schema for the report data
 * based on the Google Ads report definition. There could be 3 kinds of error:
 * 1. Non-existent fields. (typo)
 * 2. Embedded fields is not exposed as a GoogleAdsField. In this case, those
 *    fields are not supported in the report definition. Only its ancestor that
 *    is a GoogleAdsField can be in the report definition. However, the report
 *    data would be a JSON string with structure. To support the structure in
 *    BigQuery, an explict definition of the JSON object with required
 *    properties need be put in the file 'googleads_report_helper.js'.
 * 3. Used those 'ancestor' field in previous case but without the structure
 *    definition in 'googleads_report_helper.js'.
 * @param developerToken
 * @param folder
 * @return {Promise<void>}
 */
exports.checkGoogleAdsReports = async (developerToken, folder = './') => {
  //1. Get all fields
  const files = fs.readdirSync(folder)
      .filter((file) => file.endsWith('.json'));
  console.log(`Analyzing json files in folder '${folder}': `, files);
  const fields = [];
  files.forEach((file) => {
    const data = fs.readFileSync(join(folder, file));
    const tasks = JSON.parse(data);
    Object.keys(tasks).filter((taskId) => {
      const task = tasks[taskId];
      if (task.type === 'report') {
        if (task.source.target === 'ADS') {
          const {
            metrics = [],
            segments = [],
            attributes = [],
          } = task.source.config.reportQuery;
          const adsFieldNames = segments.concat(metrics).concat(attributes);
          adsFieldNames.forEach((field) => {
            if (fields.indexOf(field) === -1) fields.push(field);
          });
        }
      }
    });
  });
  //2. Get GoogleAdsFields
  const ads = new GoogleAds(developerToken);
  const adsFields = await ads.searchMetaData(0, fields);
  //3. if there are missing fields
  if (adsFields.length < fields.length) {
    const existentFields = adsFields.map((field) => field.name);
    const missed = fields.filter(
        (filter) => existentFields.indexOf(filter) < 0);
    console.log(`Found ${existentFields.length} fields, missed: `, missed);
    /**
     * For an undeclared field, its ancestor needs to be figure out to guide the
     * definition for schema, in 'googleads_report_helper.js'.
     * @param {!Array<string>} fields
     * @param {Object<string,Array<string>>} previousMappedFields
     * @return {Promise<{}>}
     */
    const analyzeFields = async (fields, previousMappedFields = {}) => {
      /**
       * Parent fields array.
       * By removing the last property of field name to get its parent.
       */
      const trimmedFields = [];
      /** Map of parent fields to children fields. */
      const mappedFields = {};
      fields.forEach((field) => {
        if (field.indexOf('.') === -1) {
          console.log(
              `Error. Could not find '${previousMappedFields[field]
              || field}'.`);
        } else {
          const trimmedField = field.substring(0, field.lastIndexOf('.'));
          if (trimmedFields.indexOf(trimmedField) === -1) {
            trimmedFields.push(trimmedField);
            mappedFields[trimmedField] = previousMappedFields[field] || [field];
          } else {
            mappedFields[trimmedField].push(
                ...(previousMappedFields[field] || [field]));
          }
        }
      });
      const adsFields = await ads.searchMetaData(0, trimmedFields);
      /** Map of Google Ads Type to field names. */
      const mappedAdsFields = {};
      adsFields.forEach((adsField) => {
        const {type_url: typeUrl, name} = adsField;
        if (!mappedAdsFields[typeUrl]) mappedAdsFields[typeUrl] = [];
        mappedAdsFields[typeUrl].push(...mappedFields[name]);
      });
      // If there are still missing fields
      if (adsFields.length < trimmedFields.length) {
        const existentFields = adsFields.map((field) => field.name);
        const missed = trimmedFields.filter(
            (filter) => existentFields.indexOf(filter) < 0);
        const parentMapped = await analyzeFields(missed, mappedFields);
        // Merge parent mapped results to current one.
        Object.keys(parentMapped).forEach((key) => {
          if (!mappedAdsFields[key]) mappedAdsFields[key] = [];
          mappedAdsFields[key].push(...parentMapped[key]);
        });
      }
      return mappedAdsFields;
    }
    const mappedAdsFields = await analyzeFields(missed);
    Object.keys(mappedAdsFields).forEach((key) => {
      console.log(`ERROR: Check its validation or add ${key} to support: `,
          mappedAdsFields[key]);
    });
  } else { // No missing Ads fields
    const adsFieldsMap = {};
    adsFields.forEach((field) => void (adsFieldsMap[field.name] = field));
    try {
      getSchemaFields(fields, adsFieldsMap);
      console.log('OK. There is no missing definition for Google Ads reports.');
    } catch (error) {
      console.error('ERROR: ', error.message);
    }
  }
}

/**
 * Invokes 'start' of a Task directly to start a task from local environment.
 * However, it still expects all resources are available at Cloud , e.g. the
 * configuration of Task in Firestore, or the loaded file is available in Cloud
 * Storage.
 * It won't trigger next tasks because it doesn't save TaskLog.
 *
 * @param {string} taskConfigId
 * @param {string=} parameters JSON string of parameters.
 * @return {!Promise<(string|undefined)>} Task job Id.
 */
exports.startTaskFromLocal = async (taskConfigId, parameters = "{}",
    namespace = 'sentinel') => {
  console.log('Task: ', taskConfigId);
  console.log('Parameters: ', parameters);
  console.log('Namespace: ', namespace);
  console.log('OAUTH2_TOKEN_JSON: ', process.env['OAUTH2_TOKEN_JSON']);
  const sentinel = await guessSentinel(namespace);
  const task = await sentinel.prepareTask(taskConfigId, JSON.parse(parameters));
  const startedTask = await task.start();
  const idDone = await task.isDone();
  if (idDone) {
    console.log('Task is done, continue to finish.', startedTask);
    const newParameters = startedTask.parameters || parameters;
    console.log('new parameters', newParameters);
    const taskToFinish =
        await sentinel.prepareTask(taskConfigId, JSON.parse(newParameters));
    const output = await taskToFinish.finish();
    console.log('After finish: ', output);
  } else {
    console.log('Task is done yet.');
  }
};

/**
 * Sends a message to Sentinel monitor topic to start a task manually in Google
 * Cloud.
 * It just like the cronjob or event triggered tasks.
 *
 * @param {string} taskConfigId TaskConfigId
 * @param {string=} parameters JSON string of parameters.
 * @param {string=} namespace
 * @return {!Promise<string>} The start task message Id.
 */
exports.startTaskThroughPubSub = (taskConfigId, parameters = '{}',
    namespace = 'sentinel') => {
  console.log('Task: ', taskConfigId);
  console.log('Parameters: ', parameters);
  console.log('Namespace: ', namespace);
  return guessSentinel(namespace).then((sentinel) =>
          sentinel.taskManager.sendTaskMessage(parameters, {taskId: taskConfigId}))
      .then((messageId) => {
        console.log('Sent out message Id:', messageId);
      });
};
