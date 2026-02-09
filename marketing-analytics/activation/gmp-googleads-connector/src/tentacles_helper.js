// Copyright 2019 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//`
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @fileoverview Helper functions for Tentacles.
 */

'use strict';

const {
  pubsub: {EnhancedPubSub},
  storage: {StorageFile},
  firestore: { getFirestoreDatabase },
} = require('@google-cloud/nodejs-common');
const { ApiConfigDao } = require('./api_config/api_config_dao.js');
const {
  ApiConfigItem,
  getApiOnGcs,
  getApiNameList,
} = require('./api_handlers/index.js');
const {
  getTopicNameByApi,
  getAttributes,
  guessTentacles,
} = require('./tentacles.js');

/** @typedef {{string:!ApiConfigItem}|undefined} */
let ApiConfigJsonItem;

/**
 * The JSON objects contains different API configurations. It's organized in
 * API name then configuration name levels. See 'config_api.json.template' for
 * example.
 *
 * @typedef {{
 *   GA:!ApiConfigJsonItem,
 *   MP:!ApiConfigJsonItem,
 *   MP_GA4:!ApiConfigJsonItem,
 *   CM:!ApiConfigJsonItem,
 *   SFTP:!ApiConfigJsonItem,
 *   GS:!ApiConfigJsonItem,
 *   SA:!ApiConfigJsonItem,
 *   ACLC:!ApiConfigJsonItem,
 *   CALL:!ApiConfigJsonItem,
 *   ACM:!ApiConfigJsonItem,
 *   ACA:!ApiConfigJsonItem,
 *   AOUD:!ApiConfigJsonItem,
 *   PB:!ApiConfigJsonItem,
 * }}
 */
let ApiConfigJson;

/**
 * Uploads ApiConfig from a JSON object to Firestore or Datastore with the given
 * ApiConfigFirestore object.
 * @see './config_api.json.template'
 * @param {!ApiConfigJson} updatedConfigs The API configurations JSON object.
 * @param {!ApiConfigDao} apiConfigDao The ApiConfigDao object.
 * @return {!Promise<!Array<!Object<string,boolean>>>} Whether API configuration
 *     updated.
 */
const uploadApiConfigImpl = (updatedConfigs, apiConfigDao) => {
  /**
   * Updates config items for a single Api.
   * @param {string} apiName Api name.
   * @return {!Promise<!Object<string,boolean>>} Whether this API configs
   *     updated.
   */
  const updateConfigsForSingleApi = (apiName) => {
    const configs = updatedConfigs[apiName];
    return Promise
        .all(Object.keys(configs).map((configName) => {
          return updateSingleConfig(configs[configName], configName, apiName);
        }))
        .then((results) => ({[apiName]: !results.includes(false)}));
  };

  /**
   * Updates single Api config item.
   * @param {!ApiConfigItem} configObject Api config object for a specific
   *     configuration.
   * @param {string} configName Config item name.
   * @param {string} apiName Api name.
   * @return {!Promise<boolean>} Whether this API config item updated.
   */
  const updateSingleConfig = async (configObject, configName, apiName) => {
    const result =
      await apiConfigDao.saveConfig(apiName, configName, configObject);
    console.log(`Import Config for API[${apiName}]_config[${configName}]`);
    return result;
  };

  return Promise.all(
      Object.keys(updatedConfigs).map(updateConfigsForSingleApi));
};

/**
 * Uploads ApiConfig from a JSON object to Firestore or Datastore. It will
 * check the Firestore mode automatically.
 * @see './config_api.json.template'
 * @param {!ApiConfigJson} apiConfigJson The API configurations JSON object.
 * @return {!Promise<!Array<!Object<string,boolean>>>} Whether API configuration
 *     updated.
 */
const uploadApiConfig = async (apiConfigJson,
    namespace = process.env['PROJECT_NAMESPACE']) => {
  const regExp = new RegExp(`#PROJECT_NAMESPACE#`, 'g');
  const updatedConfig = JSON.parse(
      JSON.stringify(apiConfigJson).replace(regExp, namespace));
  /**
   * Probes the Google Cloud Project's Firestore mode (Native or Datastore),
   * and uses the result to create an ApiConfig DAO based on Firestore.
   */
  const database = await getFirestoreDatabase();
  console.log('database', database);
  const apiConfigDao = new ApiConfigDao(database, namespace);
  return uploadApiConfigImpl(updatedConfig, apiConfigDao);
};

/**
 * Initializes the subscriptions for the topics that Tentacles will use.
 * Pub/sub subscription won't get the message before its creation. So creating
 * the topics and subscriptions is one of the installation steps. See:
 * https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/create
 *
 * @param {string} topicPrefix The prefix name of Pub/Sub topics.
 * @param {!Array<string>|undefined} apiList The list of APIs need to create
 *     topics and subscriptions.
 * @param {!EnhancedPubSub} pubsub EnhancedPubSub to help create topics and
 *     subscriptions.
 * @return {!Promise<!Array<!Object<string,boolean>>>} Whether subscriptions
 *     created.
 */
const initPubsubImpl = (topicPrefix, apiList, pubsub) => {
  const apis = apiList && apiList.length > 0 ? apiList : getApiNameList();
  console.log(`Start to create Topics for API: ${apis}`);
  return Promise.all(apis.map((apiName) => {
    const topicName = getTopicNameByApi(topicPrefix, apiName);
    const subscriptionName = `${topicName}-holder`;
    return pubsub
        .getOrCreateSubscription(topicName, subscriptionName, {
          ackDeadlineSeconds: 300,
          expirationPolicy: {},
          flowControl: {maxMessages: 1}
        })
        .then((subscription) => {
          console.log(`${topicName} <--- ${subscription.name}`);
          return true;
        })
        .catch((error) => {
          console.error(
              `Creating Topic ${topicName} and Subscription ${
                  subscriptionName}:`,
              error);
          return false;
        })
        .then((result) => ({[topicName]: result}));
  }));
};

/**
 * Initializes the subscriptions for the topics that Tentacles will use.
 * Pub/sub subscription won't get the message before its creation. So creating
 * the topics and subscriptions is one of the installation steps. See:
 * https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/create
 *
 * @param {string} topicPrefix The prefix name of Pub/Sub topics.
 * @param {!Array<string>|undefined} apiList The list of APIs need to create
 *     topics and subscriptions.
 * @return {!Promise<!Array<!Object<string,boolean>>>} Whether subscriptions
 *     created.
 */
const initPubsub = (topicPrefix, apiList) => initPubsubImpl(topicPrefix,
    apiList, new EnhancedPubSub());

/**
 * Invokes 'ApiRequester' of Tentacles to send a local file or a Google Cloud
 * Storage (GCS) file to external API endpoints. The filename convention will
 * be followed.
 *
 * @param {string} namespace Prefix of resources.
 * @param {string} file File name.
 * @param {string|undefined=} bucket Bucket name for a GCS file.
 * @return {!Promise<undefined>}
 */
const localApiRequester = async (namespace, file, bucket = undefined) => {
  const fs = require('fs');
  const attributes = getAttributes(file);
  attributes.gcs = getApiOnGcs().includes(attributes.api).toString();
  console.log(`Test file [${file}] has attributes:`, attributes);
  let messageData;
  if (attributes.gcs === 'true') {
    console.log(`API[${attributes.api}] is GCS based...`);
    if (bucket) {
      console.log(`... and this test is set on GCS file`);
      const storageFile = StorageFile.getInstance(bucket, file);
      const fileExists = await storageFile.getFile().exists();
      if (fileExists) {
        messageData = JSON.stringify({bucket, file});
      } else {
        throw new Error(`${file} doesn't exist on ${bucket}.`);
      }
    } else {
      throw new Error(`Unsupported local file test: ${file}`);
    }
  } else {
    console.log(
      `API[${attributes.api} is PS based, tests based on local file.`);
    if (fs.existsSync(file)) { //local file
      messageData = fs.readFileSync(file).toString();
    } else if (bucket) {
      const storageFile = StorageFile.getInstance(bucket, file);
      const fileExists = await storageFile.getFile().exists();
      if (fileExists) {
        messageData = await storageFile.loadContent(0);
      } else {
        throw new Error(`gs://${bucket}/${file} doesn't exist.`);
      }
    } else {
      throw new Error(`Couldn't find file: ${file}`);
    }
  }
  const context = {eventId: 'test-demo-message'};
  const message = {
    attributes,
    data: Buffer.from(messageData).toString('base64'),
  };
  const callback = () => console.log(`After send out requests from ${file}.`);
  /** @const {Tentacles} */ const tentacles = await guessTentacles(namespace);
  return tentacles.getApiRequester()(message, context, callback);
};

/**
 * Sends out a trigger message to start transport task for a given API.
 * Sometime errors will stop API requester CF sending out the message to
 * trigger next task. This can be fixed by a manual trigger message.
 * This is not an ideal solution, but a possible solution option.
 * @param {string} apiName Name of the target API.
 * @param {string} namespace Prefix of resources.
 * @return {Promise<void>}
 */
const triggerTransport = async (apiName, namespace) => {
  const topic = getTopicNameByApi(namespace, apiName);
  /** @const {Tentacles} */ const tentacles = await guessTentacles(namespace);
  const message = `Trigger tasks for ${apiName}`;
  const messageId = await tentacles.nudge(message, {topic});
  console.log(`${message}, message Id: ${messageId}.`);
}

module.exports = {
  uploadApiConfig,
  uploadApiConfigImpl,
  initPubsub,
  initPubsubImpl,
  localApiRequester,
  triggerTransport,
};
