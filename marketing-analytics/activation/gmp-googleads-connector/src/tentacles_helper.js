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
  firestore: {DataSource, FirestoreAccessBase},
} = require('@google-cloud/nodejs-common');
const {ApiConfigOnFirestore, ApiConfigJson, getApiConfig} = require(
    './api_config/index.js');
const {ApiConfigItem, getApiNameList} = require('./api_handlers/index.js');
const {
  getTopicNameByApi,
  getAttributes,
  getTentacles,
  guessTentacles,
} = require('./tentacles.js');

/**
 * Uploads ApiConfig from a JSON object to Firestore or Datastore with the given
 * ApiConfigFirestore object.
 * @see './config_api.json.template'
 * @param {!ApiConfigJson} updatedConfigs The API configurations JSON object.
 * @param {!ApiConfigOnFirestore} apiConfig The ApiConfigOnFirestore object.
 * @return {!Promise<!Array<!Object<string,boolean>>>} Whether API configuration
 *     updated.
 */
const uploadApiConfigImpl = (updatedConfigs, apiConfig) => {
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
  const updateSingleConfig = (configObject, configName, apiName) => {
    return apiConfig.saveConfig(apiName, configName, configObject)
        .then((result) => {
          console.log(
              `Import Config for API[${apiName}]_config[${configName}]`);
          return result;
        });
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
const uploadApiConfig = (apiConfigJson,
    namespace = process.env['PROJECT_NAMESPACE']) => {
  const regExp = new RegExp(`#PROJECT_NAMESPACE#`, 'g');
  const updatedConfig = JSON.parse(
      JSON.stringify(apiConfigJson).replace(regExp, namespace));
  /**
   * Probes the Google Cloud Project's Firestore mode (Native or Datastore),
   * and uses the result to create an ApiConfig DAO based on Firestore.
   */
  return FirestoreAccessBase.isNativeMode()
      .then(
          (isNative) => (isNative) ? DataSource.FIRESTORE : DataSource.DATASTORE
      )
      .then((dataSource) => getApiConfig(dataSource, namespace))
      .then((apiConfig) => uploadApiConfigImpl(updatedConfig, apiConfig));
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
 * @param {string} file File name.
 * @param {string|undefined=} bucket Bucket name for a GCS file.
 * @return {!Promise<undefined>}
 */
const localApiRequester = (file, bucket = undefined) => {
  const fs = require('fs');
  const attributes = getAttributes(file);
  console.log(`Test file [${file}] has attributes:`, attributes);
  const getMessageData = new Promise((resolve, reject) => {
    if (attributes.gcs === 'true') {
      console.log(`API[${attributes.api}] is GCS based...`);
      if (bucket) {
        console.log(`... and this test is set on GCS file`);
        const storageFile = new StorageFile.getInstance(bucket, file);
        return storageFile.getFile().exists().then((fileExists) => {
          if (fileExists) {
            resolve(JSON.stringify({bucket: bucket, file: file}));
          } else {
            reject(`${file} doesn't exist on ${bucket}.`);
          }
        });
      }
      console.log(`... this test is set on local file.`);
    } else {
      console.log(
          `API[${attributes.api} is PS based, tests based on local file.`);
    }
    if (fs.existsSync(file)) {
      resolve(fs.readFileSync(file).toString());
    } else {
      reject(`${file} doesn't exist.`);
    }
  });
  return getMessageData.then((messageData) => {
    const context = {eventId: 'test-demo-message'};
    const message = {
      attributes,
      data: Buffer.from(messageData).toString('base64'),
    };
    const callback = () => console.log(`After send out requests from ${file}.`);
    return FirestoreAccessBase.isNativeMode()
        .then((nativeMode) => (nativeMode) ? DataSource.FIRESTORE
            : DataSource.DATASTORE)
        .then(
            (dataSource) => getTentacles('test', dataSource).getApiRequester()(
                message,
                context, callback));
  });
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
