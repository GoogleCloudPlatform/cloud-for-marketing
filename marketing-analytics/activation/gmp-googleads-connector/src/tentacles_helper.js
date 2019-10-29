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
 * @fileoverview Helper functions for Tentacles.
 */

'use strict';

const path = require('path');
const {
  PubSubUtils,
  StorageUtils,
  FirestoreAccessBase: {DataSource, isNativeMode},
} = require('nodejs-common');
const {ApiConfigJson, guessApiConfig} = require('./api_config.js');
const {ApiConfigItem, getApiNameList} = require('./api_handlers/index.js');
const {getTopicNameByApi, getAttributes, Tentacles} = require('./tentacles.js');

/**
 * Fixes the service key file location's value in environment variable.
 *
 * If a key file of service account is wrapped in the source code, it will
 * be difficult for the 'nodejs-common' library to figure out the real path
 * of the key file in the circumstance of relative path, because the Cloud
 * Functions has its own strategy to deploy the codes.
 *
 * Ideally, we should use the default service account of the Cloud Functions.
 * However, due to some reasons, the API scopes of the default service account
 * can't be extended. So a user managed service account is used instead.
 */
exports.fixKeyFilePath = () => {
  const apiServiceAccount = process.env['API_SERVICE_ACCOUNT'];
  const codeLocation = process.env['CODE_LOCATION'];
  if (apiServiceAccount) {
    if (!apiServiceAccount.startsWith(codeLocation)) {
      console.log(`CODE_LOCATION: ${codeLocation}`);
      process.env['API_SERVICE_ACCOUNT'] =
          path.join(codeLocation, apiServiceAccount);
    }
    console.log(`API_SERVICE_ACCOUNT: ${process.env['API_SERVICE_ACCOUNT']}`);
  }
};

/**
 * Uploads ApiConfig from a JSON object to Firestore or Datastore. It will
 * check the Firestore mode automatically.
 * @see './api_config.js.template'
 * @param {!ApiConfigJson} updatedConfigs The API configurations JSON object.
 * @return {!Promise<!Array<!Object<string,boolean>>>} Whether API configuration
 *     updated.
 */
exports.uploadApiConfig = (updatedConfigs) => {
  return guessApiConfig().then((apiConfig) => {
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
            return updateSingleConfig(configs, configName, apiName);
          }))
          .then((results) => {
            return {[apiName]: !results.includes(false)};
          });
    };

    /**
     * Updates single Api config item.
     * @param {!Object<string, !ApiConfigItem>} configs Api config items for a
     *     specific Api.
     * @param {string} configName Config item name.
     * @param {string} apiName Api name.
     * @return {!Promise<boolean>} Whether this API config item updated.
     */
    const updateSingleConfig = (configs, configName, apiName) => {
      const configObject = configs[configName];
      return apiConfig.saveConfig(apiName, configName, configObject)
          .then(() => {
            console.log(
                `Import Config for API[${apiName}]_config[${configName}]`);
            return true;
          });
    };

    return Promise.all(
        Object.keys(updatedConfigs).map(updateConfigsForSingleApi));
  });
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
exports.initPubsub = (topicPrefix, apiList) => {
  const apis = apiList && apiList.length > 0 ? apiList : getApiNameList();
  console.log(`Start to create Topics for API: ${apis}`);
  return Promise.all(apis.map((apiName) => {
    const topicName = getTopicNameByApi(topicPrefix, apiName);
    const subscriptionName = `${topicName}-holder`;
    return PubSubUtils.getOrCreateTopic(topicName)
        .then((topic) => {
          return PubSubUtils
              .getOrCreateSubscription(topicName, subscriptionName, {
                ackDeadlineSeconds: 300,
                expirationPolicy: {},
                flowControl: {maxMessages: 1}
              })
              .then((subscription) => {
                console.log(`${topic.name} <--- ${subscription.name}`);
                return true;
              });
        })
        .catch((error) => {
          console.error(
              `Creating Topic ${topicName} and Subscription ${
                  subscriptionName}:`,
              error);
          return false;
        })
        .then((result) => {
          return {[topicName]: result};
        });
  }));
};

/**
 * Invokes 'ApiRequester' of Tentacles to send a local file or a Google Cloud
 * Storage (GCS) file to external API endpoints. The filename convention will
 * be followed.
 *
 * @param {string} file File name.
 * @param {string|undefined=} bucket Bucket name for a GCS file.
 * @return {!Promise<undefined>}
 */
exports.localApiRequester = (file, bucket = undefined) => {
  const fs = require('fs');
  const attributes = getAttributes(file);
  console.log(`Test file [${file}] has attributes:`, attributes);
  const getMessageData = new Promise((resolve, reject) => {
    if (attributes.gcs === 'true') {
      console.log(`API[${attributes.api}] is GCS based...`);
      if (bucket) {
        console.log(`... and this test is set on GCS file`);
        const storageUtils = new StorageUtils(bucket, file);
        return storageUtils.getFile().exists().then((fileExists) => {
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
      attributes: attributes,
      data: Buffer.from(messageData).toString('base64'),
    };
    const callback = () => {
      console.log(`After send out requests based on ${file}.`);
    };
    return isNativeMode()
        .then((nativeMode) => {
          return (nativeMode) ? DataSource.FIRESTORE : DataSource.DATASTORE;
        })
        .then((dataSource) => {
          const tentacles = new Tentacles('test', dataSource);
          return tentacles.getApiRequester()(message, context, callback);
        });
  });
};
