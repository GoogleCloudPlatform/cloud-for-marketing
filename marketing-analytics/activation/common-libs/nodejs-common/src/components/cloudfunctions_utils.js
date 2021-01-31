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
 * @fileoverview Definition of types used in Cloud Functions.
 */

'use strict';

const {join} = require('path');
const {StorageFile} = require('./storage.js');
/** Type definition of 'Cloud Storage Object' for Cloud Functions. */
const {storage_v1: {Schema$Object: StorageEventData}} = require('googleapis');
/**
 * Imports the 'Message' type definition from Pubsub.
 * See: https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
 */
const {Message: PubsubMessage} = require('@google-cloud/pubsub');

/**
 * Validated Storage file information. Generated from the function
 * 'validatedStorageTrigger' after validation.
 *
 * @typedef {{
 *   name:string,
 *   oldName:string,
 *   bucket:string,
 *   size:number,
 *   updated:!Date,
 * }}
 */
let ValidatedStorageFile;

/**
 * Function which does the real job for a Storage event. It is invoked after the
 * validation of new event (file) for:
 * 1. It is a targeted file (in a predefined folder).
 * 2. It is not a duplicated event. For trigger duplication, see:
 * https://cloud.google.com/functions/docs/concepts/events-triggers#triggers
 *
 * @typedef {function(!ValidatedStorageFile):!Promise<*>}
 */
let MainFunctionOfStorage;

/**
 * The context object for the event. Copy the definition from:
 * https://cloud.google.com/functions/docs/writing/background#functions_background_parameters-node8-10
 * @typedef {{
 *  eventId:string,
 *  timestamp:string,
 *  eventType:string,
 *  resource:string,
 * }}
 */
let EventContext;

/**
 * The first parameter passed to Cloud Functions as the 'Event' parameter in
 * Node6 environment.
 * @typedef {{
 *   data:(!StorageEventData|!PubsubMessage),
 *   context:!EventContext,
 * }}
 */
let CloudFunctionEvent;

/**
 * Storage event driven Cloud Functions in Node6 environment. It takes one
 * parameters and returns a Promise, see:
 * https://cloud.google.com/functions/docs/writing/background#functions_background_parameters-node6
 * @typedef {function(!CloudFunctionEvent):!Promise<*>}
 * @deprecated due to node6 env has benn sun set.
 */
let CloudFunctionNode6;

/**
 * Storage event driven Cloud Functions in Node8/10 environment. It takes two
 * parameters, see:
 * https://cloud.google.com/functions/docs/writing/background#functions_background_parameters-node8-10
 * @typedef{function((!StorageEventData|!PubsubMessage),!EventContext):
 * !Promise<*>}
 */
let CloudFunctionNode8;

/**
 * Cloud Functions triggered by Cloud Storage events.
 *
 * @typedef {(!CloudFunctionNode6|!CloudFunctionNode8)}
 */
let CloudFunction;

/**
 * Gets a Node6 compatible Cloud Functions based on a Node8/10 Cloud Functions.
 * Cloud Functions have different parameter types and numbers in Node6 and
 * Node8/10:
 * 1. Node6 (deprecated) has (event, callback);
 * 2. Node8/10 has (data, context, callback).
 * see
 * https://cloud.google.com/functions/docs/writing/background#functions-writing-background-hello-pubsub-node8-10
 *
 * @param {!CloudFunctionNode8} fn Cloud Functions in Node8 environment.
 * @return {!CloudFunction} The Cloud Functions can return any type.
 */
const adaptNode6 = (fn) => {
  /**
   * Returns the Cloud Function for both Node6 and Node8/10:
   * @param {!CloudFunctionEvent|!PubsubMessage|!StorageEventData} eventOrData
   *     The event payload for nodejs8/10 or The Cloud Functions event for
   *     nodejs6.
   * @param {?EventContext} possibleContext The event metadata for nodejs8/10
   *     or undefined for nodejs6.
   * @return {!Promise<*>}
   */
  return (eventOrData, possibleContext) => {
    const /** @type {!StorageEventData|!PubsubMessage} */ data =
        (!possibleContext)
            ? (/** @type {CloudFunctionEvent} */ eventOrData).data
            : eventOrData;
    const /** @type {!EventContext} */ context =
        (!possibleContext)
            ? (/** @type {CloudFunctionEvent} */ eventOrData).context
            : possibleContext;
    return fn(data, context);
  };
};

/**
 * Triggers the main function with the correct new coming file for once.
 * Detailed steps:
 * 1. Checks the coming file is in the proper folder. In case there are
 * different Cloud Functions with different purposes monitoring on the same
 * Storage Bucket, Cloud Functions can be distinguished with its own 'folder';
 * 2. Tries to move the original file to another specific fold before run the
 * main function. Sometimes there are duplicated GCS trigger events for Cloud
 * Functions, in order to solve that, the function will move the file to the
 * target folder before invoke the function.
 * 3. After the file is successfully moved, invoke the main function with the
 * moved file.
 *
 * @param {!MainFunctionOfStorage} fn The main function to run on the file.
 * @param {string} folder The folder that the main function should check.
 * @param {string=} processed The folder that the file will be moved to.
 * @return {!CloudFunction} The Cloud Functions that will be exported.
 */
const validatedStorageTrigger = (fn, folder, processed = 'processed/') => {
  /**
   * Returns the Cloud Function that can handle duplicated Storage triggers.
   * @type {!CloudFunctionNode8}
   */
  const handleFile = (file, context) => {
    const eventId = context.eventId;
    const fileName = file.name;
    // The second condition prevents the event of folder creation.
    if (!fileName.startsWith(folder) || folder.startsWith(fileName)) {
      const message = `Skip event[${eventId}]: ${fileName} not in the folder: ${
          folder}.`;
      console.log(message);
      return Promise.resolve(message);
    } else {
      const fileObj = new StorageFile(file.bucket, fileName);
      return fileObj.getFile()
          .move(join(processed, fileName))
          .then(([newFile]) => {
            console.log(
                `Event[${eventId}] move: '${fileName}' to '${newFile.name}'`);
            return fn({
              name: newFile.name,
              oldName: fileName,
              bucket: file.bucket,
              size: file.size,
              updated: file.updated,
            }).then(() => {
              const message = `Event[${eventId}] completed: ${
                  newFile.name} triggered the Cloud Functions.`;
              console.log(message);
              return message;
            });
          })
          .catch((error) => {
            let message;
            if (error.message.startsWith('file#delete failed with an error')) {
              message = `Quit event[${eventId}]: Fail to move ${
                  fileName}. Maybe duplicated.`;
            } else {
              message = `Event[${eventId}] triggered: ${
                  fileName} Cloud Functions got an error: ${error.message}`;
            }
            console.warn(message);
            return message;
          });
    }
  };
  return adaptNode6(handleFile);
};

/**
 * Cloud Functions has a specific folder to host deployed source code. The
 * path of the folder is stored in the environment variable named
 * 'CODE_LOCATION'.
 * Some Cloud Functions need a given service account key file or an OAuth token
 * key file to do authentication for external systems. These key files are
 * generated during the installation process and the values will be set as
 * Cloud Functions' ENV when the Cloud Functions get deployed. The values are
 * relative paths because the ENV 'CODE_LOCATION' is unavailable at deployment
 * time.
 * However, those relative paths don't work with `auth_client.js` in this
 * library because code in this library can't figure out the absolute path for
 * key files.
 * So use this function to convert an ENV with value of a relative path to an
 * absolute path in a Cloud Functions environment.
 *
 * @param {string} envVar Environment variable name.
 * @param {string=} codeLocation Absolute path of where the code is deployed in
 *     a Cloud Functions. Before Nodejs8(included) there is an environment
 *     variable `CODE_LOCATION` with the value.
 * @param {!Object<string,string>} env The environment object to hold env
 *     variables.
 */
const convertEnvPathToAbsolute = (envVar, codeLocation, env = process.env) => {
  const defaultSetting = env[envVar];
  const absoluteRootPath = codeLocation || env['CODE_LOCATION'];
  if (typeof defaultSetting !== 'undefined' && absoluteRootPath
      && !defaultSetting.startsWith('/')
      && !defaultSetting.startsWith(absoluteRootPath)) {
    console.log(`CODE_LOCATION: ${absoluteRootPath}`);
    env[envVar] = join(absoluteRootPath, defaultSetting);
  }
  console.log(`${envVar}: ${env[envVar]}`);
};

module.exports = {
  StorageEventData,
  PubsubMessage,
  ValidatedStorageFile,
  MainFunctionOfStorage,
  EventContext,
  CloudFunctionEvent,
  CloudFunctionNode8,
  CloudFunction,
  adaptNode6,
  validatedStorageTrigger,
  convertEnvPathToAbsolute,
};
