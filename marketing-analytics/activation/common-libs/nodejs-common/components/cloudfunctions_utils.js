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

const path = require('path');
const StorageUtils = require('./storage_utils.js');
const {storage_v1} = require('googleapis');
/** Type definition of 'Cloud Storage Object' for Cloud Functions. */
const {Schema$Object: StorageEventData} = storage_v1;
/**
 * Imports the 'Message' type definition from Pubsub.
 * See: https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
 */
const {Message: PubsubMessage} = require('@google-cloud/pubsub');

exports.StorageEventData = StorageEventData;
exports.PubsubMessage = PubsubMessage;

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

exports.ValidatedStorageFile = ValidatedStorageFile;

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

exports.MainFunctionOfStorage = MainFunctionOfStorage;

/**
 * The context object for the event. Copy the definition from:
 * https://cloud.google.com/functions/docs/writing/background#functions_background_parameters-node8-10
 * @typedef{{
 *  eventId:string,
 *  timestamp:string,
 *  eventType:string,
 *  resource:string,
 * }}
 */
let EventContext;

exports.EventContext = EventContext;

/**
 * The first parameter passed to Cloud Functions as the 'Event' parameter in
 * Node6 environment.
 * @typedef{{
 *   data:(!StorageEventData|!PubsubMessage),
 *   context:!EventContext,
 * }}
 */
let CloudFunctionEvent;

exports.CloudFunctionEvent = CloudFunctionEvent;

/**
 * Storage event driven Cloud Functions in Node6 environment. It takes one
 * parameters and returns a Promise, see:
 * https://cloud.google.com/functions/docs/writing/background#functions_background_parameters-node6
 * @typedef {function(!CloudFunctionEvent):!Promise<*>}
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
exports.CloudFunctionNode8 = CloudFunctionNode8;

/**
 * Cloud Functions triggered by Cloud Storage events.
 *
 * @typedef {(!CloudFunctionNode6|!CloudFunctionNode8)}
 */
let CloudFunction;

exports.CloudFunction = CloudFunction;

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
  const getCloudFunction = (eventOrData, possibleContext) => {
    const /** @type {!StorageEventData|!PubsubMessage} */ data =
        (!possibleContext) ? eventOrData.data : eventOrData;
    const /** @type {!EventContext} */ context =
        (!possibleContext) ? eventOrData.context : possibleContext;
    return fn(data, context);
  };
  return getCloudFunction;
};

exports.adaptNode6 = adaptNode6;

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
exports.validatedStorageTrigger = (fn, folder, processed = 'processed/') => {
  /**
   * Returns the Cloud Function that can handle duplicated Storage triggers.
   * @type {!CloudFunctionNode8}
   */
  const handleFile = (file, context) => {
    const fileName = file.name;
    // The second condition prevents the event of folder creation.
    if (!fileName.startsWith(folder) || folder.startsWith(fileName)) {
      const message = `Skip: ${fileName} not in the folder: ${folder}.`;
      console.log(message);
      return Promise.resolve(message);
    } else {
      const fileObj = new StorageUtils(file.bucket, fileName);
      return fileObj.getFile()
          .move(path.join(processed, fileName))
          .then(([newFile]) => {
            console.log(`Move '${fileName}' to '${newFile.name}'`);
            return Promise
                .resolve(fn({
                  name: newFile.name,
                  oldName: fileName,
                  bucket: file.bucket,
                  size: file.size,
                  updated: file.updated,
                }))
                .then(() => {
                  const message =
                      `Done: ${newFile.name} triggered the Cloud Functions.`;
                  console.log(message);
                  return Promise.resolve(message);
                });
          })
          .catch((error) => {
            console.warn(`Error in move '${file.name}': `, error.message);
            const message = `Quit: Fail to move ${fileName}. Maybe duplicated.`;
            console.warn(message);
            return Promise.resolve(message);
          });
    }
  };
  return adaptNode6(handleFile);
};
