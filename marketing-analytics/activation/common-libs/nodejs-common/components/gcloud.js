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
 * @fileoverview Provides an adapter function to get Google Cloud product
 * object from Google Cloud Library.
 */

'use strict';

const {Storage} = require('@google-cloud/storage');
const {PubSub} = require('@google-cloud/pubsub');
const {Datastore} = require('@google-cloud/datastore');
const Firestore = require('@google-cloud/firestore');
const {BigQuery} = require('@google-cloud/bigquery');

/** All available Cloud Products that this file can provide. */
const ENABLED_CLOUD_PRODUCTS = new Map([
  ['storage', Storage],
  ['pubsub', PubSub],
  ['datastore', Datastore],
  ['firestore', Firestore],
  ['bigquery', BigQuery],
]);

/**
 * Returns Google Cloud Library client objects by the product name. Cloud Client
 * Library offers different ways to get the constractor of a Cloud product. For
 * example:
 *   1. Datastore v3.0, exports an object named 'Datastore' which is a
 * constructor function and 'v1' (another object);
 *   2. Datastore v2.0 (and other Cloud products), just exports the constructor
 * function of corresponding products.
 *
 * If a project ID or an service account key
 * file is offered, the cloud product will use them to initiate.
 *
 * @param {string} product The name of product on Google Cloud Platform.
 * @param {?{
 *   projectId:(string|undefined),
 *   keyFilename:(string|undefined),
 * }=} options Options to initiate cloud product.
 * @return {(!Storage|!PubSub|!Datastore|!Firestore|!BigQuery)} Google Cloud
 *     product API client.
 */
module.exports = (product, options = {}) => {
  if (!ENABLED_CLOUD_PRODUCTS.has(product.toLowerCase())) {
    throw new Error(`Fail to find the Cloud product: ${product}`);
  }
  return new (ENABLED_CLOUD_PRODUCTS.get(product.toLowerCase()))(options);
};
