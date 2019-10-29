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
 * @fileoverview Google Cloud AutoML helper.
 */

'use strict';
const automl = require('@google-cloud/automl');
const request = require('request');
const {api: {AuthClient}} = require('nodejs-common');

const version = 'v1beta1';

/**
 * AutoML Tables API doesn't have fully library support yet. This class tries to
 * act a single helper class for different API requests of AutoML Tables API,
 * e.g. 'batch predict' based on Google Cloud Client Library,
 * https://googleapis.dev/nodejs/automl/latest/index.html
 */
class AutoMlService {

  /**
   * Initialize an instance.
   * @param {{keyFileName:(string|undefined)}}options
   */
  constructor(options = {}) {
    const defaultOptions = {};
    if (process.env['AUTOML_CREDENTIALS']) {
      defaultOptions.keyFilename = process.env['AUTOML_CREDENTIALS'];
    }
    this.options = Object.assign(defaultOptions, options);
    console.log(this.options);
  }

  /**
   * Gets authentication client of AutoML API.
   * @returns {!Promise<!OAuth2Client|!JWT|!Compute>}
   * @private
   */
  getAuthClient_() {
    const authClient = new AuthClient(
        'https://www.googleapis.com/auth/cloud-platform');
    // TODO: Currently, it only supports Service Account (JWT/COMPUTER).
    // return authClient.getDefaultAuth();
    return authClient.getServiceAccount(this.options.keyFilename);
  }

  batchPredict(projectId, computeRegion, modelId, inputConfig, outputConfig) {
    const client = new automl[version].PredictionServiceClient(this.options);
    const modelFullId = client.modelPath(projectId, computeRegion, modelId);
    return client.batchPredict({
      name: modelFullId,
      inputConfig: inputConfig,
      outputConfig: outputConfig,
    }).then((responses) => {
      const operation = responses[1];
      console.log(`Operation name: ${operation.name}`);
      return operation.name;
    }).catch((error) => {
      console.error(error);
    });
  }

  getOperation(name) {
    return this.getAuthClient_().then((auth) => {
      const url = `https://automl.googleapis.com/${version}/${name}`;
      return auth.getAccessToken().then((accessToken) => {
        const requestOptions = {
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${accessToken}`,
          },
          url: url,
        };
        return new Promise((resolve, reject) => {
          request(requestOptions, (error, response, body) => {
            if (error) reject(error);
            resolve(JSON.parse(body));
          });
        });
      });
    });
  }
}

exports.AutoMlService = AutoMlService;
