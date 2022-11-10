// Copyright 2022 Google Inc.
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
 * @fileoverview Secret Manager wrapper class.
 */

const { GoogleAuthOptions } = require('google-auth-library');
const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');

/**
 * Google Cloud Secret Manager class based on cloud client library.
 * @see https://cloud.google.com/nodejs/docs/reference/secret-manager/latest
 * @see https://cloud.google.com/secret-manager/docs/reference/libraries
 */
class SecretManager {
  /**
   * @constructor
   * @param {GoogleAuthOptions=} options
   */
  constructor(options = {}) {
    if (!options.projectId) {
      options.projectId = process.env['GCP_PROJECT'];
    }
    this.client = new SecretManagerServiceClient(options);
  }

  async access(secret, version = 'latest') {
    const projectId = await this.client.getProjectId();
    const name = `projects/${projectId}/secrets/${secret}/versions/${version}`;
    try {
      const [secretObj] = await this.client.accessSecretVersion({ name });
      return secretObj.payload.data.toString('utf8');
    } catch (error) {
      if (error.details.indexOf('not found') > -1) {
        return;
      }
      throw error;
    }
  }
}

exports.SecretManager = SecretManager;
