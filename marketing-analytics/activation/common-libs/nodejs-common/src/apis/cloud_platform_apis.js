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
 * @fileoverview API handler for Google Cloud Platform APIs.
 */

'use strict';

const {google} = require('googleapis');
const AuthClient = require('./auth_client.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/cloud-platform',
]);
const API_VERSION = 'v1';

/**
 * Google Cloud Platform API stubs. This class offers convenient request
 * wrappers for some of Google Cloud APIs that are not supported by
 * Google cloud client libraries.
 */
class CloudPlatformApis {
  constructor() {
    /** @const {!AuthClient} */
    const authClient = new AuthClient(API_SCOPES);
    this.auth = authClient.getApplicationDefaultCredentials();
    this.projectId = process.env['GCP_PROJECT'];
  }

  /**
   * Gets the GCP project Id. In Cloud Functions, it *should* be passed in
   * through environment variable during the deployment. But if it doesn't exist
   * (for example, in local unit tests), this function will fallback to ADC
   * (Application Default Credential) auth's asynchronous function to get the
   * project Id.
   * @return {string}
   * @private
   */
  async getProjectId_() {
    if (!this.projectId) this.projectId = await this.auth.getProjectId();
    return this.projectId;
  }

  /**
   * Returns the available permissions for the given project and permissions
   * list. See:
   * https://cloud.google.com/resource-manager/reference/rest/v1/projects/testIamPermissions.
   * For more information of Permission, see:
   * https://cloud.google.com/iam/docs/overview#permissions.
   * @param {!Array<string>} permissions Permissions array.
   * @return {!Promise<!Array<string>>} The available permissions that current
   *     operator (defined by ADC authentication information) has for the given
   *     permission list.
   */
  async testIamPermissions(permissions) {
    const resourceManager = google.cloudresourcemanager({
      version: API_VERSION,
      auth: this.auth,
    });
    const projectId = await this.getProjectId_();
    const request = {
      resource_: projectId,
      resource: {permissions: permissions},
    };
    return resourceManager.projects.testIamPermissions(request)
        .then((response) => response.data.permissions);
  }
}

module.exports = {
  CloudPlatformApis,
  API_VERSION,
  API_SCOPES,
};
