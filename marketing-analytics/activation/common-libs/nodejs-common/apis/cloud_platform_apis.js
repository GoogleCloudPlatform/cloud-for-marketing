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

const RESOURCE_MANAGER_API_SCOPES =
    ['https://www.googleapis.com/auth/cloud-platform'];

/**
 * Google Cloud Platform API stubs. This class offers convenient request
 * wrappers for some of Google Cloud APIs that are not supported by
 * Google cloud client libraries.
 */
class CloudPlatformApis {
  constructor() {
    /** @const {!AuthClient} */
    this.authClient = new AuthClient(RESOURCE_MANAGER_API_SCOPES);
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
  testIamPermissions(permissions) {
    return this.authClient.getApplicationDefaultCredentials().then((auth) => {
      const resourceManager = google.cloudresourcemanager({
        version: 'v1',
        auth: auth,
      });
      const projectId = (auth.projectId) ? auth.projectId :
                                           process.env['GOOGLE_CLOUD_PROJECT'];
      const request = {
        resource_: projectId,
        resource: {permissions: permissions},
      };
      return resourceManager.projects.testIamPermissions(request).then(
          (response) => response.data.permissions);
    });
  }

  /**
   * Returns the available deploy locations for Cloud Functions.
   * For more information, see:
   * https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations/list.
   * @return {!Promise<!Array<string>>} The array of location ids.
   */
  listCloudFunctionsLocations() {
    return this.authClient.getApplicationDefaultCredentials().then((auth) => {
      const cloudfunctions = google.cloudfunctions({
        version: 'v1',
        auth: auth,
      });
      const projectId = (auth.projectId) ? auth.projectId :
                                           process.env['GOOGLE_CLOUD_PROJECT'];
      const request = {name: `projects/${projectId}`};
      return cloudfunctions.projects.locations.list(request).then(
          (response) => {
            return response.data.locations.map(
                (location) => location.locationId);
          });
    });
  }
}

exports.CloudPlatformApis = CloudPlatformApis;
