// Copyright 2023 Google Inc.
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
 * @fileoverview The base class of Google Cloud Apis.
 */
const AuthClient = require('../auth_client.js');
const { AuthRestfulApi } = require('../base/auth_restful_api.js');

/**
 * A RESTful API class for Google Cloud Platform API with ADC as the
 * authorization method.
 */
class GoogleCloudApi extends AuthRestfulApi {

  constructor(env = process.env, options = {}) {
    super(env, options);
    this.projectId = options.projectId || env['GCP_PROJECT'];
  }

  /** @override */
  getScope() {
    return ['https://www.googleapis.com/auth/cloud-platform'];
  }

  /**
   * @override
   * Returns the `GoogleAuth` object for a Google Cloud component object.
   * If the component is based on Google Cloud Client Library, it will use the
   * `ADC` auth object by itself. However, some components are based on Google
   * API Client Library, which is based on API Scope and an explicit auth
   * object. In this situation, this function can help to generate such an auth
   * object.
   * By default, `AuthClient` (getDefaultAuth()) will return an auth client
   * based on the settings in ENV while the OAuth is the most preferred.
   * This works for most of the external API clients (in the '../apis'
   * folder), however this won't work in the Cloud Functions, as those OAuth
   * token usually won't have enough permission to invoke Google Cloud API.
   * Using the method `getApplicationDefaultCredentials` to force
   * `AuthClient` return an ADC auth client, which will work in the Cloud.
   */
  async getAuth() {
    if (this.auth) return this.auth;
    this.authClient = new AuthClient(this.getScope(), this.env);
    // Not required for ADC: await this.authClient.prepareCredentials();
    this.auth = this.authClient.getApplicationDefaultCredentials();
    return this.auth;
  }

  /**
   * Gets the GCP project Id. In Cloud Functions, it *should* be passed in
   * through environment variable during the deployment. But if it doesn't exist
   * (for example, in local unit tests), this function will fallback to ADC
   * (Application Default Credential) auth's asynchronous function to get the
   * project Id.
   * @return {string}
   */
  async getProjectId() {
    if (!this.projectId) {
      const auth = await this.getAuth();
      this.projectId = await auth.getProjectId();
    }
    return this.projectId;
  }

}

module.exports = { GoogleCloudApi };
