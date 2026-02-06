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
 * @fileoverview A base class for RESTful API with authorization client.
 */

const { RestfulApiBase } = require('./restful_api_base');
const AuthClient = require('../auth_client.js');

/**
 * A RESTful API class with authorization client.
 */
class AuthRestfulApi extends RestfulApiBase {

  constructor(env = process.env, options = {}) {
    super(env);
    /**
     * `authClient` can be consumed by cloud client library as the auth
     * client. By passing this in, we can offer more flexible auth clients in
     * test cases for API client library and cloud client library in future.
     */
    if (options.authClient) {
      this.auth = options.authClient;
    }
  }

  /**
   * Returns the Api scope for authorization.
   * @return {!Array<string>}
   * @abstract
   */
  getScope() { }

  /**
   * Gets the auth object.
   * @return {!Promise<{!OAuth2Client|!JWT|!Compute}>}
   */
  async getAuth() {
    if (this.auth) return this.auth;
    this.authClient = new AuthClient(this.getScope(), this.env);
    await this.authClient.prepareCredentials();
    this.auth = this.authClient.getDefaultAuth();
    return this.auth;
  }

  /**
   * Returns HTTP headers. By default, it contains access token.
   * @return {object} HTTP headers.
   * @override
   */
  async getDefaultHeaders() {
    const auth = await this.getAuth();
    const authHeaders = await auth.getRequestHeaders();
    const mergedHeaders = new Headers(await super.getDefaultHeaders());
    authHeaders.forEach((value, key) => void mergedHeaders.set(key, value));
    return mergedHeaders;
  }

}

module.exports = { AuthRestfulApi };
