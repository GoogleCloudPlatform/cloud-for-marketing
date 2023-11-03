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

const { RestfuleApiBase } = require('./restful_api_base');
const AuthClient = require('../auth_client.js');

/**
 * A RESTful API class with authorization client.
 */
class AuthRestfulApi extends RestfuleApiBase {

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
    const headers = await auth.getRequestHeaders();
    return Object.assign({}, super.getDefaultHeaders(), headers);
  }

}

module.exports = { AuthRestfulApi };
