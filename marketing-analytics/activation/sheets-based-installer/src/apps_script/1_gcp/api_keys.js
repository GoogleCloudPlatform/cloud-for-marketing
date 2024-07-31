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

/** @fileoverview API Keys API handler class.*/

class ApiKeys extends ApiBase {

  constructor(projectId) {
    super();
    this.name = 'API Keys API';
    this.api = 'apikeys.googleapis.com';
    this.apiUrl = `https://${this.api}`;
    this.version = 'v2';
    this.projectId = projectId;
    this.locations = 'global';
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /**
   * Lists the API keys owned by a project.
   * @see https://cloud.google.com/api-keys/docs/reference/rest/v2/projects.locations.keys/list
   * @return {{
   *   accessToken: string,
   *   expireTime: string
   * }}
   */
  list() {
    return this.get(`projects/${this.projectId}/locations/${this.locations}/keys`);
  }

  /**
   * Gets the result of an operation.
   * @see https://cloud.google.com/api-keys/docs/reference/rest/v2/operations/get
   * @param {string} operationName
   * @return {!Operation}
   */
  getOperation(operationName) {
    return super.get(operationName);
  }

  /**
   * Gets the metadata for an API key. The key string of the API key isn't
   * included in the response.
   * @see https://cloud.google.com/api-keys/docs/reference/rest/v2/projects.locations.keys/get
   * @param {string} keyId
   * @return {!Key}
   */
  getKey(keyId) {
    return this.get(`projects/${this.projectId}/locations/${this.locations}/keys/${keyId}`);
  }

  /**
   * Gets the key string for an API key.
   * @see https://cloud.google.com/api-keys/docs/reference/rest/v2/projects.locations.keys/getKeyString
   * @param {string} keyId
   * @return {{keyString: string}}
   */
  getKeyString(keyId) {
    return this.get(`projects/${this.projectId}/locations/${this.locations}/keys/${keyId}/keyString`);
  }

  /**
   * Creates a new API key.
   * @see https://cloud.google.com/api-keys/docs/reference/rest/v2/projects.locations.keys/create
   * @param {string} keyId
   * @param {object=} restrictions
   * @return {!Operation}
   */
  createKey(keyId, restrictions) {
    return this.mutate(
      `projects/${this.projectId}/locations/${this.locations}/keys?keyId=${keyId}`,
      {
        displayName: keyId.substring(0, 63),
        restrictions,
      });
  }

  /**
   * Creates an Api key and returns the result.
   * @param {string} keyId
   * @param {object=} restrictions
   * @return {object} The response of the create operation of an API key.
   */
  createKeyAndReturnResult(keyId, restrictions) {
    const { name } = this.createKey(keyId, restrictions);
    let rawResponse = this.getOperation(name);
    while (rawResponse.done !== true && !rawResponse.error) {
      Utilities.sleep(1000);
      rawResponse = this.getOperation(rawResponse.name);
      console.log('Wait and get operation', rawResponse.name);
    }
    return rawResponse;
  }

}
