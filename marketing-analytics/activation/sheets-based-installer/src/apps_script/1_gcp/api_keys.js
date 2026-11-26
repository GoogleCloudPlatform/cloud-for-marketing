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

class ApiKeys extends GcpApiBase {
  /**
   * @param {string} projectId
   */
  constructor(projectId) {
    super();
    this.name = 'API Keys API';
    this.api = 'apikeys.googleapis.com';
    this.permissions = ['apikeys.keys.create', 'apikeys.keys.getKeyString'];
    this.adminRole = 'API Keys Admin';
    this.runtimeRoles = [];
    this.apiUrl = `https://${this.api}`;
    this.version = 'v2';
    this.projectId = projectId;
    this.locations = 'global';
  }

  /**
   * @override
   * @return {string}
   */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /**
   * Returns the list of API keys with the give display name.
   * @see https://cloud.google.com/api-keys/docs/reference/rest/v2/projects.locations.keys/list
   * @param {string} name
   * @return {!Array<!Key>}
   */
  loadKeyByName(name) {
    const uri = `projects/${this.projectId}/locations/${this.locations}/keys`;
    const keys = this.list('keys', uri);
    return keys.filter(({displayName}) => displayName === name);
  }

  /**
   * Gets the result of an operation.
   * @see https://cloud.google.com/api-keys/docs/reference/rest/v2/operations/get
   * @param {string} operationName
   * @return {!Operation}
   */
  getOperation(operationName) {
    return this.get(operationName);
  }

  /**
   * Gets the metadata for an API key. The key string of the API key isn't
   * included in the response.
   * @see https://cloud.google.com/api-keys/docs/reference/rest/v2/projects.locations.keys/get
   * @param {string} keyId
   * @return {!Key}
   */
  getKey(keyId) {
    return this.get(
      `projects/${this.projectId}/locations/${this.locations}/keys/${keyId}`,
    );
  }

  /**
   * Gets the key string for an API key.
   * @see https://cloud.google.com/api-keys/docs/reference/rest/v2/projects.locations.keys/getKeyString
   * @param {string} keyId
   * @return {{keyString: string}}
   */
  getKeyString(keyId) {
    const {error, keyString} = this.get(
      `projects/${this.projectId}/locations/${this.locations}/keys/${
        keyId
      }/keyString`,
    );
    if (error) throw error;
    return keyString;
  }

  /**
   * Creates a new API key.
   * @see https://cloud.google.com/api-keys/docs/reference/rest/v2/projects.locations.keys/create
   * @param {string} keyName
   * @param {!Object=} restrictions
   * @return {!Operation}
   */
  createKey(keyName, restrictions) {
    const keyId = keyName.toLowerCase().replace(/[^(\w)]/g, '-');
    return this.mutate(
      `projects/${this.projectId}/locations/${this.locations}/keys?keyId=${
        keyId
      }`,
      {
        displayName: keyName,
        restrictions,
      },
    );
  }

  /**
   * Creates an Api key and returns the result.
   * @param {string} keyName
   * @param {!Object=} restrictions
   * @return {!Object} The response of the create operation of an API key.
   */
  createKeyAndReturnResult(keyName, restrictions) {
    const operation = this.createKey(keyName, restrictions);
    return this.waitAndResolveOperation(operation, 'API key creation', 1000);
  }
}
