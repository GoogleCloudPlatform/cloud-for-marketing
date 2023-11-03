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

/** @fileoverview Cloud Secret Manager API handler class.*/

class SecretManager extends ApiBase {

  constructor(projectId) {
    super();
    this.apiUrl = 'https://secretmanager.googleapis.com';
    this.version = 'v1';
    this.projectId = projectId;
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/projects/${this.projectId}/secrets`;
  }

  /**
   * Gets secrets.
   * @see https://cloud.google.com/secret-manager/docs/reference/rest/v1/projects.secrets/list
   * @return {SecretManagerService.ListSecrets}:
   */
  listSecrets() {
    return super.get('');
  }

  /**
   * Creates a secret. Secret need to be created before a version can be added.
   * @see https://cloud.google.com/secret-manager/docs/reference/rest/v1/projects.secrets/create
   * @param {string} secretId
   * @return
   */
  createSecret(secretId) {
    return this.mutate(`?secretId=${secretId}`,
      { replication: { automatic: {} } }
    );
  }

  /**
   * Gets a version of the secret. By default, it gets the latest vesion.
   * @see https://cloud.google.com/secret-manager/docs/reference/rest/v1/projects.secrets.versions/get
   * @param {string} secret
   * @param {string=} version
   * @return {!SecretVersion}
   * @see https://cloud.google.com/secret-manager/docs/reference/rest/v1/projects.secrets.versions#SecretVersion
   */
  getSecretVersion(secret, version = 'latest') {
    return this.get(`${secret}/versions/${version}`);
  }

  /**
    * Creates a new SecretVersion containing secret data and attaches it to an
    * existing Secret.
    * @see https://cloud.google.com/secret-manager/docs/reference/rest/v1/projects.secrets/addVersion
    * @param {string} secretId
    * @param {string} data
    * @return {!SecretVersion}
    */
  addSecretVersion(secretId, data) {
    const { error } = this.getSecretVersion(secretId);
    if (error) {
      if (error.status !== 'NOT_FOUND') {
        throw new Error(`Fail to get Secret ${secretId}`);
      }
      this.createSecret(secretId);
    }
    const payload = { payload: { data: Utilities.base64Encode(data) } };
    return this.mutate(`${secretId}:addVersion`, payload);
  }

  /**
   * Returns the secret data as a string of a SecretVersion.
   * @see https://cloud.google.com/secret-manager/docs/reference/rest/v1/projects.secrets.versions/access
   * @param {string} secretId
   * @param {string=} version
   * @return {{data: (string|undefined), error: (Error|undefined)}}
   */
  accessSecret(secretId, version = 'latest') {
    const { error, payload } = this.get(`${secretId}/versions/${version}:access`);
    if (error) return { error };
    const data = Utilities.newBlob(Utilities.base64Decode(payload.data))
      .getDataAsString();
    return { data };
  }
}
