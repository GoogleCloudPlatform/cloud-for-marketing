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

/** @fileoverview Service Account Credentials API handler class.*/

class IamCredentials extends ApiBase {

  constructor() {
    super();
    this.name = 'Service Account Credentials API';
    this.api = 'iamcredentials.googleapis.com';
    this.apiUrl = `https://${this.api}`;
    this.version = 'v1';
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/projects/-`;
  }

  /**
   * Generates an OAuth 2.0 access token for a service account.
   * @see https://cloud.google.com/iam/docs/reference/credentials/rest/v1/projects.serviceAccounts/generateAccessToken
   * @return {{
   *   accessToken: string,
   *   expireTime: string
   * }}
   */
  generateAccessToken(serviceAccount, scope) {
    return this.mutate(
      `serviceAccounts/${serviceAccount}:generateAccessToken`, { scope });
  }

}
