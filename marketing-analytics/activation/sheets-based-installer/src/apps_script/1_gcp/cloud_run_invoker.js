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

/** @fileoverview Invoke HTTP Cloud Functions based on ID token. */

/**
 * This class is used to invoke a Cloud Function.
 */
class CloudRunInvoker {
  /**
   * @param {!CloudRunService|!CloudRunFunction} targetClient Instantiated GCP
   *     client
   * @param {string} targetName Target Service or Function name
   */
  constructor(targetClient, targetName, options = {}) {
    this.targetClient = targetClient;
    this.targetName = targetName;
    this.url = options.url;
    this.serviceAccount = options.serviceAccount;
    this.useServiceAccount = options.useServiceAccount || true;
  }

  /**
   * For HTTP based Cloud Run Service or Function, an ID token is expected but
   * not the access token. This brings challenges:
   *   a. Only CRS or CRF deployed in the same GCP (the one back Apps Script)
   *      can be invoked.
   *   b. A new copied Cyborg can not invoke deployed CRS or CRF because that
   *      CRS or CRF doesn't have this new Cyborg's OAuth client with will be
   *      automatically created when the GCP is set to this Apps Script.
   * To solve these issues, the program will catch the error and suggest to
   * redeploy the resource.
   * @param {!Object=} payload
   * @return {*}
   */
  invoke(payload = {}) {
    let token;

    const url = this.url || this.targetClient.getServiceUrl(this.targetName);
    if (this.useServiceAccount) {
      const serviceAccount =
        this.serviceAccount ||
        this.targetClient.getServiceAccount(this.targetName);

      const iamCredentials = new IamCredentials();
      const tokenResponse = iamCredentials.generateIdToken(serviceAccount, url);
      if (!tokenResponse.error) {
        token = tokenResponse.token;
        console.log('Invoke Cloud Run with service account id token');
      } else {
        console.log(
          'Cannot get ID token for service account. Will fallback to user ID token:',
          tokenResponse.error.message,
        );
      }
    }
    if (!token) token = this.getDefaultIdToken();
    console.info(`[CloudRunInvoker] Invoking ${this.targetName} -> ${url}`);
    const response = UrlFetchApp.fetch(url, {
      method: 'POST',
      contentType: 'application/json',
      muteHttpExceptions: true,
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/json',
      },
      payload: JSON.stringify(payload),
    });

    const statusCode = response.getResponseCode();

    if (statusCode === 401) {
      throw new Error(
        `Cloud Run target [${
          this.targetName
        }] rejected the credentials (401 Unauthorized).\n` +
          `This typically happens when the target was deployed before the Apps Script OAuth Client was created.\n` +
          `Action: Please redeploy the Cloud Run Service/Function to sync the OAuth credentials.`,
      );
    }
    if (statusCode >= 400) {
      throw new Error(
        `Cloud Run invocation failed [${statusCode}]: ${response.getContentText()}`,
      );
    }
    try {
      return JSON.parse(response.getContentText() || '{}');
    } catch (e) {
      return response.getContentText();
    }
  }

  /**
   * @return {string}
   */
  getDefaultIdToken() {
    let token;
    if (typeof EXPLICIT_AUTH !== 'undefined' && EXPLICIT_AUTH.enabled()) {
      token = EXPLICIT_AUTH.getIdentityToken();
      console.log('Invoke Cloud Run with explicit auth id token');
    } else {
      token = ScriptApp.getIdentityToken();
      console.log('Invoke Cloud Run with user id token');
    }
    return token;
  }
}
