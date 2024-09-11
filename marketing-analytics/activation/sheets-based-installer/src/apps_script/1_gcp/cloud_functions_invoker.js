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

/** @fileoverview Invoke HTTP Cloud Funtcions based on ID token. */

class CloudFunctionsInvoker extends ApiBase {

  constructor(projectId, locationId) {
    super();
    this.projectId = projectId;
    this.locationId = locationId;
  }

  /** @override */
  getAccessToken() {
    return ScriptApp.getIdentityToken();;
  }

  /** @override */
  getBaseUrl() {
    return `https://${this.locationId}-${this.projectId}.cloudfunctions.net`;
  }

  /**
   * There are two ways to invoke a Cloud Function.
   * 1. `directPost`, for HTTP based Cloud Functions:
   *   (+) no limited rate.
   *   (-) only for HTTP based CF.
   *   (-) it needs an Id token to authorize the request which brings issues:
   *       a. A new copied Cyborg can not invoke deployed Cloud Functions
   *          because that CF doesn't have this new Cyborg's OAuth client ID
   *          (this will be automatically created when the GCP is set to this
   *          Apps Script.)
   *       b. Only Cloud Fucntions deployed in the same GCP (the one back Apps
   *          Script) can be invoked. CF in other GCP will have an access error.
   * 2. `callFunction`
   *    (+) based on user's OAuth token which will work in the previous two
   *        scenarios.
   *    (-) Rate limit: 16 requests / 100 seconds.
   *        @see https://cloud.google.com/functions/quotas#rate_limits
   * TODO: Set to the second one now, if the rate is problem, add a setting to
   * allow users select the first one if possible.
   */
  invoke(functionName, payload = {}) {
    return this.callFunction(functionName, payload);
  }

  /**
   * Posts payload to the target Cloud Function and returns the response.
   * @param {string} functionName Name of the Cloud Function.
   * @param {object=} payload
   * @return
   */
  directPost(functionName, payload = {}) {
    return this.mutate(functionName, payload);
  }

  /**
   * Synchronously invokes a deployed Cloud Function.
   * @param {string} functionName
   * @param {object=} payload
   * @return
   */
  callFunction(functionName, payload = {}) {
    const proxy = new CloudFunctions(this.projectId, this.locationId);
    const { error, result } = proxy.call(functionName, JSON.stringify(payload));
    if (result) {
      try {
        return JSON.parse(result);
      } catch (e) {
        return result;
      }
    }
    throw error;
  }

}
