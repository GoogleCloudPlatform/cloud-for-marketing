// Copyright 2026 Google Inc.
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

/** @fileoverview Cloud Asset Inventory API handler class. */

class CloudAsset extends GcpApiBase {
  /**
   * @param {string=} projectId
   */
  constructor(projectId) {
    super();
    this.name = 'Cloud Asset API';
    this.api = 'cloudasset.googleapis.com';
    this.permissions = ['cloudasset.assets.searchAllResources'];
    this.adminRole = 'Cloud Asset Viewer';
    this.runtimeRoles = [];
    this.apiUrl = `https://${this.api}`;
    this.version = 'v1';
    this.projectId = projectId;
  }

  /**
   * @override
   * @return {string}
   */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /**
   * Lists assets with the specified asset types.
   * @see https://cloud.google.com/asset-inventory/docs/reference/rest/v1/assets/list
   * @param {string|!Array<string>} assetTypes A list of asset types to take a
   *     snapshot for.
   * @return {!Array<!Object>} An array of assets matching the specified types.
   */
  listAssets(assetTypes) {
    const types = Array.isArray(assetTypes) ? assetTypes : [assetTypes];
    const parameters = {
      assetTypes: types,
    };
    return this.list('assets', `projects/${this.projectId}/assets`, parameters);
  }
}
