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

/** @fileoverview Apps Script API class. */

class AppsScript extends ApiBase {
  constructor() {
    super();
    this.name = 'Apps Script API';
    this.api = 'script.googleapis.com';
    this.scriptId = ScriptApp.getScriptId();
    this.apiUrl = `https://${this.api}`;
    this.version = 'v1';
  }

  /**
   * @override
   * @return {string}
   */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/projects/${this.scriptId}`;
  }

  /**
   * Gets a script project's metadata.
   * @return {!Object}
   */
  getScript() {
    return this.get('');
  }

  /**
   * Lists the deployments of an Apps Script project.
   * @return {!Array<!Object>}
   */
  listDeployments() {
    return this.list('deployments', 'deployments');
  }

  /**
   * Gets an existing deployment by its description.
   * @param {*} description
   * @return {!Object|undefined}
   */
  getDeploymentByDescription(description) {
    const deploymentsData = this.listDeployments();
    if (deploymentsData.error) {
      throw new Error(
        deploymentsData.error.content || JSON.stringify(deploymentsData.error),
      );
    }
    return deploymentsData.find(
      ({deploymentConfig: config = {}}) => config.description === description,
    );
  }

  /**
   * Creates or updates a Web App deployment for the current script.
   * @param {string} description The description to identify this deployment.
   * @return {{versionNumber: number, url: string}} The created/updated
   *     deployment details.
   */
  createOrUpdateDeployment(description) {
    const versionData = this.mutate('versions', {description}, 'POST');
    if (versionData.error) {
      throw new Error(
        versionData.error.content || JSON.stringify(versionData.error),
      );
    }
    const versionNumber = versionData.versionNumber;

    const existingDeployment = this.getDeploymentByDescription(description);

    let deploymentData;
    const deploymentConfig = {
      versionNumber,
      description,
    };
    if (existingDeployment) {
      deploymentData = this.mutate(
        `deployments/${existingDeployment.deploymentId}`,
        {deploymentConfig},
        'PUT',
      );
    } else {
      deploymentData = this.mutate('deployments', deploymentConfig, 'POST');
    }
    if (deploymentData.error) {
      throw new Error(
        deploymentData.error.content || JSON.stringify(deploymentData.error),
      );
    }
    return {
      versionNumber,
      url: this.getDeploymentUrl(deploymentData),
    };
  }

  /**
   * Gets the URL of a deployment if it has a web app entry point.
   * @param {*} deployment
   * @returns {string|null} The web app URL or null if not found.
   */
  getDeploymentUrl(deployment) {
    const webAppEntryPoint = (deployment.entryPoints || []).find(
      ({entryPointType}) => entryPointType === 'WEB_APP',
    );
    const webAppUrl = webAppEntryPoint?.webApp?.url;
    return webAppUrl;
  }
}
