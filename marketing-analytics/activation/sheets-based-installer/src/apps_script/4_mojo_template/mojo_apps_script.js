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

/**
 * Checks the status of the Apps Script Web App deployment.
 * @param {string} name - The deployment description.
 * @param {!Object} resources - The resource entry from Mojo sheet.
 * @return {!CheckResult} Result object containing status, message, and
 *     status_link.
 */
MojoFunctions.checkAppsScriptWebApp = (name, resources) => {
  const description = name || 'auto-deployed-webapp';
  const appsScript = new AppsScript();

  try {
    const deployment = appsScript.getDeploymentByDescription(description);

    if (!deployment) {
      return {
        status: RESOURCE_STATUS.TO_APPLY,
        message: 'Web app is not deployed. Please apply changes to deploy.',
      };
    }

    const projectData = appsScript.getScript();
    if (projectData.error) {
      throw new Error(
        projectData.error.content || JSON.stringify(projectData.error),
      );
    }

    const projectModifiedMs = new Date(projectData.updateTime).getTime();
    const deploymentModifiedMs = new Date(deployment.updateTime).getTime();
    const webAppUrl = appsScript.getDeploymentUrl(deployment);

    if (projectModifiedMs > deploymentModifiedMs) {
      return {
        status: RESOURCE_STATUS.TO_APPLY,
        message:
          'Apps Script source code was updated since the last deployment. Ready to apply updates.',
        status_link: webAppUrl,
      };
    }

    return {
      status: RESOURCE_STATUS.OK,
      message: `Web app is up-to-date (Version ${
        deployment.deploymentConfig.versionNumber
      }).`,
      status_link: webAppUrl,
    };
  } catch (e) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: e.toString(),
    };
  }
};

/**
 * Deploys (creates or updates) the Apps Script Web App.
 * @param {string} name - The deployment description.
 * @param {!Object} resources - The resource entry from Mojo sheet.
 * @return {!CheckResult} Result object for deployment status.
 */
MojoFunctions.deployAppsScriptWebApp = (name, resources) => {
  const description = name || 'auto-deployed-webapp';
  const api = new AppsScript();

  try {
    const result = api.createOrUpdateDeployment(description);
    return {
      status: RESOURCE_STATUS.OK,
      message: `Deployed version: ${result.versionNumber}`,
      value_link: result.url,
      status_link: result.url,
    };
  } catch (e) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: e.toString(),
    };
  }
};

/** @fileoverview MojoTemplate for Apps Script Web App resource. */
MojoTemplateRegistry.register('appsScriptWebApp', {
  category: 'Google Workspace',
  resource: 'Apps Script Web App',
  value: 'auto-deployed-webapp',
  checkFn: MojoFunctions.checkAppsScriptWebApp,
  enableFn: MojoFunctions.deployAppsScriptWebApp,
});
