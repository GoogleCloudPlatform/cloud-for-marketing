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

/** @fileoverview MojoTemplate for Google Cloud Project resource. */
/**
 * Checks GCP existence and billing status.
 * @param {string} projectId
 * @return {!CheckResult}
 */
MojoFunctions.checkProject = projectId => {
  const projectNumber = /^[a-z][a-z0-9-]{4,28}[a-z0-9]$/;
  const result = projectNumber.test(projectId);
  if (!result) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message:
        'GCP Project ID must be 6 to 30 characters long, start with a lowercase letter, end with a letter or number, and contain only lowercase letters, numbers, and hyphens.',
    };
  }
  const resourceManager = new CloudResourceManager(projectId);
  const {error, lifecycleState} = resourceManager.getProject();
  let status = RESOURCE_STATUS.ERROR;
  let message = '';
  if (error) {
    message = error.message;
  } else if (lifecycleState !== 'ACTIVE') {
    message = `Wrong project status ${lifecycleState}`;
  } else {
    const cloudBilling = new CloudBilling(projectId);
    const {billingEnabled} = cloudBilling.getBillingInfo();
    if (!billingEnabled) {
      message = 'No billing account.';
    } else {
      status = RESOURCE_STATUS.OK;
    }
  }
  return {
    status,
    message,
    status_link: resourceManager.getLinkToCloudConsole(),
  };
};

/**
 * Checks current user's permission.
 * Used property: projectId.
 * @param {string} permission
 * @return {!CheckResult}
 */
MojoFunctions.checkPermission = permission => {
  const projectId = getDocumentProperty('projectId');
  const resourceManager = new CloudResourceManager(projectId);
  const result = resourceManager.testPermissions([permission]);
  if (result) {
    return {
      status: RESOURCE_STATUS.OK,
      status_link: resourceManager.getLinkToPermissionInfo(permission),
    };
  }
  return {
    status: RESOURCE_STATUS.ERROR,
    message: 'MISSING',
    status_link: resourceManager.getLinkToPermissionInfo(permission),
  };
};

/**
 * Grants an IAM role to a principal on the project if not already present.
 * Throws an exception if the authorization grant fails.
 * @private
 * @param {string} projectId Google Cloud project ID.
 * @param {string} member Target IAM member (e.g.
 *     'serviceAccount:foo@bar.iam.gserviceaccount.com').
 * @param {string} role Required IAM role (e.g. 'roles/eventarc.eventReceiver').
 */
MojoFunctions.grantIamRoleIfNeeded = (projectId, member, role) => {
  if (!member || !role) return;
  const formattedMember =
    member.startsWith('serviceAccount:') || member.startsWith('user:')
      ? member
      : `serviceAccount:${member}`;
  const resourceManager = new CloudResourceManager(projectId);
  const existingRoles = resourceManager.getIamPolicyForMember(formattedMember);
  if (existingRoles.includes(role)) {
    console.log(`IAM role ${role} verified on ${formattedMember}.`);
    return;
  }
  const granted = resourceManager.addIamPolicyBinding(formattedMember, role);
  if (!granted) {
    throw new Error(`Failed to grant IAM role ${role} to ${formattedMember}.`);
  }
  console.log(`Granted IAM role ${role} to ${formattedMember}.`);
};

/**
 * Checks and enables the API if it is not enabled.
 * Used property: projectId.
 * @param {string} apiName
 * @param {!Object} resources
 * @return  {!CheckResult}
 */
MojoFunctions.checkOrEnableApi = (apiName, resources) => {
  const api = resources.attributeValue;
  const projectId = getDocumentProperty('projectId');
  const serviceUsage = new ServiceUsage(projectId);
  const statusLink = serviceUsage.getLinkToCloudConsole(api);
  const {error, state} = serviceUsage.getService(api);
  if (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      status_link: statusLink,
      attributeValue_link: statusLink,
      message: error.message,
    };
  }
  if (state === 'ENABLED') {
    return {
      status: RESOURCE_STATUS.OK,
      status_link: statusLink,
      attributeValue_link: statusLink,
      message: `${apiName} is enabled in project ${projectId}.`,
    };
  }
  const {service, error: enableError} = serviceUsage.enableService(api);
  const updatedState = service && service.state;
  if (updatedState === 'ENABLED') {
    return {
      status: RESOURCE_STATUS.OK,
      status_link: statusLink,
      attributeValue_link: statusLink,
      message: `${apiName} is ${updatedState} in project ${projectId}`,
    };
  }
  return {
    status: RESOURCE_STATUS.ERROR,
    status_link: statusLink,
    attributeValue_link: statusLink,
    message:
      enableError?.message ||
      `Fail to enable ${apiName} in project ${projectId}`,
  };
};

MojoTemplateRegistry.register('projectId', {
  category: 'Google Cloud',
  resource: 'Project Id',
  propertyName: 'projectId',
  editType: RESOURCE_EDIT_TYPE.USER_INPUT,
  checkFn: MojoFunctions.checkProject,
});

MojoTemplateRegistry.register('permissions', {
  category: 'Google Cloud',
  resource: 'Permissions',
  editType: RESOURCE_EDIT_TYPE.READONLY,
  optionalType: OPTIONAL_TYPE.DEFAULT_CHECKED,
  groupType: GROUP_TYPE.COLLAPSE,
  attributeName: 'Predefined role',
  checkFn: MojoFunctions.checkPermission,
});

MojoTemplateRegistry.register('apis', {
  category: 'Google Cloud',
  resource: 'APIs',
  editType: RESOURCE_EDIT_TYPE.READONLY,
  groupType: GROUP_TYPE.COLLAPSE,
  checkFn: MojoFunctions.checkOrEnableApi,
});

/**
 *
 */
ApiBase.registerInterceptor((error, apiClient) => {
  if (
    error.code === 403 &&
    error.message.indexOf('has not been used in project') > -1 &&
    apiClient.constructor.name !== 'ServiceUsage'
  ) {
    const regex = /not been used in project ([0-9]*) before or it is disabled/;
    const projectId = regex.exec(error.message)[1];
    if (projectId) {
      console.log(
        `Intercepted Disabled API. Attempting to enable: ${apiClient.api}`,
      );
      try {
        const serviceUsage = new ServiceUsage(projectId);
        return serviceUsage.checkOrEnableService(apiClient.api);
      } catch (error) {
        console.error('Auto-enable failed.', error);
        return false;
      }
    }
  }
  return false;
});
