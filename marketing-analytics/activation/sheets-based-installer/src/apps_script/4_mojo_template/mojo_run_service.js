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
 * Generates a Cloud Run Service revision template object by parsing defined
 * properties from the provided attributes map.
 * @private
 * @param {!Object} attributesMap - A map of resource attributes.
 * @return {!Object} The formatted Cloud Run Service revision template
 *     configuration.
 */
MojoFunctions.getCloudRunServiceTemplate_ = attributesMap => {
  return MojoFunctions.buildPayloadFromAttributes(
    attributesMap,
    CloudRunService.ATTRIBUTE_NAMES,
    MojoFunctions.transformCloudRunValue,
  );
};

/**
 * Checks the current status and configuration of a Cloud Run Service.
 * Compares the deployed configuration against the expected configuration
 * to determine if an update or creation is required.
 * @param {string} name - The name of the Cloud Run Service to check.
 * @param {!Object} resources - The resources object containing the
 *     attributesMap for the expected configuration.
 * @return {!CheckResult} An object containing the status, message, and optional
 *     console link.
 */
MojoFunctions.checkCloudRunServices = (name, resources) => {
  const {attributesMap} = resources;
  let expectedTemplate;
  try {
    expectedTemplate = MojoFunctions.getCloudRunServiceTemplate_(attributesMap);
  } catch (error) {
    if (
      error.message.indexOf('No image found for ') > -1 ||
      error.message.indexOf('is not ready yet') > -1
    ) {
      return {
        status: RESOURCE_STATUS.PENDING,
        message: error.message,
      };
    } else {
      return {
        status: RESOURCE_STATUS.ERROR,
        message: error.message,
      };
    }
  }
  console.log('expected', JSON.stringify(expectedTemplate));
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const cloudRun = new CloudRunService(projectId, locationId);
  const response = cloudRun.getService(name);
  const {error} = response;
  if (error) {
    if (error.status === 'NOT_FOUND' || error.code === 404) {
      return {
        status: RESOURCE_STATUS.TO_APPLY,
        message: 'Cloud Run Service is not installed yet. Ready to apply.',
      };
    }
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
  const updatedConfig = diffApiResponseObjects(response, expectedTemplate);
  console.log('response', JSON.stringify(response));
  console.log('updatedConfig', JSON.stringify(updatedConfig));
  const consoleLink = cloudRun.getLinkToCloudConsole(name);
  if (!updatedConfig) {
    return {
      status: RESOURCE_STATUS.OK,
      message: 'Cloud Run Service configuration verified up-to-date.',
      status_link: consoleLink,
    };
  } else {
    console.log('Cloud Run Service differs', JSON.stringify(updatedConfig));
    return {
      status: RESOURCE_STATUS.TO_APPLY,
      message:
        'Cloud Run Service configuration differs from the expected template. Ready to apply updates.',
      status_link: consoleLink,
    };
  }
};
/**
 * Returns the result (CheckResult) of an operation of a Cloud Run Service.
 * @param {string} operationName Operation name of deploying a Cloud Run
 *     Service.
 * @return {!CheckResult}
 */
MojoFunctions.getCloudRunServiceStatus = operationName => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const cloudRunService = new CloudRunService(projectId, locationId);
  const rawResponse = cloudRunService.getOperation(operationName);

  if (rawResponse.error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: rawResponse.error,
    };
  } else if (rawResponse.done === true) {
    const {name} = rawResponse.response;
    const serviceId = name.split('/').slice(-1)[0];
    const consoleLink = cloudRunService.getLinkToCloudConsole(serviceId);
    return {
      status: RESOURCE_STATUS.OK,
      message: `Cloud Run Service has been created/updated.`,
      status_link: consoleLink,
    };
  }
  return {
    status: '',
    message: 'Deploying ',
    refreshFn: () => {
      return MojoFunctions.getCloudRunServiceStatus(operationName);
    },
  };
};

/**
 * Creates or updates a Cloud Run Service with the specified configuration.
 * Generates the required template from the provided resources and applies it.
 * @param {string} serviceId - The identifier for the Cloud Run Service.
 * @param {!Object} resources - The resources object containing the
 *     attributesMap for the configuration.
 * @return {!CheckResult} An object containing the operation status, message,
 *     and console link.
 */
MojoFunctions.createOrUpdateCloudRunServices = (serviceId, resources) => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const cloudRunService = new CloudRunService(projectId, locationId);

  const {attributesMap} = resources;
  const templatePayload =
    MojoFunctions.getCloudRunServiceTemplate_(attributesMap);
  console.log(
    'Creating or updating Cloud Run Service',
    serviceId,
    JSON.stringify(templatePayload),
  );
  const operation = cloudRunService.asyncCreateOrUpdateService(
    serviceId,
    templatePayload,
  );

  const {name, error} = operation;
  if (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
  return MojoFunctions.getCloudRunServiceStatus(name);
};

/**
 * Checks the availability of a Cloud Run Service URL.
 * @param {string} name - The name of the Cloud Run Service to check.
 * @param {!Object} resources - The resources object containing the
 *     attributesMap for the configuration.
 * @return {!CheckResult} An object containing the status, message, and optional
 *     console link.
 */
MojoFunctions.checkCloudRunServiceUrl = (name, resources) => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const cloudRun = new CloudRunService(projectId, locationId);
  try {
    const url = cloudRun.getServiceUrl(resources.attributeValue);
    return {
      value: url,
      status: url ? RESOURCE_STATUS.OK : RESOURCE_STATUS.ERROR,
      message: url
        ? 'Cloud Run Service URL is available.'
        : 'Cloud Run Service URL is not available.',
    };
  } catch (error) {
    if (error.message.includes('not found')) {
      return {
        status: RESOURCE_STATUS.PENDING,
        message:
          'Cloud Run Service is not ready yet. Please check the service status and try again later.',
      };
    }
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
};

MojoTemplateRegistry.register('cloudRunService', {
  category: 'Google Cloud',
  resource: 'Cloud Run Service',
  value: '${namespace|hyphened}-service',
  attributes: [
    {
      attributeName:
        CloudRunService.ATTRIBUTE_NAMES['template.containers.0.image'],
    },
    {
      attributeName:
        CloudRunService.ATTRIBUTE_NAMES[
          'template.containers.0.resources.limits.cpu'
        ],
      attributeValue: '2',
    },
    {
      attributeName:
        CloudRunService.ATTRIBUTE_NAMES[
          'template.containers.0.resources.limits.memory'
        ],
      attributeValue: '4',
    },
    {
      attributeName: CloudRunService.ATTRIBUTE_NAMES['template.timeout'],
      attributeValue: '3600',
    },
    {
      attributeName:
        CloudRunService.ATTRIBUTE_NAMES[
          'template.maxInstanceRequestConcurrency'
        ],
      attributeValue: '80', // the GCP default is often 80
    },
    {
      attributeName: CloudRunService.ATTRIBUTE_NAMES.ingress,
      attributeValue: CloudRunService.INGRESS_TRAFFIC.INTERNAL,
      attributeValue_datarange: Object.values(CloudRunService.INGRESS_TRAFFIC),
    },
    {
      attributeName: CloudRunService.ATTRIBUTE_NAMES['template.serviceAccount'],
      attributeValue: '${serviceAccount}',
    },
  ],
  groupType: GROUP_TYPE.COLLAPSE,
  propertyName: 'runServiceId',
  alwaysCheck: true,
  checkFn: MojoFunctions.checkCloudRunServices,
  enableFn: MojoFunctions.createOrUpdateCloudRunServices,
});

MojoTemplateRegistry.register('cloudRunServiceUrl', {
  category: 'Google Cloud',
  resource: 'Cloud Run Service URL',
  attributeName: 'Cloud Run Service',
  checkFn: MojoFunctions.checkCloudRunServiceUrl,
});
