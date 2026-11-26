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
 * Generates a Cloud Run Job template object by inheriting Service container
 * attributes and adapting into the Job execution template hierarchy.
 * @private
 * @param {!Object} attributesMap - A map of resource attributes.
 * @return {Object} The formatted Cloud Run Job template configuration.
 */
MojoFunctions.getCloudRunJobTemplate_ = attributesMap => {
  return MojoFunctions.buildPayloadFromAttributes(
    attributesMap,
    CloudRunJob.ATTRIBUTE_NAMES,
    MojoFunctions.transformCloudRunValue,
  );
};

/**
 * Checks the current status and configuration of a Cloud Run Job.
 * Compares the deployed configuration against the expected configuration
 * to determine if an update or creation is required.
 * @param {string} name - The name of the Cloud Run Job to check.
 * @param {!Object} resources - The resources object containing the
 *     attributesMap for the expected configuration.
 * @return {!CheckResult} An object containing the status, message, and optional
 *     console link.
 */
MojoFunctions.checkCloudRunJobs = (name, resources) => {
  const {attributesMap} = resources;
  let expectedTemplate;
  try {
    expectedTemplate = MojoFunctions.getCloudRunJobTemplate_(attributesMap);
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
  console.log('expected config:', JSON.stringify(expectedTemplate));
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const cloudRun = new CloudRunJob(projectId, locationId);
  const response = cloudRun.getJob(name);
  const {error} = response;
  if (error) {
    if (error.status === 'NOT_FOUND' || error.code === 404) {
      return {
        status: RESOURCE_STATUS.TO_APPLY,
        message: 'Cloud Run Job is not installed yet. Ready to apply.',
      };
    }
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }

  const updatedConfig = diffApiResponseObjects(response, expectedTemplate);
  console.log('actual config:', JSON.stringify(response));
  const link = cloudRun.getLinkToCloudConsole(name);
  if (!updatedConfig) {
    return {
      status: RESOURCE_STATUS.OK,
      message: `Cloud Run Job configuration verified up-to-date.`,
      status_link: link,
    };
  } else {
    console.log('Cloud Run Job differs', JSON.stringify(updatedConfig));
    return {
      status: RESOURCE_STATUS.TO_APPLY,
      message:
        'Cloud Run Job configuration differs from the expected template. Ready to apply updates.',
      status_link: link,
    };
  }
};

/**
 * Returns the result (CheckResult) of an operation of a Cloud Run Job.
 * Cloud Run Job deployment is very fast.
 * @param {string} operationName Operation name of deploying a Cloud Run Job.
 * @return {!CheckResult}
 */
MojoFunctions.getCloudRunJobStatus = operationName => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const cloudRunJob = new CloudRunJob(projectId, locationId);
  const rawResponse = cloudRunJob.getOperation(operationName);

  if (rawResponse.error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: rawResponse.error,
    };
  } else if (rawResponse.done === true) {
    const {name} = rawResponse.response;
    const jobId = name.split('/').slice(-1)[0];
    const consoleLink = cloudRunJob.getLinkToCloudConsole(jobId);
    return {
      status: RESOURCE_STATUS.OK,
      message: `Cloud Run Job has been created/updated.`,
      status_link: consoleLink,
    };
  }
  return {
    status: '',
    message: 'Deploying ',
    refreshFn: () => {
      return MojoFunctions.getCloudRunJobStatus(operationName);
    },
  };
};

/**
 * Creates or updates a Cloud Run Job with the specified configuration.
 * Generates the required template from the provided resources and applies it.
 * @param {string} jobId - The identifier for the Cloud Run Job.
 * @param {!Object} resources - The resources object containing the
 *     attributesMap for the configuration.
 * @return {!CheckResult} An object containing the operation status, message,
 *     and console link.
 */
MojoFunctions.createOrUpdateCloudRunJobs = (jobId, resources) => {
  console.log('Creating or updating Cloud Run Job', jobId, resources);
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const cloudRunJob = new CloudRunJob(projectId, locationId);

  const {attributesMap} = resources;
  const expectedTemplate = MojoFunctions.getCloudRunJobTemplate_(attributesMap);
  console.log('expected config:', JSON.stringify(expectedTemplate));
  const operation = cloudRunJob.asyncCreateOrUpdateJob(jobId, expectedTemplate);

  const {name, error} = operation;
  if (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
  return MojoFunctions.getCloudRunJobStatus(name);
};

MojoTemplateRegistry.register('cloudRunJob', {
  category: 'Google Cloud',
  resource: 'Cloud Run Job',
  value: '${namespace|hyphened}-job',
  attributes: [
    {
      attributeName:
        CloudRunJob.ATTRIBUTE_NAMES['template.template.containers.0.image'],
    },
    {
      attributeName:
        CloudRunJob.ATTRIBUTE_NAMES[
          'template.template.containers.0.resources.limits.cpu'
        ],
      attributeValue: '2',
    },
    {
      attributeName:
        CloudRunJob.ATTRIBUTE_NAMES[
          'template.template.containers.0.resources.limits.memory'
        ],
      attributeValue: '4',
    },
    {
      attributeName: CloudRunJob.ATTRIBUTE_NAMES['template.template.timeout'],
      attributeValue: '3600',
    },
    {
      attributeName:
        CloudRunJob.ATTRIBUTE_NAMES['template.template.maxRetries'],
      attributeValue: '1',
    },
    {
      attributeName: CloudRunJob.ATTRIBUTE_NAMES['template.parallelism'],
    },
    {
      attributeName:
        CloudRunJob.ATTRIBUTE_NAMES['template.template.serviceAccount'],
      attributeValue: '${serviceAccount}',
    },
  ],
  groupType: GROUP_TYPE.COLLAPSE,
  propertyName: 'runJobId',
  alwaysCheck: true,
  checkFn: MojoFunctions.checkCloudRunJobs,
  enableFn: MojoFunctions.createOrUpdateCloudRunJobs,
});
