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

// /**
//  * Returns the function to get the versions of the specified package and
//  return
//  * the check results.
//  *
//  * @param {string} packageName
//  * @return {function(string):!CheckResult}
//  */
// const getCheckFunctionForVersion = packageName => {
//   return version => {
//     const versions = getNpmVersions(packageName);
//     const latest = versions[0];
//     let message = '';
//     if (!version) {
//       message = 'By default, will install the latest version.';
//     } else if (version !== latest) {
//       message = `New version ${latest} is available.`;
//     }
//     return {
//       status: RESOURCE_STATUS.OK,
//       value: version ? version : latest,
//       value_datarange: versions,
//       message,
//       status_link:
//       `https://www.npmjs.com/package/${packageName}?activeTab=versions`,
//     };
//   };
// };

/**
 * Returns the status of a Cloud Functions.
 * @param {*} response Response of get or deploy the Cloud Functions.
 * @return {!CheckResult}
 * @private
 */
MojoFunctions.getCloudFunctionsResult_ = response => {
  const {
    state,
    updateTime,
    serviceConfig: {environmentVariables} = {},
  } = response;
  const {VERSION: currentVersion = ''} = environmentVariables;
  const message = currentVersion
    ? `v${currentVersion} was deployed at ${updateTime}.`
    : `Deployed at ${updateTime}.`;
  if (state !== 'ACTIVE') {
    return {
      status: RESOURCE_STATUS.TO_APPLY,
      attributeValue: '',
      message: `Wrong status: ${state}. ${message}`,
    };
  }
  return {
    status: RESOURCE_STATUS.OK,
    attributeValue: currentVersion,
    status_link: getLinkToCloudFunction(response),
    message: `${message}`,
  };
};

/**
 * Checks the status and version of a Cloud Functions.
 * This is not a 'default' gcloud function as a Mojo checking function because
 * the expected version can not get by default. This is used in 'Tentacles' and
 * 'Sentinel' Mojo sheet.
 * @param {string} name The name of a Cloud Functions.
 * @param {!object} expectedProperties Properties that target Cloud Functions
 *   should have the expected values, e.g. version, generation(GEN_1 or GEN_2).
 * @param {string} expectedProperties.expectedVersion The expected version of
 *   the Cloud Functions. The value is from the Cloud Functions environment
 *   variable 'VERSION'.
 * @param {string} expectedProperties.expectedGeneration The expected generation
 *   of the Cloud Functions. The value is from the Cloud Functions environment
 *   variable 'VERSION'.
 * @return {!CheckResult}
 */
MojoFunctions.checkCloudRunFunctions = (name, expectedProperties) => {
  const {expectedVersion, expectedGeneration = DEFAULT_GENERATION} =
    expectedProperties;

  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const cloudFunction = new CloudRunFunction(projectId, locationId);
  const response = cloudFunction.getFunction(name);
  const {error} = response;
  if (error) {
    if (error.status === 'NOT_FOUND') {
      return {
        status: RESOURCE_STATUS.TO_APPLY,
        attributeValue: '',
        message: 'Cloud Run Function is not installed yet. Ready to apply.',
      };
    }
    return {
      status: RESOURCE_STATUS.ERROR,
      attributeValue: '',
      message: error.message,
    };
  }
  const databaseId = getDocumentProperty('databaseId');
  const {updateTime, state, environment: generation} = response;
  const environmentVariables = response.serviceConfig
    ? response.serviceConfig.environmentVariables
    : {};
  const {VERSION: currentVersion = '', DATABASE_ID: currentDatabaseId} =
    environmentVariables;
  if (generation !== expectedGeneration) {
    return {
      status: RESOURCE_STATUS.TO_APPLY,
      attributeValue: currentVersion,
      message: `There has been a ${
        generation
      } Cloud Function. Continue applying changes will delete that.`,
    };
  }
  const message = currentVersion
    ? `v${currentVersion} was deployed at ${updateTime}.`
    : `Deployed at ${updateTime}.`;
  if (expectedVersion !== currentVersion) {
    return {
      status: RESOURCE_STATUS.TO_APPLY,
      attributeValue: currentVersion,
      message: `${message} Will deploy v${expectedVersion}`,
    };
  }
  if (currentDatabaseId && currentDatabaseId !== databaseId) {
    return {
      status: RESOURCE_STATUS.TO_APPLY,
      attributeValue: currentVersion,
      message:
        `${message} Will deploy v${expectedVersion}` +
        ` and replace Firestore ${currentDatabaseId} -> ${databaseId}.`,
    };
  }
  return MojoFunctions.getCloudFunctionsResult_(response);
};

/**
 * Returns the result (CheckResult) of an operation of Cloud Functions
 * deployment.
 * After a Cloud Function deployment is initiated, this function is used to
 * poll and generate a unified CheckResult based on the LRO operation name.
 * @param {string} operationName Operation name of deploying a Cloud Function.
 * @param {string} locationId
 * @param {string} projectId
 * @return {!CheckResult}
 */
MojoFunctions.getCloudFunctionDeployStatus = (
  operationName,
  locationId,
  projectId,
) => {
  const cloudFunction = new CloudRunFunction(projectId, locationId);
  const rawResponse = cloudFunction.getOperation(operationName);
  const {done, error, response} = rawResponse;
  if (done) {
    if (error) {
      return {
        status: RESOURCE_STATUS.ERROR,
        attributeValue: '',
        message: error.message,
      };
    }
    return MojoFunctions.getCloudFunctionsResult_(response);
  }
  return {
    status: '',
    message: 'Deploying ',
    refreshFn: () => {
      return MojoFunctions.getCloudFunctionDeployStatus(
        operationName,
        locationId,
        projectId,
      );
    },
  };
};

MojoTemplateRegistry.register('cloudFunctions', {
  category: 'Google Cloud',
  resource: 'Cloud Functions',
});
