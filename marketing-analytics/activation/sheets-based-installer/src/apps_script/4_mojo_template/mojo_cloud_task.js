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
 * Transforms Cloud Tasks Queue attribute values.
 * Standardizes duration units ('s') and numeric rate/limit values.
 * @private
 * @param {string} dotPath Dotted property path.
 * @param {*} rawValue Cell value string.
 * @return {*} Transformed value.
 */
MojoFunctions.transformCloudTasksValue_ = (dotPath, rawValue) => {
  if (dotPath.endsWith('minBackoff') || dotPath.endsWith('maxBackoff')) {
    return String(rawValue)
      .trim()
      .replace(/\s*s?$/i, 's');
  }
  if (
    dotPath.endsWith('maxConcurrentDispatches') ||
    dotPath.endsWith('maxDispatchesPerSecond') ||
    dotPath.endsWith('maxAttempts') ||
    dotPath.endsWith('maxDoublings')
  ) {
    return Number(rawValue);
  }
  return rawValue;
};

/**
 * Combined one-step check and idempotent update/create for Cloud Tasks Queues.
 * @param {string} queueId The identifier for the Cloud Tasks Queue.
 * @param {!Object} resources - The resources object containing the
 *     attributesMap.
 * @return {!CheckResult} Standardized status, descriptive outcome message, and
 *     console link.
 */
MojoFunctions.checkOrUpdateCloudTasksQueue = (queueId, resources) => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const cloudTasks = new CloudTasks(projectId, locationId);
  const expectedPayload = MojoFunctions.buildPayloadFromAttributes(
    resources.attributesMap,
    CloudTasks.ATTRIBUTE_NAMES,
    MojoFunctions.transformCloudTasksValue_,
  );
  return MojoFunctions.reconcileDeclarativeResource(
    'Cloud Tasks Queue',
    cloudTasks.getQueue(queueId),
    payload => cloudTasks.createOrUpdateQueue(queueId, payload),
    expectedPayload,
    cloudTasks.getLinkToCloudConsole(queueId),
  );
};

MojoTemplateRegistry.register('cloudTasksQueue', {
  category: 'Google Cloud',
  resource: 'Cloud Tasks Queue',
  value: '${namespace|hyphened}-queue',
  attributes: [
    {
      attributeName:
        CloudTasks.ATTRIBUTE_NAMES['rateLimits.maxConcurrentDispatches'],
      attributeValue: '2',
    },
    {
      attributeName:
        CloudTasks.ATTRIBUTE_NAMES['rateLimits.maxDispatchesPerSecond'],
      attributeValue: '1',
    },
    {
      attributeName: CloudTasks.ATTRIBUTE_NAMES['retryConfig.maxAttempts'],
      attributeValue: '4',
    },
    {
      attributeName: CloudTasks.ATTRIBUTE_NAMES['retryConfig.minBackoff'],
      attributeValue: '60s',
    },
    {
      attributeName: CloudTasks.ATTRIBUTE_NAMES['retryConfig.maxBackoff'],
      attributeValue: '3600s',
    },
    {
      attributeName: CloudTasks.ATTRIBUTE_NAMES['retryConfig.maxDoublings'],
      attributeValue: '3',
    },
  ],
  propertyName: 'tasksQueueId',
  groupType: GROUP_TYPE.COLLAPSE,
  checkFn: MojoFunctions.checkOrUpdateCloudTasksQueue,
});
