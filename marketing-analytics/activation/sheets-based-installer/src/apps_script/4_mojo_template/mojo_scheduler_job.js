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
 * Transforms Cloud Scheduler Job attribute values.
 * Standardizes PubSub topic resource name and base64-encodes payload data.
 * @private
 * @param {string} dotPath Dotted property path.
 * @param {*} rawValue Raw attribute value.
 * @return {*} Transformed value.
 */
MojoFunctions.transformSchedulerValue_ = (dotPath, rawValue) => {
  if (dotPath === 'pubsubTarget.topicName') {
    const projectId = getDocumentProperty('projectId');
    const topicStr = String(rawValue).trim();
    if (!topicStr.startsWith('projects/')) {
      return `projects/${projectId}/topics/${topicStr}`;
    }
    return topicStr;
  }
  if (dotPath === 'pubsubTarget.data') {
    const rawStr = String(rawValue);
    return Utilities.base64Encode(rawStr, Utilities.Charset.UTF_8);
  }
  if (dotPath === 'pubsubTarget.attributes') {
    if (typeof rawValue === 'object' && rawValue !== null) {
      return rawValue;
    }
    const rawStr = String(rawValue).trim();
    if (rawStr.startsWith('{')) {
      try {
        return JSON.parse(rawStr);
      } catch (e) {
        console.warn(
          'Failed to parse JSON for Cloud Scheduler pubsubTarget.attributes:',
          rawStr,
        );
      }
    }
    return rawValue;
  }
  return rawValue;
};

/**
 * Combined one-step check and idempotent update/create for Cloud Scheduler
 * Jobs. Verifies current scheduler job configuration and creates or updates
 * immediately if needed.
 * @param {string} jobId The identifier for the Cloud Scheduler Job.
 * @param {!Object} resources The resources object containing attributesMap.
 * @return {!CheckResult} Standardized status, descriptive outcome message, and
 *     console link.
 */
MojoFunctions.checkOrUpdateSchedulerJob = (jobId, resources) => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const cloudScheduler = new CloudScheduler(projectId, locationId);
  const statusLink = cloudScheduler.getLinkToCloudConsole();

  const {attributesMap} = resources;
  const expectedPayload = MojoFunctions.buildPayloadFromAttributes(
    attributesMap,
    CloudScheduler.ATTRIBUTE_NAMES,
    MojoFunctions.transformSchedulerValue_,
  );

  if (attributesMap[CloudScheduler.ATTRIBUTE_NAMES['httpTarget.uri']]) {
    const uri =
      expectedPayload.httpTarget && expectedPayload.httpTarget.uri
        ? String(expectedPayload.httpTarget.uri).trim()
        : '';
    if (!uri.startsWith('https://') && !uri.startsWith('http://')) {
      return {
        status: RESOURCE_STATUS.PENDING,
        message: `Target URI is not ready yet${
          uri ? ` (current: "${uri}")` : ''
        }. Waiting for the upstream service URL.`,
      };
    }
  }

  return MojoFunctions.reconcileDeclarativeResource(
    'Cloud Scheduler Job',
    cloudScheduler.getJob(jobId),
    targetPayload => cloudScheduler.createOrUpdateJob(jobId, targetPayload),
    expectedPayload,
    statusLink,
  );
};

MojoTemplateRegistry.register('schedulerJob', {
  category: 'Google Cloud',
  resource: 'Scheduler Job',
  value: '${namespace|hyphened}-scheduler-job',
  attributes: [
    {
      attributeName: CloudScheduler.ATTRIBUTE_NAMES.schedule,
      attributeValue: '*/5 * * * *',
    },
    {
      attributeName: CloudScheduler.ATTRIBUTE_NAMES.timeZone,
      attributeValue: '${timeZone}',
    },
  ],
  groupType: GROUP_TYPE.COLLAPSE,
  checkFn: MojoFunctions.checkOrUpdateSchedulerJob,
});

MojoTemplateRegistry.register('schedulerHttpJob', {
  template: 'schedulerJob',
  attributes: [
    {
      attributeName: CloudScheduler.ATTRIBUTE_NAMES['httpTarget.uri'],
    },
    {
      attributeName: CloudScheduler.ATTRIBUTE_NAMES['httpTarget.httpMethod'],
      attributeValue: 'GET',
    },
    {
      attributeName:
        CloudScheduler.ATTRIBUTE_NAMES[
          'httpTarget.oidcToken.serviceAccountEmail'
        ],
      attributeValue: '${serviceAccount}',
    },
  ],
});
MojoTemplateRegistry.register('schedulerPubsubJob', {
  template: 'schedulerJob',
  attributes: [
    {
      attributeName: CloudScheduler.ATTRIBUTE_NAMES['pubsubTarget.topicName'],
    },
    {
      attributeName: CloudScheduler.ATTRIBUTE_NAMES['pubsubTarget.attributes'],
    },
    {
      attributeName: CloudScheduler.ATTRIBUTE_NAMES['pubsubTarget.data'],
    },
  ],
});
