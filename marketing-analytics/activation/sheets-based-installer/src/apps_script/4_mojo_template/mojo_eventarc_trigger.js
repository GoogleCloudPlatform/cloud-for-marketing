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
 * Verifies and grants required IAM roles for an Eventarc trigger.
 * Grants 'roles/iam.serviceAccountTokenCreator' to the Pub/Sub service agent
 * and 'roles/pubsub.publisher' to the Cloud Storage service agent when Cloud
 * Storage event filters are present.
 * @private
 * @param {string} projectId Target Google Cloud project ID.
 * @param {!Object} triggerPayload Trigger configuration containing
 *     eventFilters.
 */
MojoFunctions.verifyEventarcTriggerIamRoles_ = (projectId, triggerPayload) => {
  const hasStorageFilter =
    Array.isArray(triggerPayload.eventFilters) &&
    triggerPayload.eventFilters.some(
      f =>
        f.attribute === 'type' &&
        String(f.value).startsWith('google.cloud.storage.'),
    );
  if (hasStorageFilter) {
    const resourceManager = new CloudResourceManager(projectId);
    const projectInfo = resourceManager.getProject();
    if (projectInfo && projectInfo.projectNumber) {
      const pubsubSa =
        `serviceAccount:service-${projectInfo.projectNumber}` +
        `@gcp-sa-pubsub.iam.gserviceaccount.com`;
      MojoFunctions.grantIamRoleIfNeeded(
        projectId,
        pubsubSa,
        'roles/iam.serviceAccountTokenCreator',
      );
      const storageSa =
        `serviceAccount:service-${projectInfo.projectNumber}` +
        `@gs-project-accounts.iam.gserviceaccount.com`;
      MojoFunctions.grantIamRoleIfNeeded(
        projectId,
        storageSa,
        'roles/pubsub.publisher',
      );
    }
  }
};

/**
 * Transforms Eventarc attribute values.
 * Parses comma-separated key=value filter strings using strict official
 * Eventarc filter attributes ('bucket', 'topic', 'type'). Automatically
 * supplies standard event types if 'type' is omitted.
 * @private
 * @param {string} dotPath Dotted property path.
 * @param {*} rawValue Cell value string.
 * @return {*} Transformed value or parsed eventFilters array.
 */
MojoFunctions.transformEventarcValue_ = (dotPath, rawValue) => {
  // if (dotPath === 'eventFilters' && typeof rawValue === 'string') {
  //   const filtersMap = new Map();
  //   String(rawValue)
  //     .split(',')
  //     .forEach(pair => {
  //       const parts = pair.split('=').map(s => s.trim());
  //       if (parts.length >= 2 && parts[0] && parts[1]) {
  //         filtersMap.set(parts[0], parts.slice(1).join('='));
  //       }
  //     });

  //   // Automatically default standard Eventarc types if 'type' is omitted
  //   if (!filtersMap.has('type')) {
  //     if (filtersMap.has('bucket')) {
  //       filtersMap.set('type', 'google.cloud.storage.object.v1.finalized');
  //     } else if (filtersMap.has('topic')) {
  //       filtersMap.set('type',
  //       'google.cloud.pubsub.topic.v1.messagePublished');
  //     }
  //   }

  //   const filters = [];
  //   filtersMap.forEach((val, key) => {
  //     filters.push({attribute: key, value: val});
  //   });
  //   return filters;
  // }
  if (dotPath === 'eventFilters.0') {
    return {
      attribute: 'type',
      value: rawValue,
    };
  }
  if (dotPath === 'eventFilters.1') {
    return {
      attribute: 'bucket',
      value: rawValue,
    };
  }
  return rawValue;
};

/**
 * Combined one-step check and idempotent update/create for Eventarc Triggers.
 * Verifies current trigger configuration and creates or updates immediately if
 * needed.
 * @param {string} triggerId The identifier for the Eventarc Trigger.
 * @param {!Object} resources The resources object containing attributesMap.
 * @return {!CheckResult} Standardized status, descriptive outcome message, and
 *     console link.
 */
MojoFunctions.checkOrUpdateEventarcTrigger = (triggerId, resources) => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const cloudRun = new CloudRunService(projectId, locationId);
  const destinationServiceName =
    resources.attributesMap[
      Eventarc.ATTRIBUTE_NAMES['destination.cloudRun.service']
    ].attributeValue;
  if (!cloudRun.existService(destinationServiceName)) {
    return {
      status: RESOURCE_STATUS.PENDING,
      message: `Cloud Run Service ${
        destinationServiceName
      } is not ready yet. Please check the service status and try again later.`,
    };
  }
  const eventarc = new Eventarc(projectId, locationId);
  const expectedPayload = MojoFunctions.buildPayloadFromAttributes(
    resources.attributesMap,
    Eventarc.ATTRIBUTE_NAMES,
    MojoFunctions.transformEventarcValue_,
  );
  MojoFunctions.verifyEventarcTriggerIamRoles_(projectId, expectedPayload);
  return MojoFunctions.reconcileDeclarativeResource(
    'Eventarc Trigger',
    eventarc.getTrigger(triggerId),
    payload => eventarc.createOrUpdateTrigger(triggerId, payload),
    expectedPayload,
    eventarc.getLinkToCloudConsole(triggerId),
  );
};

MojoTemplateRegistry.register('eventarcTrigger', {
  category: 'Google Cloud',
  resource: 'Eventarc Trigger',
  value: '${namespace|hyphened}-trigger',
  attributes: [
    {
      attributeName: Eventarc.ATTRIBUTE_NAMES['destination.cloudRun.service'],
    },
    {
      attributeName: Eventarc.ATTRIBUTE_NAMES['destination.cloudRun.path'],
    },
    {
      attributeName: Eventarc.ATTRIBUTE_NAMES['eventFilters.0'],
    },
    {
      attributeName: Eventarc.ATTRIBUTE_NAMES.serviceAccount,
      attributeValue: '${serviceAccount}',
    },
    // {
    //   attributeName: Eventarc.ATTRIBUTE_NAMES['transport.pubsub.topic'],
    // },
  ],
  groupType: GROUP_TYPE.COLLAPSE,
  checkFn: MojoFunctions.checkOrUpdateEventarcTrigger,
});
