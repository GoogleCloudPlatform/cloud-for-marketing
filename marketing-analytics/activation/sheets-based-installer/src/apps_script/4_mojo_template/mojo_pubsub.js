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
 * Combined one-step check and idempotent update/create for Pub/Sub Topics.
 * Verifies current topic existence, and creates it immediately if needed.
 * @param {string} topicId The identifier or full path for the Pub/Sub Topic.
 * @param {!Object=} resources The resources object containing attributesMap.
 * @return {!CheckResult} Standardized status, descriptive outcome message, and
 *     console link.
 */
MojoFunctions.checkOrUpdatePubsubTopic = (topicId, resources = {}) => {
  const projectId = getDocumentProperty('projectId');
  const pubsub = new PubSub(projectId);
  const cleanTopicId = String(topicId)
    .replace(/^projects\/[^/]+\/topics\//, '')
    .trim();
  const statusLink = pubsub.getLinkToCloudConsole(cleanTopicId);

  const response = pubsub.getTopic(cleanTopicId);
  if (response && response.name) {
    return {
      status: RESOURCE_STATUS.OK,
      message: `Pub/Sub Topic [${cleanTopicId}] configuration verified up-to-date.`,
      status_link: statusLink,
    };
  }

  if (response && response.error && response.error.code === 404) {
    console.log(`Pub/Sub Topic [${cleanTopicId}] not found, creating...`);
    const createRes = pubsub.createTopic(cleanTopicId);
    if (createRes && createRes.error) {
      return {
        status: RESOURCE_STATUS.ERROR,
        message: `Failed to create Pub/Sub Topic [${cleanTopicId}]: ${
          createRes.error.message
        }`,
        status_link: statusLink,
      };
    }
    return {
      status: RESOURCE_STATUS.OK,
      message: `Pub/Sub Topic [${cleanTopicId}] created successfully.`,
      status_link: statusLink,
    };
  }

  const errMsg =
    (response && response.error && response.error.message) ||
    'Unknown error occurred while checking Pub/Sub Topic.';
  return {
    status: RESOURCE_STATUS.ERROR,
    message: errMsg,
    status_link: statusLink,
  };
};

MojoTemplateRegistry.register('pubsubTopic', {
  category: 'Google Cloud',
  resource: 'Pub/Sub Topic',
  value: '${namespace}-topic',
  propertyName: 'pubsubTopic',
  checkFn: MojoFunctions.checkOrUpdatePubsubTopic,
});
