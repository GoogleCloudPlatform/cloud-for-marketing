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

/** @fileoverview Cloud Eventarc API handler class. */

class Eventarc extends GcpApiBase {
  /**
   * @param {string=} projectId
   * @param {string=} locationId
   */
  constructor(projectId, locationId) {
    super();
    this.name = 'Eventarc API';
    this.api = 'eventarc.googleapis.com';
    this.permissions = ['eventarc.triggers.create', 'eventarc.triggers.get'];
    this.adminRole = 'Eventarc Admin';
    this.runtimeRoles = ['roles/eventarc.eventReceiver'];
    this.apiUrl = `https://${this.api}`;
    this.version = 'v1';
    this.projectId = projectId;
    this.locationId = locationId;
  }

  /**
   * @override
   * @return {string}
   */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/projects/${
      this.projectId
    }/locations/${this.locationId}`;
  }

  /**
   * Returns the fully qualified trigger resource name.
   * @param {string} triggerId
   * @return {string}
   */
  getTriggerName(triggerId) {
    return `projects/${this.projectId}/locations/${this.locationId}/triggers/${
      triggerId
    }`;
  }

  /**
   * Gets an Eventarc trigger.
   * @see https://cloud.google.com/eventarc/docs/reference/rest/v1/projects.locations.triggers/get
   * @param {string} triggerId
   * @return {!Object}
   */
  getTrigger(triggerId) {
    return this.get(`triggers/${triggerId}`);
  }

  /**
   * Returns whether an Eventarc trigger with the given name exists.
   * @param {string} triggerId
   * @return {boolean}
   */
  existTrigger(triggerId) {
    const response = this.getTrigger(triggerId);
    if (response.name) return true;
    if (
      response.error &&
      (response.error.code === 404 || response.error.status === 'NOT_FOUND')
    ) {
      return false;
    }
    console.error(`Unknown status of Eventarc Trigger ${triggerId}`, response);
    throw new Error(`Unknown status of Eventarc Trigger ${triggerId}`);
  }

  /**
   * Normalizes Eventarc trigger definition payload by supplying required
   * defaults such as Cloud Run destination region and default Compute Engine
   * service account.
   * @param {string} triggerId Target trigger identifier.
   * @param {!Object} triggerPayload Raw trigger configuration.
   * @return {!Object} Normalized payload with defaults populated.
   * @private
   */
  normalizeTriggerPayload_(triggerId, triggerPayload) {
    const name = this.getTriggerName(triggerId);
    const payload = Object.assign({name}, triggerPayload);

    if (
      payload.destination &&
      payload.destination.cloudRun &&
      !payload.destination.cloudRun.region
    ) {
      payload.destination.cloudRun.region = this.locationId;
    }

    if (!payload.serviceAccount) {
      console.log(
        'No serviceAccount provided for Eventarc trigger, resolving default Compute Engine service account...',
      );
      const resourceManager = new CloudResourceManager(this.projectId);
      payload.serviceAccount =
        resourceManager.getDefaultComputeServiceAccount();
      console.log('Resolved default serviceAccount:', payload.serviceAccount);
    }

    return payload;
  }

  /**
   * Creates a new Eventarc trigger.
   * @see https://cloud.google.com/eventarc/docs/reference/rest/v1/projects.locations.triggers/create
   * @param {string} triggerId
   * @param {!Object} triggerPayload Trigger definition payload (serviceAccount,
   *     destination, eventFilters).
   * @return {!Object} Long-Running Operation resource.
   */
  createTrigger(triggerId, triggerPayload) {
    const payload = this.normalizeTriggerPayload_(triggerId, triggerPayload);
    console.log(
      'Creating Eventarc Trigger',
      triggerId,
      JSON.stringify(payload),
    );
    return this.mutate(`triggers?triggerId=${triggerId}`, payload);
  }

  /**
   * Updates an existing Eventarc trigger.
   * @see https://cloud.google.com/eventarc/docs/reference/rest/v1/projects.locations.triggers/patch
   * @param {string} triggerId
   * @param {!Object} triggerPayload Updated trigger configuration.
   * @return {!Object} Long-Running Operation resource.
   */
  updateTrigger(triggerId, triggerPayload) {
    const payload = this.normalizeTriggerPayload_(triggerId, triggerPayload);
    const maskFields = Object.keys(triggerPayload)
      .filter(key => key !== 'name')
      .join(',');
    const updateMask = encodeURIComponent(
      maskFields || 'serviceAccount,destination,eventFilters',
    );
    console.log(
      'Updating Eventarc Trigger',
      triggerId,
      JSON.stringify(payload),
    );
    return this.mutate(
      `triggers/${triggerId}?updateMask=${updateMask}`,
      payload,
      'PATCH',
    );
  }

  /**
   * Creates or updates an Eventarc trigger idempotently and polls until
   * completion. Checks existence and applies create or patch accordingly,
   * waiting for LRO success.
   * @param {string} triggerId
   * @param {!Object} triggerPayload
   * @return {!Object} Resolved Eventarc Trigger resource.
   */
  createOrUpdateTrigger(triggerId, triggerPayload) {
    let operation;
    if (this.existTrigger(triggerId)) {
      console.log('Updating existing Eventarc trigger:', triggerId);
      operation = this.updateTrigger(triggerId, triggerPayload);
    } else {
      console.log('Eventarc trigger not found, creating:', triggerId);
      operation = this.createTrigger(triggerId, triggerPayload);
    }
    return this.waitAndResolveOperation(operation, triggerId);
  }

  /**
   * Deletes an Eventarc trigger.
   * @see https://cloud.google.com/eventarc/docs/reference/rest/v1/projects.locations.triggers/delete
   * @param {string} triggerId
   * @return {!Object}
   */
  deleteTrigger(triggerId) {
    return this.mutate(`triggers/${triggerId}`, undefined, 'DELETE');
  }

  /**
   * Returns a direct link to the Eventarc Trigger in Google Cloud Console.
   * @param {string=} triggerId Optional trigger name to open directly.
   * @return {string}
   */
  getLinkToCloudConsole(triggerId) {
    return `https://console.cloud.google.com/eventarc/triggers/${
      this.locationId
    }/${triggerId}?project=${this.projectId}`;
  }
}

Eventarc.ATTRIBUTE_NAMES = Object.freeze({
  serviceAccount: 'Service Account',
  'destination.cloudRun.service': 'Destination Cloud Run Service',
  'destination.cloudRun.path': 'Destination Path',
  'destination.cloudRun.region': 'Destination Region',
  'eventFilters.0': 'Event Type',
  'eventFilters.1': 'Bucket Name',
  'transport.pubsub.topic': 'Pub/Sub Topic',
});
