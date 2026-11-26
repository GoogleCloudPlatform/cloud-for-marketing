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

/** @fileoverview Cloud Tasks API handler class. */

class CloudTasks extends ApiBase {
  /**
   * @param {string=} projectId
   * @param {string=} locationId
   */
  constructor(projectId, locationId) {
    super();
    this.name = 'Cloud Tasks API';
    this.api = 'cloudtasks.googleapis.com';
    this.permissions = ['cloudtasks.queues.create', 'cloudtasks.queues.get'];
    this.adminRole = 'Cloud Tasks Admin';
    this.runtimeRoles = ['roles/cloudtasks.enqueuer'];
    this.apiUrl = `https://${this.api}`;
    this.version = 'v2';
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
   * Returns the fully qualified queue resource name.
   * @param {string} queueId
   * @return {string}
   */
  getQueueName(queueId) {
    return `projects/${this.projectId}/locations/${this.locationId}/queues/${
      queueId
    }`;
  }

  /**
   * Gets a queue.
   * @see https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues/get
   * @param {string} queueId
   * @return {!Object}
   */
  getQueue(queueId) {
    return this.get(`queues/${queueId}`);
  }

  /**
   * Checks whether a Cloud Tasks queue exists.
   * @param {string} queueId
   * @return {boolean}
   */
  existQueue(queueId) {
    const response = this.getQueue(queueId);
    if (response.name) return true;
    if (
      response.error &&
      (response.error.code === 404 || response.error.status === 'NOT_FOUND')
    ) {
      return false;
    }
    console.error(`Unknown status of Cloud Tasks Queue ${queueId}`, response);
    throw new Error(`Unknown status of Cloud Tasks Queue ${queueId}`);
  }

  /**
   * Creates a new queue.
   * @see https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues/create
   * @param {string} queueId
   * @param {!Object=} options Optional queue configuration (rateLimits,
   *     retryConfig, etc.).
   * @return {!Object}
   */
  createQueue(queueId, options = {}) {
    const name = this.getQueueName(queueId);
    const payload = Object.assign({name}, options);
    console.log('Creating Cloud Tasks Queue', queueId, JSON.stringify(payload));
    return this.mutate('queues', payload);
  }

  /**
   * Updates an existing queue.
   * @see https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues/patch
   * @param {string} queueId
   * @param {!Object} options Queue configuration to update.
   * @return {!Object}
   */
  updateQueue(queueId, options) {
    const name = this.getQueueName(queueId);
    const payload = Object.assign({name}, options);
    const updateMask = encodeURIComponent(Object.keys(options).join(','));
    return this.mutate(
      `queues/${queueId}?updateMask=${updateMask}`,
      payload,
      'PATCH',
    );
  }

  /**
   * Creates or updates a queue idempotently.
   * @param {string} queueId
   * @param {!Object=} options
   * @return {!Object}
   */
  createOrUpdateQueue(queueId, options = {}) {
    if (this.existQueue(queueId)) {
      console.log('Updating existing Cloud Tasks Queue', queueId);
      return this.updateQueue(queueId, options);
    }
    console.log('Queue not found, creating new Cloud Tasks Queue', queueId);
    return this.createQueue(queueId, options);
  }

  /**
   * Purges all tasks from a queue.
   * @see https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues/purge
   * @param {string} queueId
   * @return {!Object}
   */
  purgeQueue(queueId) {
    return this.mutate(`queues/${queueId}:purge`);
  }

  /**
   * Pauses a queue.
   * @see https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues/pause
   * @param {string} queueId
   * @return {!Object}
   */
  pauseQueue(queueId) {
    return this.mutate(`queues/${queueId}:pause`);
  }

  /**
   * Resumes a paused queue.
   * @see https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues/resume
   * @param {string} queueId
   * @return {!Object}
   */
  resumeQueue(queueId) {
    return this.mutate(`queues/${queueId}:resume`);
  }

  /**
   * Gets a task from the specified queue.
   * @see https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/get
   * @param {string} queueId
   * @param {string} taskId
   * @return {!Object}
   */
  getTask(queueId, taskId) {
    return this.get(`queues/${queueId}/tasks/${taskId}`);
  }

  /**
   * Checks whether a specific task exists in the queue.
   * @param {string} queueId
   * @param {string} taskId
   * @return {boolean}
   */
  existTask(queueId, taskId) {
    const response = this.getTask(queueId, taskId);
    if (response.name) return true;
    if (
      response.error &&
      (response.error.code === 404 || response.error.status === 'NOT_FOUND')
    ) {
      return false;
    }
    console.error(
      `Unknown status of Cloud Task ${taskId} in queue ${queueId}`,
      response,
    );
    throw new Error(
      `Unknown status of Cloud Task ${taskId} in queue ${queueId}`,
    );
  }

  /**
   * Creates a new task in the specified queue.
   * @see https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/create
   * @param {string} queueId
   * @param {!Object} taskPayload Content payload for the task object (e.g.
   *     httpRequest, scheduleTime).
   * @return {!Object} The created Task resource.
   */
  createTask(queueId, taskPayload) {
    console.log('Creating Task in queue', queueId);
    return this.mutate(`queues/${queueId}/tasks`, {task: taskPayload});
  }

  /**
   * Forces a task to run immediately.
   * @see https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/run
   * @param {string} queueId
   * @param {string} taskId
   * @return {!Object}
   */
  runTask(queueId, taskId) {
    return this.mutate(`queues/${queueId}/tasks/${taskId}:run`);
  }

  /**
   * Deletes a task from the queue.
   * @see https://docs.cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/delete
   * @param {string} queueId
   * @param {string} taskId
   * @return {!Object}
   */
  deleteTask(queueId, taskId) {
    return this.mutate(
      `queues/${queueId}/tasks/${taskId}`,
      undefined,
      'DELETE',
    );
  }

  /**
   * Returns a direct link to the Cloud Tasks Queue in Google Cloud Console.
   * @param {string=} queueId Optional queue name to open directly.
   * @return {string}
   */
  getLinkToCloudConsole(queueId) {
    if (queueId) {
      return `https://console.cloud.google.com/cloudtasks/queue/${
        this.locationId
      }/${queueId}?project=${this.projectId}`;
    }
    return `https://console.cloud.google.com/cloudtasks?project=${
      this.projectId
    }`;
  }
}

CloudTasks.ATTRIBUTE_NAMES = Object.freeze({
  'rateLimits.maxConcurrentDispatches': 'Max Concurrent Dispatches',
  'rateLimits.maxDispatchesPerSecond': 'Max Dispatches Per Second',
  'retryConfig.maxAttempts': 'Max Attempts',
  'retryConfig.minBackoff': 'Min Backoff (s)',
  'retryConfig.maxBackoff': 'Max Backoff (s)',
  'retryConfig.maxDoublings': 'Max Doublings',
});
