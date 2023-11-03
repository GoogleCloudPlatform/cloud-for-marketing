// Copyright 2019 Google Inc.
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

/** @fileoverview Cloud Pub/Sub API handler class.*/

class PubSub extends ApiBase {

  constructor(projectId) {
    super();
    this.apiUrl = 'https://pubsub.googleapis.com';
    this.version = 'v1';
    this.projectId = projectId;
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/projects/${this.projectId}`;
  }

  /**
   * Gets the configuration of a topic.
   * @see https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/get
   * @param {string} topicName
   * @return {!Topic}
   * @see https://cloud.google.com/static/pubsub/docs/reference/rest/v1/projects.topics#Topic
   */
  getTopic(topicName) {
    return this.get(`topics/${topicName}`);
  }

  /**
   * Returns whether the topic exists.
   * @param {string} topicName
   * @return {boolean}
   */
  existTopic(topicName) {
    const response = this.getTopic(topicName);
    if (response.name) return true;
    if (response.error.code === 404) return false;
    console.error(`Unknown status of Pub/sub Topic ${topicName}`, response);
    throw new Error(`Unknown status of Pub/sub Topic ${topicName}`);
  }

  /**
   * Creates the given topic with the given name.
   * @see https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/create
   * @param {string} topicName
   * @return {!Topic}
   */
  createTopic(topicName) {
    return this.mutate(`topics/${topicName}`, {}, 'PUT');
  }

  /**
   * Gets the configuration details of a subscription.
   * @see https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/get
   * @param {string} subscriptionName
   * @return {!Subscription}
   * @see https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions#Subscription
  */
  getSubscription(subscriptionName) {
    return this.get(`subscriptions/${subscriptionName}`);
  }

  /**
   * Creates a subscription to a given topic.
   * @see https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/create
   * @param {string} subscriptionName
   * @param {!Subscription} payload
   * @return {!Subscription} Here is an example:
   * {
   *   name: 'projects/PROJECT_ID/subscriptions/SUB_NAME',
   *   topic: 'projects/PROJECT_ID/topics/TOPIC_NAME',
   *   pushConfig: {},
   *   ackDeadlineSeconds: 10,
   *   messageRetentionDuration: '604800s',
   *   expirationPolicy: { ttl: '2678400s' },
   *   state: 'ACTIVE'
   * }
   */
  createSubscription(subscriptionName, payload) {
    return this.mutate(`subscriptions/${subscriptionName}`, payload, 'PUT');
  }

  /**
   * Updates an existing subscription.
   * @see https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/patch
   * @param {string} subscriptionName
   * @param {!Subscription} payload
   * @return {!Subscription}
   */
  updateSubscription(subscriptionName, payload) {
    return this.mutate(`subscriptions/${subscriptionName}`, payload, 'PATCH');
  }

  /**
   * Returns whether the subscription exists.
   * @param {string} subscriptionName
   * @return {boolean}
   */
  existSubscription(subscriptionName) {
    const response = this.getSubscription(subscriptionName);
    if (response.name) return true;
    if (response.error.code === 404) return false;
    console.error(`Unknown status of Pub/sub Subscription ${subscriptionName}`, response);
    throw new Error(`Unknown status of Pub/sub Subscription ${subscriptionName}`);
  }

  /**
   * Creates or updates a subscription. If the the topic doesn't exist, it will
   * create the topic first.
   * @param {string} topicName
   * @param {string} subscriptionName
   * @param {!Subscription} options
   * @return {!Subscription}
   */
  createOrUpdateSubscription(topicName, subscriptionName, options = {}) {
    if (!this.existTopic(topicName)) this.createTopic(topicName);
    let subscription;
    const payload = Object.assign(
      { topic: `projects/${this.projectId}/topics/${topicName}` }, options);
    if (this.existSubscription(subscriptionName)) {
      subscription = this.updateSubscription(subscriptionName, {
        subscription: payload,
        updateMask: Object.keys(options).join(','),
      });
    } else {
      subscription = this.createSubscription(subscriptionName, payload);
    }
    return subscription;
  }

  /**
   * Adds one or more messages to the topic.
   * @see https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.topics/publish
   * @param {string} topic
   * @param {!feedbackPubsubMessage} messages
   * @see https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
   * @return {{messageIds: !Array<string>}}
   */
  publish(topic, messages) {
    return this.mutate(`topics/${topic}:publish`, { messages });
  }

}
