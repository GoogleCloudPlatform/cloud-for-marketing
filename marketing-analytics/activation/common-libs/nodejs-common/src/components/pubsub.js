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

/**
 * @fileoverview Pubsub Utilities class, including: get (create for new) a
 * topic/subscription, publish a message.
 */

'use strict';

const {
  PubSub, Message, Topic, Subscription, ClientConfig, CreateSubscriptionOptions,
} = require('@google-cloud/pubsub');

/**
 * Pub/Sub enhanced class. Main usages:
 * 1. gets or creates a topic.
 * 2. gets or creates a subscription.
 * 3. publishes a message after confirms the topic exists.
 */
class EnhancedPubSub {
  /**
   * Initializes PubSubUtils.
   * @param {(ClientConfig|undefined)=} options ClientConfig object. see:
   *     https://googleapis.dev/nodejs/pubsub/latest/global.html#ClientConfig
   */
  constructor(options = undefined) {
    /** @type {!PubSub} */
    this.pubsub = new PubSub(options);
  }

  /**
   * Returns the Pub/sub Topic based on the given topic name. If it doesn't
   * exist, creates the topic and returns it.
   * @param {string} topicName Topic name.
   * @return {!Promise<!Topic>} The topic.
   */
  async getOrCreateTopic(topicName) {
    const topic = this.pubsub.topic(topicName);
    const [topicExists] = await topic.exists();
    let newOrExistingTopic;
    if (topicExists) {
      [newOrExistingTopic] = await topic.get();
    } else {
      console.log(`Topic[${topicName}] doesn't exist. Create now.`);
      [newOrExistingTopic] = await this.pubsub.createTopic(topicName);
    }
    return newOrExistingTopic;
  };

  /**
   * Returns the Pub/sub Subscription based on the given topic name and
   * subscription name. If it doesn't exist, creates the subscription and returns
   * it.
   * @param {string} topicName Topic name.
   * @param {string} subscriptionName Subscription Name.
   * @param {CreateSubscriptionOptions?} options Configuration object.
   * @return {!Promise<!Subscription>} The subscription.
   */
  async getOrCreateSubscription(topicName, subscriptionName, options) {
    const subscription = this.pubsub.topic(topicName).subscription(
        subscriptionName);
    const [subscriptionExists] = await subscription.exists();
    let newOrExistingSubscription;
    if (subscriptionExists) {
      [newOrExistingSubscription] = await subscription.get();
    } else {
      console.log(`Sub[${subscriptionName}] doesn't exist. Create now.`);
      const topic = await this.getOrCreateTopic(topicName);
      [newOrExistingSubscription] = await topic.createSubscription(
          subscriptionName, options);
    }
    return newOrExistingSubscription;
  };

  /**
   * Publishes a message (Buffer) to the topic.
   * Explanation of the message encoding and decoding process:
   * Take the input string `message` as 'hello world'
   * 1. Before send out the `message` as string, invoke Buffer.from(message) to
   * get an array of byte(`bytes`) which is the `message` decoded in 'utf8'.
   * 2. Pub/Sub takes and passes the byte[] in a 'base64' encoded string. (Under
   * the hood of Pub/Sub, there is `bytes.toString('base64')`. The delivered
   * string is: 'aGVsbG8gd29ybGQ='. Note: the length is 16 now.
   * 3. To get the original message back: `Buffer.from(data,
   * 'base64').toString()`. It will returns the string 'hello world'.
   *
   * @param {string|Topic} topic The Pub/Sub topic name or object.
   * @param {string} message The message string.
   * @param {?Object<string,string>=} attributes The attributes of the
   *     message.
   * @return {!Promise<string>} Message ID.
   */
  async publish(topic, message, attributes = {}) {
    const confirmedTopic = typeof topic === 'string'
        ? await this.getOrCreateTopic(topic) : topic;
    const name = confirmedTopic.name;
    const bytes = Buffer.from(message);
    try {
      const messageId = await confirmedTopic.publish(bytes, attributes);
      console.log(
          `[Pubsub] Send message[${messageId}] to [${name}] with ${bytes.length} bytes.`);
      return messageId;
    } catch (error) {
      console.error(`Fail to publish to topic[${confirmedTopic}]', ${message}`);
      console.error('ERROR:', error);
    }
  };

  /**
   * Returns a new instance of this class. Using this function to replace
   * constructor to be more friendly to unit tests.
   * @param {(ClientConfig|undefined)=} options ClientConfig object. see:
   *     https://googleapis.dev/nodejs/pubsub/latest/global.html#ClientConfig
   * @return {!EnhancedPubSub}
   * @static
   */
  static getInstance(options) {
    return new EnhancedPubSub(options);
  };
}

/**
 * Returns the string of message. Message data is transferred as base64 codes.
 * @param {!Message} message Pub/sub message.
 * @return {string} The decoded message.
 */
const getMessage = (message) => {
  return Buffer.from(message.data, 'base64').toString();
};

module.exports = {
  EnhancedPubSub: EnhancedPubSub,
  getMessage,
};
