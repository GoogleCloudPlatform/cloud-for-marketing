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

const {Topic, Subscription, CreateSubscriptionOptions} =
    require('@google-cloud/pubsub');
const getCloudProduct = require('./gcloud.js');
const pubsub = getCloudProduct('pubsub');

/**
 * Returns the Pub/sub Topic based on the given topic name. If it doesn't
 * exist, creates the topic and returns it.
 * @param {string} topicName Topic name.
 * @return {!Promise<!Topic>} The topic.
 */
function getOrCreateTopic(topicName) {
  const topic = pubsub.topic(topicName);
  return topic.exists()
      .then(([topicExists]) => {
        if (topicExists) return topic.get();
        console.log(`Topic[${topicName}] doesn't exist. Create now.`);
        return pubsub.createTopic(topicName);
      })
      .then(([newOrExistingTopic]) => newOrExistingTopic);
}

/**
 * Returns the Pub/sub Subscription based on the given topic name and
 * subscription name. If it doesn't exist, creates the subscription and returns
 * it.
 * @param {string} topicName Topic name.
 * @param {string} subscriptionName Subscription Name.
 * @param {?CreateSubscriptionOptions} options Configuration object.
 * @return {!Promise<!Subscription>} The subscription.
 */
function getOrCreateSubscription(topicName, subscriptionName, options) {
  return getOrCreateTopic(topicName).then((topic) => {
    const subscription = pubsub.topic(topicName).subscription(subscriptionName);
    return subscription.exists()
        .then(([subscriptionExists]) => {
          if (subscriptionExists) return subscription.get();
          console.log(`Sub[${subscriptionName}] doesn't exist. Create now.`);
          return topic.createSubscription(subscriptionName, options);
        })
        .then(([newOrExistingSubscription]) => newOrExistingSubscription);
  });
}

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
 * @param {string} topicName The Pub/Sub topic name.
 * @param {string} message The message string.
 * @param {?Object<string,string>=} attributes The attributes of the
 *     message.
 * @return {!Promise<string>} Message ID.
 */
function publish(topicName, message, attributes = {}) {
  return getOrCreateTopic(topicName).then((topic) => {
    const bytes = Buffer.from(message);
    return topic.publish(bytes, attributes)
        .then((messageId) => {
          console.log(`[Pubsub] Send message[${messageId}] to [${
              topicName}] with ${bytes.length} bytes.`);
          return messageId;
        })
        .catch((error) => {
          console.error(`Fail to publish to topic[${topicName}]', ${message}`);
          console.error('ERROR:', error);
        });
  });
}

exports.getOrCreateTopic = getOrCreateTopic;
exports.getOrCreateSubscription = getOrCreateSubscription;
exports.publish = publish;
