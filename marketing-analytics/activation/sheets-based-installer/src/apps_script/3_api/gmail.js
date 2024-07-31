// Copyright 2022 Google Inc.
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

/** @fileoverview Gmail API handler class.*/

class Gmail extends ExternalApi {

  constructor(option) {
    super(option);
    this.name = 'Gmail API';
    this.api = 'gmail.googleapis.com';
    this.apiUrl = 'https://gmail.googleapis.com/gmail';
    this.version = 'v1';
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/users`;
  }

  /**
   * @see https://developers.google.com/gmail/api/reference/rest/v1/users.messages/send#authorization-scopes
   * @override
   */
  getScope() {
    return 'https://www.googleapis.com/auth/gmail.send';
  }

  /**
   * Sends a simple email (no attachment).
   * @see https://developers.google.com/gmail/api/reference/rest/v1/users.messages/send
   * @param {string} recipient
   * @param {string} subject
   * @param {string} content
   * @return {!Message}
   */
  sendSimpleEmail(recipient, subject, content) {
    const message = `To: ${recipient}
Subject: ${subject}

${content}`;
    return this.mutate('me/messages/send',
      { raw: Utilities.base64Encode(message) });
  }

  /**
   * Verifies the existence of the custom data source.
   * @see https://developers.google.com/gmail/api/reference/rest/v1/users/getProfile
   * @param {string} userId
   * @return {{
   *   "emailAddress": string,
   *   "messagesTotal": integer,
   *   "threadsTotal": integer,
   *   "historyId": string
   * }}
   */
  getProfile(userId) {
    return this.get(`${userId}/profile`);
  }

  watch(userId, topicName) {
    const payload = { topicName };
    return this.mutate(`${userId}/watch`, payload);
  }
}
