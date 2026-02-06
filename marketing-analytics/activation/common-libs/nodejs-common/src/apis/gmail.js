// Copyright 2024 Google Inc.
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

/** @fileoverview Gmail API Client Library. */

'use strict';

const { GoogleApiClient } = require('./base/google_api_client.js');

const API_SCOPES =
  Object.freeze(['https://www.googleapis.com/auth/gmail.send']);
const API_VERSION = 'v1';

/**
 * @typedef {{
 *     from: string|undefined,
 *     to: string|Array<string>,
 *     cc: string|Array<string>|undefined,
 *     bcc: string|Array<string>|undefined,
 *     'reply-to': string|Array<string>|undefined,
 *     subject: string,
 *     content: string,
 *     type: 'text/plain'|'text/html'|undefined,
 * }}
 */
let EmailOptions;

/**
 * Email sending class based on Gmail API.
 */
class Gmail extends GoogleApiClient {

  constructor(env = process.env) {
    super(env);
    this.googleApi = 'gmail';
  }

  /** @override */
  getScope() {
    return API_SCOPES;
  }

  /** @override */
  getVersion() {
    return API_VERSION;
  }

  /**
   * Generates a string for an email in RFC 2822 format.
   * If 'from' is different from the OAuth token's owner account, it would be
   * ignored.
   * @param {!EmailOptions} options
   * @return {string}
   * @private
   */
  getRawEmail_(options = {}) {
    const message = [];
    ['from', 'to', 'cc', 'bcc', 'reply-to', 'subject'].forEach((key) => {
      if (options[key]) {
        const value = Array.isArray(options[key])
          ? options[key].join(',') : options[key];
        message.push(`${key}: ${value}`);
      }
    });
    if (options.type) {
      message.push(`content-type: ${options.type}`);
    }
    if (message.length === 0)
      throw new Error(`Can not get email from ${options}`);
    message.push('\n');
    message.push(options.content);
    return message.join('\n');
  }

  /**
   * Sends a simple email (no attachment).
   * @see https://developers.google.com/gmail/api/reference/rest/v1/users.messages/send
   * @param {!EmailOptions} options
   * @return {!Message}
   */
  async sendSimpleEmail(options) {
    const email = this.getRawEmail_(options);

    const gmail = await this.getApiClient();
    const message = {
      userId: 'me',
      requestBody: { raw: Buffer.from(email).toString('base64') }
    };
    try {
      const response = await gmail.users.messages.send(message);
      return { responseStatus: response.status };
    } catch (error) {
      this.logger.error('Result of sending an email', JSON.stringify(error));
      return { responseStatus: error.status };
    }
  }

}

module.exports = {
  Gmail,
  EmailOptions,
  API_SCOPES,
  API_VERSION,
};
