// Copyright 2023 Google Inc.
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
 * @fileoverview SendGrip API class.
 */

'use strict';

const { EmailOptions } = require('./gmail.js');
const { RestfulApiBase } = require('./base/restful_api_base.js');
const { getLogger } = require('../components/utils.js');

const API_VERSION = 'v3';
const API_ENDPOINT = 'https://api.sendgrid.com';

/**
 * Returns a SendGrid email object based on a given RFC 2822 `mailbox` string.
 * For `mailbox` @see https://www.rfc-editor.org/rfc/rfc2822#page-15
 * @param {string} mailbox
 * @return {{name: string, email: string}} A SendGrid email object.
 */
function getSendGridEmail(mailbox) {
  if (typeof mailbox === 'string') {
    if (mailbox.indexOf('<') > -1) {
      const name = mailbox.substring(0, mailbox.indexOf('<')).trim();
      const email =
        mailbox.substring(mailbox.indexOf('<') + 1, mailbox.indexOf('>'));
      return { name, email };
    } else {
      return { email: mailbox.trim() };
    }
  }
  return mailbox;
}

/**
 * SendGrid API access class.
 * @see https://docs.sendgrid.com/api-reference/how-to-use-the-sendgrid-v3-api/authentication
 */
class SendGrid extends RestfulApiBase {

  constructor(apiKey, env = process.env) {
    super(env);
    this.apiKey = apiKey;
    this.logger = getLogger('SendGrid');
  }

  /** @override */
  getBaseUrl() {
    return `${API_ENDPOINT}/${API_VERSION}`;
  }

  /** @override */
  async getDefaultHeaders() {
    const headers = await super.getDefaultHeaders();
    headers.set('Authorization', `Bearer ${this.apiKey}`);
    return headers;
  }

  /**
   * No responseType is required for empty body response.
   * @override
   */
  getRequesterOptions() {
    return {};
  }
  /**
   * Generates an email object for Sendgrid.
   * @param {!EmailOptions} options
   * @return {object}
   * @see https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send#reques
   * @private
   */
  getEmail_(options = {}) {
    const {
      from,
      'reply-to': reply_to,
      subject,
      content,
      type = 'text/plain',
    } = options;
    const recipientObject = {};
    ['to', 'cc', 'bcc'].forEach((key) => {
      if (options[key]) {
        recipientObject[key] = [options[key]].flat().map(getSendGridEmail);
      }
    });
    const email = {
      subject,
      content: [{ type, value: content }],
      personalizations: [recipientObject],
    };
    if (from) email.from = getSendGridEmail(from);
    //@see https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send#reques
    if (reply_to) {
      if (Array.isArray(reply_to)) {
        email.reply_to_list = reply_to.map(getSendGridEmail);
      } else {
        email.reply_to = getSendGridEmail(reply_to);
      }
    }
    return email;
  }

  /**
   * Sends an email.
   * @param {!EmailOptions} options
   * @see https://docs.sendgrid.com/api-reference/mail-send/mail-send
   */
  async sendMail(options) {
    const email = this.getEmail_(options);
    const response = await this.request('mail/send', 'POST', email);
    return response;
  }
}

module.exports = {
  SendGrid,
  API_VERSION,
  API_ENDPOINT,
};
