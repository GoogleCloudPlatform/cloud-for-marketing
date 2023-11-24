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

const { RestfulApiBase } = require('./base/restful_api_base.js');
const { getLogger } = require('../components/utils.js');

const API_VERSION = 'v3';
const API_ENDPOINT = 'https://api.sendgrid.com';

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
    return Object.assign({}, super.getDefaultHeaders(), {
      Authorization: `Bearer ${this.apiKey}`,
    });
  }

  /**
   * Sends an email.
   * @see https://docs.sendgrid.com/api-reference/mail-send/mail-send
   */
  async sendMail(email) {
    const response = await this.request('mail/send', 'POST', email);
    return response;
  }
}

module.exports = {
  SendGrid,
  API_VERSION,
  API_ENDPOINT,
};
