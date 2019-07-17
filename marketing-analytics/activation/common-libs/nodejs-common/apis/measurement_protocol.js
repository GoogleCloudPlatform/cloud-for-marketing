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
 * @fileoverview This is Google Analytics Measurement Protocol based on nodejs
 * 'request' library.
 */

'use strict';

const request = require('request');
const {getLogger} = require('../components/utils.js');
/** Base URL for Google Analytics service. */
const BASE_URL = 'https://www.google-analytics.com';

/**
 * Measurement Protocol hits are HTTP request. Reference:
 * 1. Measurement Protocol Parameter Reference
 * https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters
 * 2. Google Analytics Collection Limits and Quotas
 * https://developers.google.com/analytics/devguides/collection/protocol/v1/limits-quotas
 * 3. Validating Hits - Measurement Protocol
 * https://developers.google.com/analytics/devguides/collection/protocol/v1/validating-hits
 * 4. Batching multiple hits in a single request
 * https://developers.google.com/analytics/devguides/collection/protocol/v1/devguide#batch
 */
class MeasurementProtocol {
  constructor() {
    this.debug = (process.env['DEBUG'] === 'true');
    this.logger = getLogger('API.MP');
    this.logger.debug(`Init ${this.constructor.name} with Debug Mode.`);
  }

  /**
   * Returns the function to send out a Measurement Protocol request with
   * multiple hits.
   * @param {!Object<string,string>} config Measurement Protocol common
   *     configuration, e.g. web property ID.
   * @return {function(!Array<string>, string): !Promise<boolean>} Function
   *     which can send a batch of hits to Measurement Protocol.
   */
  getSinglePingFn(config) {
    /**
     * Sends a batch of hits to Measurement Protocol.
     * @param {!Array<string>} lines Data for single request. It should be
     *     guaranteed that it doesn't exceed quota limitation.
     * @param {string} batchId The tag for log.
     * @return {!Promise<boolean>}
     */
    const sendRequest = (lines, batchId) => {
      const payload =
          lines
              .map((line) => {
                const record = JSON.parse(line);
                const hit = Object.assign({}, config, record);
                return Object.keys(hit)
                    .map((key) => {
                      return `${key}=${encodeURIComponent(hit[key])}`;
                    })
                    .join('&');
              })
              .join('\n');
      // In debug mode, the path is fixed to '/debug/collect'.
      const path = (this.debug) ? '/debug/collect' : '/batch';
      const requestOptions = {
        method: 'POST',
        uri: `${BASE_URL}${path}`,
        body: payload,
      };
      return new Promise((resolve, reject) => {
        request(requestOptions, (error, response, body) => {
          if (error) {
            const errorMessages = [
              `Measurement Protocol [${batchId}] failed.`,
              `Error: ${error.toString()}`,
              `response: ${body}`,
              `request body: ${data}`,
            ];
            console.error(errorMessages.join('\n'));
            reject(error);
          }
          if (response.statusCode !== 200) {
            const errorMessages = [
              `Measurement Protocol [${batchId}] didn't succeed.`,
              `Get response code: ${response.statusCode}`,
              `response: ${body}`,
            ];
            console.error(errorMessages.join('\n'));
            reject(`Status code not 200`);
          }
          this.logger.debug(`Configuration:`, config);
          this.logger.debug(`Input Data:   `, lines);
          this.logger.debug(`Batch[${batchId}] status: ${response.statusCode}`);
          this.logger.debug(body);
          // There is not enough information from the non-debug mode. So here we
          // just return true.
          resolve(true);
        });
      });
    };
    return sendRequest;
  }
}

exports.MeasurementProtocol = MeasurementProtocol;
