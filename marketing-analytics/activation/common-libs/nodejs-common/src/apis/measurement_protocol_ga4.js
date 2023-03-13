// Copyright 2021 Google Inc.
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
 * @fileoverview This is Measurement Protocol (Google Analytics 4) based on
 * 'gaxios' library.
 */

'use strict';

const {request} = require('gaxios');
const lodash = require('lodash');
const {
  getLogger,
  SendSingleBatch,
  BatchResult,
  wait,
} = require('../components/utils.js');
/** Base URL for Google Analytics service. */
const BASE_URL = 'https://www.google-analytics.com';

/** @const{number} Times to retry when server responds an error. */
const RETRY_TIMES = 2;

/**
 * Configuration for Measurement Protocol GA4.
 * @see https://developers.google.com/analytics/devguides/collection/protocol/ga4/reference#payload_post_body
 *
 * @typedef {{
 *   queryString:{
 *     api_secret:string,
 *     measurement_id:string,
 *   }|{
 *     api_secret:string,
 *     firebase_app_id:string,
 *   },
 *   requestBody:{
 *     non_personalized_ads:boolean,
 *     events: Array<{
 *       name:string,
 *       params:Array<object>
 *     }>
 *   },
 * }}
 */
let MeasurementProtocolGA4Config;

/**
 * Measurement Protocol GA4 Reference:
 * 1. Measurement Protocol Parameter Reference
 * https://developers.google.com/analytics/devguides/collection/protocol/ga4/reference#payload_post_body
 * 2. Validating Hits - Measurement Protocol (GA4)
 * https://developers.google.com/analytics/devguides/collection/protocol/ga4/validating-events
 * 3. GA4 Event Builder
 * https://ga-dev-tools.appspot.com/ga4-event-builder/
 * 4. Limitations
 * https://developers.google.com/analytics/devguides/collection/protocol/ga4/sending-events#limitations
 */
class MeasurementProtocolGA4 {
  /**
   * Measurement Protocol GA4 has a debug endpoint which can validate the event.
   * By given this initial parameter with 'true' value, this class can send hits
   * to the debug endpoint.
   * @param {boolean=} debugMode
   */
  constructor(debugMode = false) {
    this.debugMode = debugMode;
    this.path = ((this.debugMode) ? '/debug' : '') + '/mp/collect';
    this.logger = getLogger('API.MP_GA4');
    this.logger.debug(`Init ${this.constructor.name} with Debug Mode.`);
  }

  /**
   * Returns the function to send out a Measurement Protocol GA4 request.
   * @param {!MeasurementProtocolGA4Config} config Measurement Protocol
   *     configuration, e.g. api_secret.
   * @return {!SendSingleBatch} Function which can send a batch of hits to
   *     Measurement Protocol.
   */
  getSinglePingFn(config) {
    /**
     * Sends a hit to Measurement Protocol GA4.
     * @param {!Array<string>} lines Data for single request. It should be
     *     guaranteed that it doesn't exceed quota limitation.
     * @param {string} batchId The tag for log.
     * @return {!BatchResult}
     */
    return async (lines, batchId) => {
      const line = lines[0]; // Each request contains one record only.
      if (lines.length > 1) {
        this.logger.warn(
            "Only one line data expected. Will only send the first line.");
      }
      const hit = lodash.merge({}, config.requestBody, JSON.parse(line));

      const requestOptions = {
        method: 'POST',
        url: `${BASE_URL}${this.path}`,
        params: config.queryString,
        paramsSerializer: (params) => {
          return Object.keys(params)
              .map((param) => `${param}=${params[param]}`)
              .join('&');
        },
        validateStatus: () => true,
        body: JSON.stringify(hit),
        headers: { 'User-Agent': 'Tentacles/MeasurementProtocol-GA4' },
      };
      /** @type {BatchResult} */ const batchResult = {
        numberOfLines: lines.length,
      };
      let retriedTimes = 0;
      let response;
      while (retriedTimes <= RETRY_TIMES) {
        try {
          response = await request(requestOptions);
          if (response.status < 500) break; // Only retry when server errors.
          this.logger.warn('Got a 5XX error', response);
        } catch (error) {
          this.logger.warn('Got an error', error);
          if (retriedTimes === RETRY_TIMES) {
            this.logger.error('Maximum retry times exceeded.');
            batchResult.result = false;
            batchResult.errors = [error.toString()];
            break;
          }
        }
        retriedTimes++;
        await wait(retriedTimes * 1000);
        this.logger.warn('Will retry now...');
      };
      if (response) {
        this.logger.debug('Configuration:', config);
        this.logger.debug('Input Data:   ', lines);
        this.logger.debug(`Batch[${batchId}] status: ${response.status}`);
        this.logger.debug(response.data);
        // There is not enough information from the non-debug mode.
        if (!this.debugMode) {
          if (response.status >= 200 && response.status < 300) {
            batchResult.result = true;
          } else {
            batchResult.result = false;
            const errorMessage =
              `MP GA4 [${batchId}] http status ${response.status}.`;
            this.logger.error(errorMessage, line);
            batchResult.errors = [errorMessage];
          }
        } else {
          if (response.data.validationMessages.length === 0) {
            batchResult.result = true;
          } else {
            batchResult.result = false;
            batchResult.errors = response.data.validationMessages.map(
              ({ description }) => description);
          }
        }
      }
      if (!batchResult.result) {
        batchResult.failedLines = [line];
        batchResult.groupedFailed = { [batchResult.errors.join()]: [line] };
      }
      return batchResult;
    };
  };

}

module.exports = {
  MeasurementProtocolGA4,
  MeasurementProtocolGA4Config,
};
