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
 * @fileoverview Base class for API handlers.
 */

'use strict';

const { utils: { apiSpeedControl, BatchResult, getLogger } } =
  require('@google-cloud/nodejs-common');

/**
 * Configurations to control the API pacing speed.
 * Some configurations have a hard limit which can't be larager than
 * a default value (required by the speicic API).
 * `recordsPerRequest`, the number of records for each request.
 * `qps`, queries per second, the number of requests n a second of time.
 * `numberOfThreads`, the number of concurrent threads to send requests.
 * @typedef {{
 *   recordsPerRequest:number,
 *   qps:number,
 *   numberOfThreads:number,
 * }}
 */
let SpeedOptions;

/**
 * Definition of API handler function. It takes three parameters:
 * {string} Data to send out.
 * {string} Pub/sub message ID for log.
 * {!ApiConfigItem} API configuration.
 * @typedef {function(string,string,!ApiConfigItem):!BatchResult}
 */
let ApiHandlerFunction;

/**
 * Base class for API handlers.
 * @abstract
 */
class ApiHandler {

  constructor() {
    this.logger = getLogger(`API.${this.constructor.code || 'unknown'}`);
  }

  /**
   * Returns the configurations of speed control for this API.
   * Note: Some configurations have a hard limit which can't be over laraged than
   * a default value (required by the speicic API).
   * @param {object} config
   * @return {!SpeedOptions}
   * @abstract
  */
  getSpeedOptions(config) {
    throw new Error(
      `Unimplemented API handler function: ${this.constructor.name}`);
  }

  /**
   * Sends out the data to target system.
   * @type {!ApiHandlerFunction}
   * @abstract
   */
  sendData(records, messageId, config) {
    throw new Error(
      `Unimplemented API handler function: ${this.constructor.name}`);
  }

  /**
   * Gets option object to create a new API object.
   * By default, the API classes will figure out the authorization from env
   * variables. The authorization can also be set in the 'config' so each
   * integration can have its own authorization. This function is used to get the
   * authorization related information from the 'config' object and form an
   * 'option' object for the API classes.
   *
   * @param {{secretName:(string|undefined)}} config The connector configuration.
   * @return {{SECRET_NAME:(string|undefined)}}
   */
  getOption(config) {
    const options = {};
    if (config.secretName) options.SECRET_NAME = config.secretName;
    return options;
  }

  /**
   * Gets the boolean value of 'debug' in config.
   * When 'debug' is set with a non-boolean value, it returns ture only if it is
   * a string 'true'.
   * @param {(boolean|string|undefined)} debug The 'debug' value in 'config'.
   * @return {boolean}
   */
  getDebug(debug) {
    return typeof debug === 'boolean' ? debug : debug === 'true';
  }


  /**
   * Gets a function to send out a batch of data with speed managed.
   * @param {object} config Api config.
   * @return {function(!SendSingleBatch,(string|!Array<string>),string=):
   *     !BatchResult}
   */
  getManagedSendFn(config) {
    const { recordsPerRequest, numberOfThreads, qps } =
      this.getSpeedOptions(config);
    return apiSpeedControl(recordsPerRequest, numberOfThreads, qps);
  }

}

module.exports = {
  ApiHandler,
  ApiHandlerFunction,
};
