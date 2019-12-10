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
 * @fileoverview Contains utility functions.
 */

'use strict';

const log4js = require('log4js');
const {CloudPlatformApis} = require('../apis/cloud_platform_apis.js');

/**
 * Function which sends a batch of data. Takes two parameters:
 * {!Array<string>} Data for single request. It should be guaranteed that it
 *     doesn't exceed quota limitation.
 * {string} The tag for log.
 * @typedef {function(!Array<string>,string): !Promise<boolean>}
 */
let SendSingleBatch;

/**
 * Returns an instance of 'log4js' with the given appender. By default log4js
 * will skip all levels' logs. The instance here will check the value of
 * environment variable named 'DEBUG': if its value is 'true', the logger
 * level will be set as 'debug'.
 * @param {string} appender The Appender of the logger instance.
 * @return {!Logger}
 */
function getLogger(appender) {
  const logger = log4js.getLogger(appender);
  if ((process.env['DEBUG'] === 'true')) {
    logger.level = 'debug';
  }
  return logger;
}

exports.getLogger = getLogger;

/**
 * Splits the given array to small sized arrays based on the given split size.
 *
 * @param {!Array<T>} records Array of any type to be split.
 * @param {number} splitSize Split size.
 * @return {!Array<!Array<T>>} Array of 'any type array' whose size is no
 *     greater than the given split size.
 * @template T
 */
const splitArray = (records, splitSize) => {
  const results = [];
  const rounds = Math.floor((records.length - 1) / splitSize) + 1;
  for (let i = 0; i < rounds; i++) {
    results.push(records.slice(
        i * splitSize, Math.min((i + 1) * splitSize, records.length)));
  }
  return results;
};

exports.splitArray = splitArray;

/**
 * Sends a round of data in multiple batches (requests). Number of records in
 * every batch is defined by 'recordSize'. All these requests will be send out
 * simultaneously. A safety waiting time based on qps also affects even if all
 * these requests finished earlier.
 *
 * @param {!SendSingleBatch} sendingFn Function to send out a single request of
 *     the API.
 * @param {!Array<string>} sliced Data for a round.
 * @param {number} recordSize The number of records (hits, conversions) for a
 *     single request.
 * @param {number} qps Queries per second.
 * @param {string} roundId Round ID for log.
 * @return {!Promise<boolean>}
 * @private
 */
const sendSingleRound = (sendingFn, sliced, recordSize, qps, roundId) => {
  const logger = getLogger('SPEED_CTL');
  const batchArray = splitArray(sliced, recordSize);
  const securedDefer = Math.ceil(batchArray.length / qps * 1000);
  logger.debug(`Task round[${roundId}] has ${
      batchArray.length} requests/batches. Start:`);
  const deferPromise = wait(securedDefer).then(() => {
    logger.debug(`Task round[${roundId}] is secured for ${securedDefer} ms.`);
  });
  const batchPromises = [deferPromise];
  batchArray.forEach((batch, index) => {
    const batchId = `${roundId}-${index}`;
    batchPromises.push(sendingFn(batch, batchId).then((batchResult) => {
      logger.debug(`Task batch[${batchId}] has ${batch.length} records: ${
          batchResult ? 'succeeded' : 'failed'}.`);
      return batchResult;
    }));
  });
  return Promise.all(batchPromises).then((results) => {
    const roundResult = !results.includes(false);
    logger.debug(
        `Task round[${roundId}] ${roundResult ? 'succeeded' : 'failed'}.`);
    if (!roundResult) {
      console.log(
          `Task round[${roundId}] has batch(es) failed batches:`, results);
    }
    return roundResult;
  });
};

/**
 * Some APIs will have the limitations for:
 * 1. The number of conversions for each request;
 * 2. Queries per second (QPS).
 * Splits the input data into proper pieces for each request and sending out
 * requests in a QPS aligned speed.
 * @param {number=} recordSize The number of records (hits, conversions) for a
 *     single request.
 * @param {number=} numberOfThreads The number of requests will be fired
 *     simultaneously.
 * @param {number=} qps Queries per second.
 * @return {function(!SendSingleBatch,(string|!Array<string>),string=):
 *     !Promise<boolean>} Speed and content managed sending function.
 */
exports.apiSpeedControl = (recordSize = 1, numberOfThreads = 1, qps = 1) => {
  const roundSize = recordSize * numberOfThreads;
  /**
   * Returns a sending function with speed and content managed.
   * @param {!SendSingleBatch} sendingFn Function to send out a
   *     single request of the API.
   * @param {string|!Array<string>} data Data to be sent out. If the data is
   *     a string, it will be split into Array<string> with '\n'. At the same
   *     time, the element in Array<string> is expected to be in the format of
   *     JSON object.
   * @param {string=} taskId Task ID for log.
   * @return {!Promise<boolean>}
   */
  const sendWithSpeedControl = (sendingFn, data, taskId = 'unnamed') => {
    const records = Array.isArray(data) ?
        data :
        data.split('\n').filter((line) => line.trim() !== '');
    const roundArray = splitArray(records, roundSize);
    console.log(`Task[${taskId}] has ${records.length} records in ${
        roundArray.length} rounds.`);
    let roundPromise = Promise.resolve(true);
    roundArray.forEach((round, index) => {
      const roundId = `${taskId}-${index}`;
      roundPromise = roundPromise.then((previousResult) => {
        return sendSingleRound(sendingFn, round, recordSize, qps, roundId)
            .then(roundResult => previousResult && roundResult);
      });
    });
    return roundPromise.then((taskResult) => {
      console.log(`Task[${taskId}] ${taskResult ? 'succeeded' : 'failed'}.`);
      return taskResult;
    });
  };
  return sendWithSpeedControl;
};

/**
 * Returns a 'proper value' based on the setting and default value.
 * Three parameters are used to control the speed of sending requests out for
 * some APIs. They are:
 * 1. the number of records per request;
 * 2. QPS
 * 3. the number of requests will be fired at the same time.
 * Some of these have the restriction from API specification. This function
 * offers default values in case of users' setting is missing or out the range
 * of APIs' requirement.
 * See function 'apiSpeedControl'.
 *
 * @param {number|undefined} value The value in configuration.
 * @param {number} defaultValue Default value. It's usually the maximum value
 *     that one API allows.
 * @param {boolean=} capped Whether or not the setting value should be no larger
 *     than the default value.
 * @return {number} The proper value of the property.
 */
exports.getProperValue = (value, defaultValue, capped = true) => {
  if (!value || value <= 0) return defaultValue;
  return capped ? Math.min(value, defaultValue) : value;
};

/**
 * Waits a given time before return the given object.
 * @param {number} time Time to wait in milliseconds.
 * @param {?Object=} value Any value that will be returned after the waiting
 *     time.
 * @return {!Promise<!Object>}
 */
const wait = (time, value = '') => {
  let timeoutId;
  const promise = new Promise((resolve) => {
    timeoutId = setTimeout(resolve, time);
  });
  return promise.then(() => {
    clearTimeout(timeoutId);
    timeoutId = null;
    return Promise.resolve(value);
  });
};

exports.wait = wait;

/**
 * Replaces a string with parameters in the pattern like `${key}`. Gets values
 * from the parameters object. Nested keys are supported.
 * @param {string} str Original string with parameters.
 * @param {!Object<string, string>} parameters
 * @return {string} Parameters replaced string.
 */
const replaceParameters = (str, parameters) => {
  if (str.indexOf('${') === -1) return str;
  const regex = /\${([^}]*)}/;
  const matchResult = str.match(regex);
  const splitNames = matchResult[1].split('.');
  let value = parameters;
  splitNames.forEach((namePiece) => {
    if (!value) {
      console.error(`Fail to find property ${matchResult[1]} in parameters: `,
          parameters);
      throw new Error(`Fail to find property ${matchResult[1]} in parameter.`);
    }
    value = value[namePiece];
  });
  const newStr = str.replace(matchResult[0], value);
  return replaceParameters(newStr, parameters);
};
exports.replaceParameters = replaceParameters;

/**
 * Gets a function that will pick existent properties from the given object.
 * @param {Array<string>} properties
 */
const getFilterFunction = (properties) => {
  return ((obj) => {
    const result = {};
    properties.forEach((property) => {
      if (typeof obj[property] !== 'undefined') {
        result[property] = obj[property];
      }
    });
    return result;
  });
};
exports.getFilterFunction = getFilterFunction;

/**
 * Checks whether the permissions are granted for current authentication.
 * This function will be invoked during the deployment of a specific solution,
 * e.g. Tentacles, to make sure the operator has the proper permissions to
 * carry on. If the operator doesn't have enough permissions, it will exit with
 * status code 1 to let the invoker (the Bash installation script) know that it
 * doesn't pass.
 * @param {!Array<string>} permissions Array of permissions to check.
 * @return {!Promise<undefined>}
 */
const checkPermissions = (permissions) => {
  const cloudPlatformApis = new CloudPlatformApis();
  return cloudPlatformApis.testIamPermissions(permissions)
      .then((grantedPermissions) => {
        grantedPermissions = grantedPermissions || [];
        if (grantedPermissions.length < permissions.length) {
          const missedPermissions = permissions.filter(
              (permission) => grantedPermissions.indexOf(permission) === -1);
          console.error(`[MISSED] ${missedPermissions.join(',')}.`);
          process.exit(1);
        }
      })
      .catch((error) => {
        console.error(`[ERROR] ${error.message}.`);
        process.exit(1);
      });
};

exports.checkPermissions = checkPermissions;
