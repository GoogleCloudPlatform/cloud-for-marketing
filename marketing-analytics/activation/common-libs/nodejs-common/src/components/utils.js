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

const winston = require('winston');
const {inspect} = require('util');
const {LoggingWinston} = require('@google-cloud/logging-winston');
const {CloudPlatformApis} = require('../apis/cloud_platform_apis.js');

/**
 * The result of a batch of data sent to target API. The batch here means the
 * data that will be sent out in one single request.
 * Some APIs allows partial failure: it will take those correct data and
 * response with reasons for those failed ones. 'groupedFailed' uses error
 * message as the key, and tthe array of related failed lines(records) as value.
 * Some APIs upload whole file. In this case, there will be not 'numberOfLines'
 * or 'failedLines', etc.
 * @typedef {{
 *   result: boolean,
 *   numberOfLines: (number|undefined),
 *   failedLines: (Array<string>|undefined),
 *   errors: (Array<string>|undefined),
 *   groupedFailed: (Object<string,Array<string>>|undefined),
 * }}
 */
let BatchResult;

/**
 * Function which sends a batch of data. Takes two parameters:
 * {!Array<string>} Data for single request. It should be guaranteed that it
 *     doesn't exceed quota limitation.
 * {string} The tag for log.
 * @typedef {function(!Array<string>,string): !BatchResult}
 */
let SendSingleBatch;

/**
 * Returns an modified instance of 'winston' configured for Console or
 * Stackdriver logging.
 * winston's logging methods are different from traditional logging library.
 * It expects a string message and an optional JSON object as arguments. This
 * will cause following problems:
 *  1. The second string parameter will be ignored. winston takes second string
 *     either from the property named 'message' in the JSON object, or through
 *     `Splat` format transform. `Splat` is not used a lot here because
 *     JavaScript has template literals.
 *  2. Exceptions when the second object is not a plain JSON object, but a
 *     complex object. Some objects will crash winston. If `exitOnError` wasn't
 *     set as `false`, it would cause a quiet sudden death when exception
 *     happens.
 *  3. If the first parameter is not a string, the log message will be
 *     '[Object object]'.
 *
 * This modified winston instance's behavior:
 *  1. Change the second string into a JSON object {message:second_string_arg}
 *     before it is passed to winston's logging methods.
 *  2. If the second argument is an object, then use `inspect` to convert it
 *     into a string, and wrap the string into {message:inspect_string} and pass
 *     to winston.
 *  3. Use a format to `inspect` the first argument if it's not a string.
 *
 *
 *
 * @param {string} label The label of logs.
 * @param {string=} logLevel Default log level depends on the env var named
 *     'DEBUG'. If it is set as 'true', the log level is 'debug', otherwise is
 *     'info'.
 * @param {string=} isGoogleCloudEnv Whether current environment is Google Cloud
 *     which will use Stackdriver Logging.
 * @return {!winston.Logger}
 */
const getLogger = (label = '',
    logLevel = (process.env['DEBUG'] === 'true' ? 'debug' : 'info'),
    isGoogleCloudEnv = process.env['IN_GCP']) => {
  /**
   * Converts the first argument (message) into a meaningful string if it is an
   * object.
   */
  const transformNonStringMessage = winston.format((info) => {
    if (typeof info.message !== 'string') info.message = inspect(info.message);
    return info;
  });
  /** Converts level to upper case as a convention. */
  const upperCaseLevel = winston.format((info) => {
    info.level = info.level.toUpperCase();
    return info;
  });
  /**
   * Output the log messages in Console, which lack the default format/value of
   * 'level', 'timestamp' and 'label' in the message.
   */
  const formatForConsole = winston.format.printf((info) => {
    const labelStr = label ? `[${label}] ` : '';
    return `${info.level} ${info.timestamp} ${labelStr}${info.message}`;
  });

  let mainLogTransport;
  if (isGoogleCloudEnv === 'true') {
    // Running on Google Cloud, stream logs to Stackdriver.
    mainLogTransport = new LoggingWinston({prefix: label});
  } else {
    // Running locally, stream logs to stdout.
    mainLogTransport = new winston.transports.Console({
      format: winston.format.combine(
          upperCaseLevel(),
          winston.format.timestamp(),
          winston.format.colorize(),
          formatForConsole
      ),
    });
  }

  /** @type {winston.Logger} */ const logger = winston.createLogger({
    level: logLevel,
    format: transformNonStringMessage(), // shared format for transports.
    transports: [mainLogTransport],
    exitOnError: false,
  });

  /**
   * Wraps log functions with pre-treated parameters as described in this
   * function comments.
   *
   * @param {winston.LeveledLogMethod} fn
   * @param {string} level Log level.
   * @return {function(): *}
   */
  const safeLogger = function (fn, level) {
    return function () {
      if (this.isLevelEnabled(level) && arguments.length > 1) {
        const lastArgument = arguments.length - 1;
        const meta = arguments[lastArgument];
        const message = typeof meta === 'string' ? meta : inspect(meta);
        arguments[lastArgument] = {message};
      }
      return fn.apply(null, arguments);
    };
  };

  /** Wraps all the LeveledLogMethod. */
  Object.keys(winston.config.npm.levels).forEach((level) => {
    logger[level] = safeLogger(logger[level], level);
  });

  return logger;
};

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

/**
 * Merges an object of 'groupedFailed into the object 'mergedResult'
 * @param {!BatchResult} mergedResult
 * @param {!BatchResult} groupedFailed
 * @private
 */
const mergeGroupedFailed_ = (mergedResult, groupedFailed) => {
  if (groupedFailed) {
    const mergedKeys = Object.keys(mergedResult.groupedFailed);
    mergedKeys.forEach((key) => {
      mergedResult.groupedFailed[key] =
          mergedResult.groupedFailed[key].concat(groupedFailed[key]);
    });
    Object.keys(groupedFailed)
        .filter((key) => mergedKeys.indexOf(key) < 0)
        .forEach((key) => {
          mergedResult.groupedFailed[key] = groupedFailed[key];
        });
  }
}

/**
 * Merges an array of API results (BatchResult) in to a single one.
 *
 * @param {!Array<!BatchResult>} batchResults
 * @param {string} batchPrefix Prefix tag for this batch, used for logging.
 * @return {!BatchResult}
 */
const mergeBatchResults = (batchResults, batchPrefix) => {
  const logger = getLogger('SPEED_CTL');
  /** @const {!BatchResult} */ const mergedResult = {
    result: true,
    numberOfLines: 0,
    failedLines: [],
    errors: [],
    groupedFailed: {},
  };
  batchResults.forEach((batchResult, index) => {
    const batchId = `${batchPrefix}-${index}`;
    const {
      result,
      numberOfLines,
      failedLines = [],
      errors = [],
      groupedFailed,
    } = batchResult;
    if (logger.isDebugEnabled()) {
      logger.debug(
          `  Task [${batchId}] has ${numberOfLines} lines: ${result
              ? 'succeeded' : 'failed'}.`);
      if (!result) {
        logger.debug(`  Errors: ${errors.join('\n')}`);
        logger.debug(`  Failed lines: ${failedLines.join('\n')}`);
      }
    }
    mergedResult.result = mergedResult.result && result;
    mergedResult.numberOfLines += numberOfLines;
    mergedResult.failedLines = mergedResult.failedLines.concat(failedLines);
    errors.forEach((error) => {
      if (mergedResult.errors.indexOf(error) === -1) {
        mergedResult.errors.push(error);
      }
    });
    mergeGroupedFailed_(mergedResult, groupedFailed);
  });
  return mergedResult;
};

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
 * @return {!Promise<!Array<!BatchResult>>}
 * @private
 */
const sendSingleRound = async (sendingFn, sliced, recordSize, qps, roundId) => {
  const logger = getLogger('SPEED_CTL');
  const batchArray = splitArray(sliced, recordSize);
  const securedDefer = Math.ceil(batchArray.length / qps * 1000);
  logger.debug(
      `Task round[${roundId}] has ${batchArray.length} requests/batches:`);
  const deferPromise = async () => {
    await wait(securedDefer);
    logger.debug(`Task round[${roundId}] is secured for ${securedDefer} ms.`);
  };
  const batchPromises = batchArray.map(
      (batch, index) => sendingFn(batch, `${roundId}-${index}`));
  const [batchResults] = await Promise.all([
    Promise.all(batchPromises),
    deferPromise(),
  ]);
  return batchResults;
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
 * @param {function(!Array<!BatchResult>, string):!BatchResult} mergeFn
 *     The function to merge the array of result of each request.
 * @param {number} timeout The time (seconds) that this function can run. This
 *     is used to evaluate whether the given data could be processed with the
 *     given 'qps' and 'recordSize' in time.
 * @return {function(!SendSingleBatch,(string|!Array<string>),string=):
 *     !BatchResult} Speed and content managed sending function.
 */
const apiSpeedControl = (recordSize = 1, numberOfThreads = 1, qps = 1,
    mergeFn = mergeBatchResults, timeout = 540) => {
  const roundSize = recordSize * numberOfThreads;
  const maxRecords = qps * recordSize * timeout;
  /**
   * Returns a sending function with speed and content managed.
   * @param {!SendSingleBatch} sendingFn Function to send out a
   *     single request of the API.
   * @param {string|!Array<string>} data Data to be sent out. If the data is
   *     a string, it will be split into Array<string> with '\n'. At the same
   *     time, the element in Array<string> is expected to be in the format of
   *     JSON object.
   * @param {string=} taskId Task ID for log.
   * @return {!BatchResult}
   */
  return async (sendingFn, data, taskId = 'unnamed') => {
    const logger = getLogger('SPEED_CTL');
    const records = Array.isArray(data) ?
        data :
        data.split('\n').filter((line) => line.trim() !== '');
    if (maxRecords < records.length) {
      const error = `Predicted timeout: ${records.length} records with config: `
          + `${recordSize} records/request and ${qps} QPS in ${timeout} sec.`;
      logger.error(error);
      return {
        result: false,
        numberOfLines: records.length,
        errors: [error],
      };
    }
    const roundArray = splitArray(records, roundSize);
    logger.debug(
        `Task [${taskId}] has ${records.length} records in ${roundArray.length} rounds.`);
    const reduceFn = async (previous, roundData, index) => {
      const results = await previous;
      const roundId = `${taskId}-${index}`;
      /** @const {!Array<!BatchResult>} */ const roundResult =
          await sendSingleRound(sendingFn, roundData, recordSize, qps, roundId);
      /** @const {!BatchResult} */
      const mergedRoundResult = mergeFn(roundResult, roundId);
      return results.concat(mergedRoundResult);
    };
    /** @const {!Array<!BatchResult>} */
    const taskResult = await roundArray.reduce(reduceFn, []);
    /** @const {!BatchResult} */
    const mergedTaskResult = mergeBatchResults(taskResult, taskId);
    logger.debug(
        `Task [${taskId}]: ${mergedTaskResult.result ? 'succeeded' : 'failed'}.`
    );
    return mergedTaskResult;
  };
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
const getProperValue = (value, defaultValue, capped = true) => {
  if (!value || value <= 0) {
    return defaultValue;
  }
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

/**
 * Replaces a string with parameters in the pattern like `${key}`. Gets values
 * from the parameters object. Nested keys are supported.
 * @param {string} str Original string with parameters.
 * @param {!Object<string, string>} parameters
 * @param {boolean=} ignoreUnfounded Whether to ignore those properties that are
 *     not founded in the parameters . Default it throws an error if any
 *     property is not found. If set as true, it will keep parameters in
 *     original `${key}` way.
 * @return {string} Parameters replaced string.
 */
const replaceParameters = (str, parameters, ignoreUnfounded = false) => {
  const indexOfFirstPlaceholder = str.indexOf('${');
  if (indexOfFirstPlaceholder === -1) return str;
  const prefix = str.substring(0, indexOfFirstPlaceholder);
  const regex = /\${([^}]*)}/;
  const matchResult = str.match(regex);
  const splitNames = matchResult[1].split('.');
  const left = str.substring(indexOfFirstPlaceholder + matchResult[0].length);
  let value = parameters;
  for (let index in splitNames) {
    const namePiece = splitNames[index];
    if (!value || !value[namePiece]) {
      if (ignoreUnfounded) {
        value = matchResult[0];
        break;
      }
      console.error(`Fail to find property ${matchResult[1]} in parameters: `,
          parameters);
      throw new Error(`Fail to find property ${matchResult[1]} in parameter.`);
    }
    value = value[namePiece];
  }
  return prefix + value + replaceParameters(left, parameters, ignoreUnfounded);
};

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

/**
 * Returns a function to extract values of properties based on an array of
 * fields. This is a generalized version of 'getFilterFunction', and nestled
 * properties are supported in this function.
 *
 * @param{Array<string>} paths
 * @return {object}
 */
const extractObject = (paths) => {

  /**
   * The reduce function to transcribe values of a given path from the source
   * object to the target object. Nestled objects are supported as segmented
   * names in the path. Segmented names are separated with 'dot' in the path.
   * @param {object} sourceObject
   * @param {object} targetObject
   * @param {string|undefined} lastSegment A segment of the path. This is the
   *    one before 'currentSegment'. It is undefined for the initial execution.
   * @param {string} currentSegment Current segment of the path.
   * @return {[object|undefined, object, string]}
   */
  const transcribe = ([sourceObject, targetObject, lastSegment],
      currentSegment) => {
    if (lastSegment && !targetObject[lastSegment]) {
      targetObject[lastSegment] = {};
    }
    return [
      sourceObject ? sourceObject[currentSegment] : undefined,
      lastSegment ? targetObject[lastSegment] : targetObject,
      currentSegment,
    ];
  };

  return (sourceObject) => {
    const output = {};
    paths.forEach((path) => {
      const [value, owner, property] = path.split('.')
          .reduce(transcribe, [sourceObject, output, undefined]);
      if (value) {
        owner[property] = value;
      }
    });
    return output;
  };
};

/**
 * Checks whether the permissions are granted for current authentication.
 * This function will be invoked during the deployment of a specific solution,
 * e.g. Tentacles, to make sure the operator has the proper permissions to
 * carry on. If the operator doesn't have enough permissions, it will exit with
 * status code 1 to let the invoker (the Bash installation script) know that it
 * doesn't pass.
 * @param {!Array<string>} permissions Array of permissions to check.
 * @param {string} projectId The Id of Cloud project.
 * @return {!Promise<undefined>}
 */
const checkPermissions = (permissions,
    projectId = process.env['GCP_PROJECT']) => {
  const cloudPlatformApis = new CloudPlatformApis(projectId);
  return cloudPlatformApis.testIamPermissions(permissions)
      .then((grantedPermissions) => {
        console.log(grantedPermissions);
        grantedPermissions = grantedPermissions || [];
        if (grantedPermissions.length < permissions.length) {
          const missedPermissions = permissions.filter(
              (permission) => grantedPermissions.indexOf(permission) === -1);
          console.error(`[MISSED] ${missedPermissions.join(',')}.`);
          process.exit(1);
        }
      })
      .catch((error) => {
        console.error(`[ERROR] ${error.message}`);
        process.exit(1);
      });
};

/**
 * For more details, see:
 * https://developers.google.com/google-ads/api/docs/rest/design/json-mappings
 * @param {string} name Identifiers.
 * @return {string}
 */
const changeNamingFromSnakeToUpperCamel = (name) => {
  return `_${name}`.replace(/(_[a-z])/ig,
      (initial) => initial.substring(1).toUpperCase());
};

/**
 * For more details, see:
 * https://developers.google.com/google-ads/api/docs/rest/design/json-mappings
 * @param {string} name Identifiers.
 * @return {string}
 */
const changeNamingFromSnakeToLowerCamel = (name) => {
  return name.replace(/(_[a-z])/ig,
      (initial) => initial.substring(1).toUpperCase());
};

// noinspection JSUnusedAssignment
module.exports = {
  getLogger,
  wait,
  BatchResult,
  SendSingleBatch,
  mergeBatchResults,
  sendSingleRound,
  apiSpeedControl,
  splitArray,
  getProperValue,
  replaceParameters,
  getFilterFunction,
  extractObject,
  checkPermissions,
  changeNamingFromSnakeToUpperCamel,
  changeNamingFromSnakeToLowerCamel,
};
