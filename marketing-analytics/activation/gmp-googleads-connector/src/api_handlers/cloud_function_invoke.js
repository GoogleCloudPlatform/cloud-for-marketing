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
 * @fileoverview Tentacles API handler for SFTP upload.
 */

'use strict';

const lodash = require('lodash');
const {
  cloudfunctions: { getIdTokenForFunction },
  storage: { StorageFile },
  utils: { getProperValue, BatchResult, requestWithRetry },
} = require('@google-cloud/nodejs-common');
const { ApiHandler } = require('./api_handler.js');

/**
 * One message per request.
 */
const RECORDS_PER_REQUEST = 1;
/**
 * Queries per second.
 */
const QUERIES_PER_SECOND = 1;
const NUMBER_OF_THREADS = 1;

/** Retry times if error happens in invoking Cloud Function. */
const RETRY_TIMES = 3;

/**
 * A Cloud Function source. The Cloud Functions here are HTTP Cloud Functions
 * deployed by solution Pupa for Python functions defined in a Colab.
 * The request has a fixed data structure including properties:
 *   `functionName`, the name of Python function that will be invoked if there
 *      are more than one functions;
 *   `args`, an array of arguments for the target Python function;
 *   `vars`, an object of gloabl variables for the Python function.
 * @typedef {{
 *   url:string,
 *   functionName:string|undefined,
 *   args:Array<Object>|undefined,
 *   vars:Object|undefined,
 * }}
 */
let CloudFunctionEndpoint;

/**
 * Configuration for a Cloud Function invoker.
 *
 * @typedef {{
 *   service:!CloudFunctionEndpoint,
 *   output:undefined,
 *   recordsPerRequest:(number|undefined),
 *   qps:(number|undefined),
 *   numberOfThreads:(number|undefined),
 * }}
 */
let CloudFunctionInvokeConfig;

/**
 * Inovke a Cloud Function and output the result into the target GCS file(s).
 */
class CloudFunctionInvoke extends ApiHandler {

  /** @override */
  getSpeedOptions(config) {
    const recordsPerRequest =
      getProperValue(config.recordsPerRequest, RECORDS_PER_REQUEST, false);
    const numberOfThreads =
      getProperValue(config.numberOfThreads, NUMBER_OF_THREADS, false);
    const qps = getProperValue(config.qps, QUERIES_PER_SECOND, false);
    return { recordsPerRequest, numberOfThreads, qps };
  }

  /**
   * Returns whether the arguements need to be merged.
   * By default, each line (of input data) contains the arguments for one
   * invocation to the target service. However, there are some services that can
   * support mulitple lines for better performance. To leverage this kind of
   * services, multiple lines will be merged into one before the invocation.
   * @see getMergedData_ for the details of how data is merged.
   *
   * @param {object} config
   * @return {boolean}
   * @private
   */
  needMergeArgs_(config) {
    const { recordsPerRequest } = this.getSpeedOptions(config);
    return recordsPerRequest > 1;
  }

  /**
   * Returns the data object to be sent to the target Cloud Function. It will
   * merge the `args` if required. For example, the `recordsPerRequest` is more
   * than 1 and the input lines are:
   *    {"args":["a1","b1"]}
   *    {"args":["a2","b2"]}
   * Then the `args` will be merged to `{"args":[["a1","a2"],["b1","b2"]]}`.
   *
   * The `vars` will not be merged and only use the one in the first element.
   *
   * Note, the parameters of target service (suppose it is `fn(a, b)`), both
   * `a` and `b` are Array. To support single line invocation (e.g. for test), a
   * wrapper function can be added into the Python code in Colab something like:
   * `singleFn = (a,b) => fn([a],[b])`
   *
   * @param {*} needMergeArg
   * @param {*} records
   * @return {{
   *   args:Array<Object>|undefined,
   *   vars:Object|undefined,
   * }}
   */
  getMergedData_(needMergeArg, records) {
    const data = !needMergeArg ? records[0] : {
      args: records[0].args.map(
        (_, index) => records.map(({ args }) => args[index])
      ),
      vars: records[0].vars,
    };
    return data;
  }

  /**
  * Uploads a file to a SFTP server. One use case is to upload business data in
  * Search Ads 360.
  * @param {string} records Lines of JSON string which will be sent to the
  *   target Cloud Function as payload.
  * @param {string} messageId Pub/sub message ID as log tag.
  * @param {!CloudFunctionInvokeConfig} config
  * @return {!BatchResult}
  */
  async sendData(records, messageId, config) {
    this.logger.debug(`Init CF invoker with Debug Mode.`);
    const managedSend = this.getManagedSendFn(config);
    const { service: { url, args, vars, functionName } } = config;
    try {
      const token = await getIdTokenForFunction(url);
      const needMergeArgs = this.needMergeArgs_(config);
      const results = [];
      const getSingleInvokeFn = async (lines, batchId) => {
        const records = lines.map((line) => JSON.parse(line));
        /** @const {!BatchResult} */ const batchResult = {
          numberOfLines: records.length,
        };
        const source = this.getMergedData_(needMergeArgs, records);
        const options = {
          url,
          headers: { Authorization: `bearer ${token}` },
        }
        options.data = lodash.merge({ args, vars, functionName }, source);
        try {
          const responseData = await requestWithRetry(options, this.logger);
          if (!needMergeArgs) {
            results.push(lodash.merge(source, responseData));
          } else {
            responseData.result.forEach((result, index) => {
              results.push(lodash.merge(records[index], result));
            });
          }
          batchResult.result = true;
          return batchResult;
        } catch (error) {
          this.logger.error(`CF invoke[${batchId}] failed: ${lines[0]}`, error);
          batchResult.result = false;
          batchResult.errors = [error.message];
          batchResult.failedLines = lines;
          return batchResult;
        }
      };
      const batchResult =
        await managedSend(getSingleInvokeFn, records, messageId);
      const { bucket, folder, projectId } = config.output;
      const storageFile =
        StorageFile.getInstance(bucket, folder + messageId, { projectId });
      await storageFile.getFile().save(
        results.map((result) => JSON.stringify(result)).join('\n'));
      return batchResult;
    } catch (error) {
      this.logger.error(`CF invoke[${messageId}] failed: `, error);
      const batchResult = {
        result: false,
        numberOfLines: records.split('\n').length,
        errors: [error.message],
      };
      return batchResult;
    }
  }
}

/** API name in the incoming file name. */
CloudFunctionInvoke.code = 'CF';

/** Data for this API will be transferred through GCS by default. */
CloudFunctionInvoke.defaultOnGcs = false;

module.exports = {
  CloudFunctionEndpoint,
  CloudFunctionInvokeConfig,
  CloudFunctionInvoke,
};
