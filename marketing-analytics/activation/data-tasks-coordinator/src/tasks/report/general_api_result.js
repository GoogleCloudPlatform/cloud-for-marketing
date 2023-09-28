// Copyright 2019 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this fileAccessObject except in compliance with the License.
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
 * @fileoverview Interface for DoubleClick BidManager (DV360) Reporting.
 */

'use strict';

const { api, utils: { getObjectByPath } }
  = require('@google-cloud/nodejs-common');
const { Report } = require('./base_report.js');

/**
 * General API Result class.
 * This report task presents a general Google Api stub class based on Google API
 * Client Libraries. By following naming convension, this task can extract a
 * kind of data from the given API. It will hanlde 'nextPageToken' and assemble
 * the results into one array.
 *
 * The supported API should be implemented in the nodejs-common lib with
 * following conditions:
 * 1. It has its own class(`className`) and been exported as an object
 * (`packageName`) in the `api` object of nodejs-common;
 * 2. The class offer a function named `getFunctionObject` to return the
 * instance of function object created based on Google API client library;
 * 3. If the API supports next page token, then the proper way to use the token
 * is to set it as property `pageToken` in the following request.
 * @see ApiResultConfig in './base_report.js'
 */
class GeneralApiResult extends Report {

  constructor(config, apiStub) {
    super(config);
    const { packageName, className } = this.config;
    this.apiStub = apiStub || new api[packageName][className](super.getOption());
  }

  /** @override */
  generate(parameters) {
    return Promise.resolve();
  }

  /** @override */
  isReady(parameters) {
    return Promise.resolve(true);
  }

  /** @override */
  isAsynchronous() {
    return false;
  }

  /** @override */
  async getContent(parameters) {
    const {
      functionObjectName,
      functionName,
      args,
      limit = 0,
      entityPath,
      pageTokenPath,
    } = this.config;
    const functionObject =
      await this.apiStub.getFunctionObject(functionObjectName);
    let result = [];
    let updatedArgs = args;
    let pageToken;
    do {
      const response = await functionObject[functionName](updatedArgs);
      result = result.concat(getObjectByPath(response, entityPath));
      if (pageTokenPath) {
        pageToken = getObjectByPath(response, pageTokenPath);
        if (pageToken) updatedArgs = Object.assign(args, { pageToken });
      }
    } while (pageToken && (limit === 0 || limit > result.length));
    result = limit > 0 ? result.slice(0, limit) : result;
    return result.map(JSON.stringify).join('\n');
  }
}

module.exports = { GeneralApiResult };
