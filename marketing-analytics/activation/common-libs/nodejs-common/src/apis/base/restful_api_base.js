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
 * @fileoverview A base class for RESTful API based on gaxios.
 */

const {
  request: gaxiosRequest,
  GaxiosResponse,
  GaxiosOptions,
} = require('gaxios');

const { getLogger } = require('../../components/utils.js');

/**
 * A type for an Api response which is independent to the 'request' library.
 * @typedef {{
 *   data:string,
 *   code:number,
 * }}
 */
let Response;

/**
 * Base class for RESTful API based on gaxios.
 * @see https://github.com/googleapis/gaxios
 */
class RestfulApiBase {

  constructor(env = process.env) {
    this.env = env;
    this.logger = getLogger(this.constructor.name);
  }

  /**
   * Returns the base Url of the Api request.
   * @return {string}
   * @abstract
   */
  async getBaseUrl() { }

  /**
   * Returns default HTTP headers.
   * @return {!Headers} HTTP headers.
   */
  async getDefaultHeaders() {
    return new Headers();
  }

  /**
   * Gets an object as request options.
   * @return {Object=}
   */
  getRequesterOptions() {
    return {
      responseType: 'json',
    };
  }

  /**
   * Gets a response object. Different `request` libraries can be used in
   * different situations. This function is used to isolate the different
   * responses from different `request` libraries.
   * @param {GaxiosResponse} response
   * @return {!Response}
   */
  getResponseProcessor(response) {
    return {
      data: response.data,
      code: response.status,
    };
  }

  /**
   * Sends our a request and returns the response.
   * @param {string|undefined} uri
   * @param {string=} method
   * @param {object|undefined} data
   * @param {object|undefined} headers
   * @return {!Response}
   */
  async request(uri, method = 'GET', data, headers) {
    /** @type {GaxiosOptions} */
    const options = Object.assign(
      this.getRequesterOptions(),
      {
        url: await this.getRequestUrl(uri),
        method,
        headers: headers || await this.getDefaultHeaders(),
        data,
      }
    );
    return this.getResponseProcessor(await gaxiosRequest(options));
  }

  /**
   * Constructs the fully-qualified URL to the API using the given @param
   * {requestUri}.
   * If it is an absolute url, return it directly;
   * Otherwise append it after base url which is defined in detail API classes.
   *
   * @param {string} requestUri The URI of the specific resource to request.
   * @return {string} representing the fully-qualified API URL.
   */
  async getRequestUrl(requestUri) {
    const baseUrl = await this.getBaseUrl();
    if (requestUri) {
      if (requestUri.toLocaleLowerCase().startsWith('http')) return requestUri;
      return `${baseUrl}/${requestUri}`;
    }
    return baseUrl;
  }

  /**
   * Gets the URL encoded querystring from a parameter object.
   * If a parameter is an array, the parameter will be in the querystring for
   * multiple times.
   * @param {object} params.
   * @return {string} A querystring which starts with '?'.
   */
  getQueryString(params) {
    if (!params || Object.keys(params).length === 0) return '';
    return '?' + Object.keys(params).map((key) => {
      if (!Array.isArray(params[key]))
        return `${key}=${encodeURIComponent(params[key])}`;
      return params[key].map((v) => `${key}=${encodeURIComponent(v)}`).join('&');
    }).join('&');
  }
}

module.exports = {
  Response,
  RestfulApiBase,
};
