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

/** @fileoverview REST API base class. */

/**
 * This class contains basic methods to interact with general APIs using the
 * {@link UrlFetchApp} and {@link ScriptApp} classes.
 * @see https://developers.google.com/apps-script/reference/url-fetch/url-fetch-app?hl=en
 * The recommended way is to consume the API if it is available as 'Advanced
 * Google Service' within Apps Script's 'Resources' section. However if the
 * required API is not  available, this class is used to cover that.
 * `muteHttpExceptions` is set as true, so the fetch doesn't throw an exception
 * if the response code indicates failure, and instead returns the HTTPResponse.
 *
 * @abstract
 */
class ApiBase {
  /**
   * Registers a response interceptor function to be called after each API call.
   * The interceptor function should accept the response object and return a
   * modified response object or throw an error if needed.
   * @param {function(!Object): !Object} interceptorFn The interceptor function.
   */
  static registerInterceptor(interceptorFn) {
    ApiBase.responseInterceptors.push(interceptorFn);
  }

  /**
   * Sets the explicit token provider function.
   * @param {?function(): ?string} provider
   */
  static setExplicitTokenProvider(provider) {
    ApiBase.explicitTokenProvider = provider;
  }

  /**
   * Returns the base Url of the Api request.
   * @return {string}
   * @abstract
   */
  getBaseUrl() {}

  /**
   * Sends a HTTP GET request and returns the response.
   * @param {string} requestUri
   * @param {!Object} parameters Parameters in querystring.
   * @return {!Object} Returned JSON object.
   */
  get(requestUri, parameters) {
    const url =
      this.getRequestUrl(requestUri) + this.getQueryString(parameters);
    const params = this.getFetchAppParams();
    params.method = 'GET';
    return this.fetchAndReturnJson(url, params);
  }

  /**
   * Most APIs offer a `list` service to return a list of resources with a
   * possible property in response: nextPageToken, which is used to fetch the
   * next page of resources. This function is used to offer a general way to
   * get the complete list.
   * Note: unlike other `get` or `mutate` functions which will silently include
   * an 'error' object in the response whenever an error occurs, this function
   * will throw the error. The main reason is that this function is not basic
   * HTTP methods, but a layer above them. The caller is expecting to receive a
   * list of given objects, and there usually are following up steps, so a
   * silent error makes the processing complicated,
   *
   * @param {string} propertyName
   * @param {string} requestUri
   * @param {!Object=} parameters
   * @return {!Array<!Object>}
   */
  list(propertyName, requestUri, parameters = {}) {
    const results = [];
    do {
      const response = this.get(requestUri, parameters);
      if (response.error) {
        console.error(`Fail to list ${propertyName}`, response.error);
        throw response.error;
      }
      results.push(...(response[propertyName] || []));
      parameters.pageToken = response.nextPageToken;
    } while (parameters.pageToken);
    return results;
  }

  /**
   * Sends a HTTP request other than GET, e.g. POST or DELETE and returns the
   * response.
   * @param {string} requestUri
   * @param {(!Object|string)=} payload A JSON object as the payload.
   *     Default is undefined.
   * @param {string=} method HTTP method, default is POST.
   * @return {!Object} Returned JSON object.
   */
  mutate(requestUri, payload = undefined, method = 'POST') {
    const url = this.getRequestUrl(requestUri);
    const params = this.getFetchAppParams();
    params.method = method;
    if (typeof payload === 'object') {
      params.payload = JSON.stringify(payload);
    } else if (typeof payload === 'string') {
      params.payload = payload;
    } else if (typeof payload !== 'undefined') {
      console.warn('Ignore unknown type payload', payload);
    }
    return this.fetchAndReturnJson(url, params);
  }

  /**
   * Uses UrlFetchApp to send out the HTTP request and returns HTTPResponse
   * @see https://developers.google.com/apps-script/reference/url-fetch/http-response
   * @param {string} url Request url.
   * @param {!Object} params The optional JavaScript object specifying advanced
   *   parameters.
   *   @see https://developers.google.com/apps-script/reference/url-fetch/url-fetch-app#advanced-parameters
   * @return {!Object} Raw response.
   */
  fetchAndReturnRawResponse(url, params) {
    console.info(`UrlFetchApp ${params.method}: ${url}`);
    const response = UrlFetchApp.fetch(url, params);
    return response;
  }

  /**
   * Uses UrlFetchApp to send out the HTTP request and returns content as a JSON
   * object.
   * @param {string} url Request url.
   * @param {!Object} params The optional JavaScript object specifying advanced
   *   parameters. @see
   *   https://developers.google.com/apps-script/reference/url-fetch/url-fetch-app#advanced-parameters
   * @return {!Object} Returned JSON object.
   */
  fetchAndReturnJson(url, params) {
    const response = this.fetchAndReturnRawResponse(url, params);
    try {
      const result = JSON.parse(response.getContentText() || '{}');
      if (result.error) {
        console.log(`Get error from ${url}`, result.error);
        for (const interceptor of ApiBase.responseInterceptors) {
          const resolved = interceptor(result.error, this);
          if (resolved) {
            console.log(`Applied interceptor for ${url}, retrying...`);
            return this.fetchAndReturnJson(url, params);
          }
        }
      }
      return result;
    } catch (error) {
      console.error('Failed to get JSON from', response.getContentText());
      return {
        error: {
          code: response.getResponseCode(),
          content: response.getContentText(),
        },
      };
    }
  }

  /**
   * Constructs the fully-qualified URL to the API using the given requestUri.
   * If it is an absolute url, return it directly;
   * Otherwise append it after base url which is defined in detail API classes.
   *
   * @param {string} requestUri The URI of the specific resource to request.
   * @param {string=} baseUrl The API base url before specific resource.
   * @return {string} representing the fully-qualified API URL.
   */
  getRequestUrl(requestUri, baseUrl = this.getBaseUrl()) {
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
   * @param {!Object} params
   * @return {string} A querystring which starts with '?'.
   */
  getQueryString(params) {
    if (!params || Object.keys(params).length === 0) return '';
    return (
      '?' +
      Object.keys(params)
        .map(key => {
          if (!Array.isArray(params[key]))
            return `${key}=${encodeURIComponent(params[key])}`;
          return params[key]
            .map(v => `${key}=${encodeURIComponent(v)}`)
            .join('&');
        })
        .join('&')
    );
  }

  /**
   * Returns the default params for UrlFetchApp.
   * @see https://developers.google.com/apps-script/reference/url-fetch/url-fetch-app#fetch(String,Object)
   * @return {!Object}
   */
  getFetchAppParams() {
    return {
      contentType: 'application/json',
      muteHttpExceptions: true,
      headers: this.getDefaultHeader(),
    };
  }

  /**
   * Returns HTTP headers. By default, it contains OAuth token.
   * @return {!Object} HTTP headers.
   */
  getDefaultHeader() {
    const token = this.getAccessToken();
    return {
      Authorization: `Bearer ${token}`,
      Accept: 'application/json',
    };
  }

  /**
   * Gets the OAuth access token managed by Apps Script.
   * @return {string} access token.
   */
  getAccessToken() {
    if (ApiBase.explicitTokenProvider) {
      const token = ApiBase.explicitTokenProvider();
      if (token) return token;
    }
    return ScriptApp.getOAuthToken();
  }
}

/** @private @const {!Array<function(!Object): !Object>} */
ApiBase.responseInterceptors = [];

/** @private {?function(): ?string} */
ApiBase.explicitTokenProvider = null;
