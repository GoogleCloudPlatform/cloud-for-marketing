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
 * @link {UrlFetchApp} and @link {ScriptApp} classes.
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
   * Returns the base Url of the Api request.
   * @return {string}
   * @abstract
   */
  getBaseUrl() { }

  /**
   * Sends a HTTP GET request and returns the response.
   * @param {string} requestUri
   * @param {object} parameters Parameters in querystring.
   * @return {object} Returned JSON object.
   */
  get(requestUri, parameters) {
    const url = this.getRequestUrl(requestUri) + this.getQueryString(parameters);
    const params = this.getFetchAppParams();
    params.method = 'GET';
    return this.fetchAndReturnJson(url, params);
  }

  /**
   * Sends a HTTP request other than GET, e.g. POST or DELETE and returns the
   * response.
   * @param {string} requestUri
   * @param {(object|string|undefined)=} payload A JSON object as the payload.
   *     Default is undefined.
   * @param {string=} method HTTP method, default is POST.
   * @return {object} Returned JSON object.
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
   * @param {object} params The optional JavaScript object specifying advanced
   *   parameters.
   *   @see https://developers.google.com/apps-script/reference/url-fetch/url-fetch-app#advanced-parameters
   * @return {object} Raw response.
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
   * @param {object} params The optional JavaScript object specifying advanced
   *   parameters. @see
   *   https://developers.google.com/apps-script/reference/url-fetch/url-fetch-app#advanced-parameters
   * @return {object} Returned JSON object.
   */
  fetchAndReturnJson(url, params) {
    const response = this.fetchAndReturnRawResponse(url, params);
    try {
      const result = JSON.parse(response.getContentText() || '{}');
      return result;
    } catch (error) {
      console.error('Failed to get JSON from', response.getContentText());
      return {
        error: {
          code: response.getResponseCode(),
          content: response.getContentText(),
        }
      };
    }
  }

  /**
   * Constructs the fully-qualified URL to the API using the given @param
   * {requestUri}.
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

  /**
   * Returns the default params for UrlFetchApp.
   * @see https://developers.google.com/apps-script/reference/url-fetch/url-fetch-app#fetch(String,Object)
   * @return {object}
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
   * @return {object} HTTP headers.
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
    if (typeof EXPLICIT_AUTH !== 'undefined' && EXPLICIT_AUTH.enabled()) {
      return EXPLICIT_AUTH.getAccessToken();
    }
    return ScriptApp.getOAuthToken();
  }

}
