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
 * @fileoverview Google Ads API (unofficial) Wrapper.
 */
'use strict';

const {GoogleAdsApi} = require('google-ads-api');
const {
  GoogleAdsClient,
  SearchGoogleAdsRequest,
  SearchGoogleAdsStreamRequest,
  GetGoogleAdsFieldRequest,
  SearchGoogleAdsFieldsRequest,
  GoogleAdsField,
} = require('google-ads-node');
const {buildReportQuery} = require('google-ads-api/build/utils');
const AuthClient = require('./auth_client.js');
const {getLogger} = require('../components/utils.js');
const { customer } = require('google-ads-node/build/lib/fields');

/** @type {!ReadonlyArray<string>} */
const API_SCOPES = Object.freeze(['https://www.googleapis.com/auth/adwords',]);

/**
 * Configuration for uploading click conversions for Google Ads, includes:
 * gclid, conversion_action, conversion_date_time, conversion_value,
 * currency_code, order_id, external_attribution_data
 * @see https://developers.google.com/google-ads/api/reference/rpc/v4/ClickConversion 
 * @typedef {{
    *   gclid: string,
    *   conversion_action: string,
    *   conversion_date_time: string,
    *   conversion_value: number,
    *   currency_code:(string|undefined),
    *   order_id: (string|undefined),
    *   external_attribution_data: (GoogleAdsApi.ExternalAttributionData|undefined),
    * }}
    */
let ClickConversionConfig;

/**
 * Configuration for querying report from Google Ads, includes:
 * entity, attributes, metrics, and constraints etc.
 * For other properties, see
 * https://opteo.com/dev/google-ads-api/#report
 * https://developers.google.com/google-ads/api/docs/query/grammar
 * @typedef {{
 *   entity:string,
 *   attributes:(!Array<string>|undefined),
 *   metrics:(!Array<string>|undefined),
 *   segments:(!Array<string>|undefined),
 *   constraints:(!Array<{
 *     key:string,
 *     op:string,
 *     val:string,
 *   }|{string,string}>|undefined),
 *   date_constant:(string|undefined),
 *   from_date:(string|undefined),
 *   to_date:(string|undefined),
 *   limit:(number|undefined),
 *   order_by:(string|undefined),
 *   sort_order:('DESC'|'ASC'|undefined),
 *   page_size:(number|undefined),
 * }}
 */
let ReportQueryConfig;

/**
 * Google Ads API v4.0 stub.
 * see https://opteo.com/dev/google-ads-api/#features
 */
class GoogleAds {
  /**
   * Note: Rate limits is set by the access level of Developer token.
   * @param {string} developerToken Developer token to access the API.
   */
  constructor(developerToken) {
    this.debug = process.env['DEBUG'] === 'true';
    const authClient = new AuthClient(API_SCOPES);
    this.token = authClient.getOAuth2Token();
    this.nodeClientOption = {
      client_id: this.token.clientId,
      client_secret: this.token.clientSecret,
      refresh_token: this.token.refreshToken,
      developer_token: developerToken,
      /* Un-marshals the gRPC response blobs to plain Javscript objects. */
      parseResults: true,
    };
    this.logger = getLogger('API.AD');
    this.logger.debug(`Init ${this.constructor.name} with Debug Mode.`);
  }

  /**
   * Gets all child advertise accounts owned by a given account.
   * If the given one is an advertiser account, it will return it self.
   * @param {string} parentCustomerId
   * @return {!Promise<!Array<string>>}
   */
  async getChildAccounts(parentCustomerId) {
    /** @type {ReportQueryConfig} */
    const childAccountsReportConfig = {
      entity: 'customer_client',
      attributes: ['customer_client.id'],
      constraints: [{'customer_client.manager': 'FALSE'}],
    };
    this.logger.debug(
        `Get Child Accounts for parentCustomerId: ${parentCustomerId}`);
    const {resultsList} = await this.reportOnNode(parentCustomerId,
        parentCustomerId, childAccountsReportConfig);
    return resultsList.map(({customerClient: {id}}) => id.toString());
  }

  /**
   * Gets stream report of a given Customer account on Nodejs library.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {!ReportQueryConfig} reportQueryConfig
   * @return {!Promise<!ReadableStream>}
   */
  streamReportOnNode(customerId, loginCustomerId, reportQueryConfig) {
    const client = this.getGoogleAdsNodeClient_(loginCustomerId);
    const service = client.getService("GoogleAdsService", {useStreaming: true});
    const request = new SearchGoogleAdsStreamRequest();
    request.setQuery(buildReportQuery(reportQueryConfig));
    request.setCustomerId(customerId);
    return service.searchStream(request);
  }

  /**
   * Gets report of a given Customer account on Nodejs library.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {!ReportQueryConfig} reportQueryConfig
   * @return {!Promise<{resultsList:!Array<{string:Object<string,string>}>}>}
   */
  reportOnNode(customerId, loginCustomerId, reportQueryConfig) {
    const client = this.getGoogleAdsNodeClient_(loginCustomerId);
    const service = client.getService("GoogleAdsService");
    const request = new SearchGoogleAdsRequest();
    request.setQuery(buildReportQuery(reportQueryConfig));
    request.setCustomerId(customerId);
    return service.search(request);
  }

  /**
   * Gets resource information from Google Ads API. see:
   * https://developers.google.com/google-ads/api/docs/concepts/field-service
   * @param {string|number} loginCustomerId Login customer account ID.
   * @param {string} resourceName
   * @return {!Promise<!GoogleAdsField>}
   */
  getMetaData(loginCustomerId, resourceName) {
    const client = this.getGoogleAdsNodeClient_(loginCustomerId);
    const service = client.getService("GoogleAdsFieldService");
    const request = new GetGoogleAdsFieldRequest();
    request.setResourceName(`googleAdsFields/${resourceName}`);
    return new Promise((resolve, reject) => {
      // 2020.10.18 This API current requires a callback function.
      service.getGoogleAdsField(request, (error, response) => {
        if (error) return reject(error);
        resolve(response);
      });
    });
  }

  /**
   * Returns resources information from Google Ads API. see:
   * https://developers.google.com/google-ads/api/docs/concepts/field-service
   * @param {string|number} loginCustomerId Login customer account ID.
   * @param {Array<string>} adFields Array of Ad fields.
   * @param {Array<string>} metadata Select fields, default values are:
   *     name, data_type, is_repeated, type_url.
   * @return {!Promise<!Array<GoogleAdsField>>}
   */
  searchMetaData(loginCustomerId, adFields, metadata = [
    'name', 'data_type', 'is_repeated', 'type_url',]) {
    const client = this.getGoogleAdsNodeClient_(loginCustomerId);
    const service = client.getService("GoogleAdsFieldService");
    const request = new SearchGoogleAdsFieldsRequest();
    request.setQuery(
        `SELECT ${metadata.join(',')} WHERE name IN ("${adFields.join('","')}")`
    );
    return new Promise((resolve, reject) => {
      // 2020.10.18 This API current requires a callback function.
      service.searchGoogleAdsFields(request, (error, response) => {
        if (error) return reject(error);
        resolve(response.resultsList);
      });
    });
  }

  /**
   * Returns an instance of GoogleAdsClient on google-ads-node.
   * @param {string} loginCustomerId Login customer account ID.
   * @return {GoogleAdsClient}
   * @private
   */
  getGoogleAdsNodeClient_(loginCustomerId) {
    return new GoogleAdsClient({
      login_customer_id: loginCustomerId,
      ...this.nodeClientOption,
    });
  }
  /**
   * Returns the function to send out a request to Google Ads API with a batch
   * of click conversions.
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @param {!ClickConversionConfig} adsConfig Default click conversion params
   * @return {!SendSingleBatch} Function which can send a batch of hits to
   *     Google Ads API.
   */
   getUploadConversionFn(customerId, loginCustomerId, adsConfig) {
    /**
     * Sends a batch of hits to Google Ads API.
     * @param {!Array<string>} lines Data for single request. It should be
     *     guaranteed that it doesn't exceed quota limitation.
     * @param {string} batchId The tag for log.
     * @return {!Promise<boolean>}
     */
    return (lines, batchId) => {
      /** @type {function} Gets the conversion elements from the data object. */
      const conversions = lines.map((line) => {
        const record = JSON.parse(line);
        return Object.assign({}, adsConfig, record);
      });
      try {
        return this.uploadClickConversions(conversions, customerId, loginCustomerId);
      } catch (err) {
        console.log(`Error in getUploadConFn ${err} in batchId: ${batchId}`);
        return false;
      }
    }
  }
  /**
   * Uploads click conversions to google ads account.
   * It requires an array of click conversions and customer id.
   * @param {Array<ClickConversionConfig>} clickConversionConfig ClickConversions
   * @param {string} customerId
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @return {!Promise<boolean>}
   */
  async uploadClickConversions(clickConversionConfig, customerId, loginCustomerId) {
    this.logger.debug(`Upload click conversions for customerId: ${customerId}`);
    const customer = this.getGoogleAdsApiCustomer_(customerId, loginCustomerId);
    try{
        const result = await customer.conversionUploads.uploadClickConversions(
            clickConversionConfig,
            {
                validate_only: this.debug, // when true makes no changes
                partial_failure: true, // Will still create the non-failed entities
            },
            true // Will return an array of all uploaded conversions,
        );
        const response = result[0].results_list
            .filter((record) => typeof record.gclid !== 'undefined')
            .map((record) => record.gclid.value);
        this.logger.debug(`Response: ${response}`);
        const failed = result[0].partial_failure_error;
        if (failed) {
            const errors = result[0].partial_failure_error.errors
                .map((e) => e.message);
            console.log(`Errors: ${errors.join('\n')}`);
        }
        return !failed;
    } catch (err) {
        console.log(`Error ${err} in uploadClickConversions`);
        return false;
    }
  }

  /**
   * Returns an instance of GoogleAdsApi.Customer on google-ads-api.
   * @param {string} customerId Customer account ID.
   * @param {string} loginCustomerId Login customer account ID (Mcc Account id).
   * @return {GoogleAdsApi.Customer}
   * @private
   */
  getGoogleAdsApiCustomer_(customerId, loginCustomerId) {
    const googleAdsApiClient = this.getGoogleAdsApiClient_();
    return googleAdsApiClient.Customer({
        customer_account_id: customerId,
        login_customer_id: loginCustomerId,
        refresh_token: this.nodeClientOption.refresh_token,
    });
  }

  /**
   * Returns an instance of GoogleAdsApi on google-ads-api.
   * @return {GoogleAdsApi}
   * @private
   */
  getGoogleAdsApiClient_() {
    return new GoogleAdsApi({
      client_id: this.nodeClientOption.client_id,
      client_secret: this.nodeClientOption.client_secret,
      developer_token: this.nodeClientOption.developer_token,
    });
  }
}

module.exports = {
  ClickConversionConfig,
  GoogleAds,
  ReportQueryConfig,
  GoogleAdsField,
};
