// Copyright 2022 Google Inc.
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

/** @fileoverview Display & Video 360 API handler class.*/

class DisplayVideo extends ExternalApi {

  constructor(option) {
    super(option);
    this.name = 'Display & Video 360 API';
    this.api = 'displayvideo.googleapis.com';
    this.apiUrl = `https://${this.api}`;
    this.version = 'v3';
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /** @override */
  getScope() {
    return 'https://www.googleapis.com/auth/display-video';
  }

  /**
   * DV 360 API lists results in pages. To get the complete list, this function
   * uses the `nextPageToken` in the response to generate next request.
   * @param {string} requestUri
   * @param {Object} parameters
   * @param {*} entityName The name of entity in the response.
   * @return {Array<Object>}
   */
  getEntities(requestUri, parameters, entityName) {
    let result = [];
    let nextPageToken;
    let counter = 0;
    do {
      const response = super.get(requestUri, parameters);
      if (response[entityName])
        result = result.concat(response[entityName]);
      nextPageToken = response.nextPageToken;
      if (nextPageToken) {
        parameters.pageToken = nextPageToken;
        console.log(`Get next page for ${entityName}`, counter++);
      }
    } while (nextPageToken)
    return result;
  }

  listPartners(parameters = {}) {
    const response = super.get('partners', parameters);
    return response;
  }

  /**
   * Lists the advertisers of the given partner.
   * @see https://developers.google.com/display-video/api/reference/rest/v3/advertisers/list
   * @param {string} partnerId
   * @param {Object=} parameters
   * @return {Array<Advertiser>}
   */
  listAdvertisers(partnerId, parameters = {}) {
    parameters.partnerId = partnerId;
    const advertisers = this.getEntities(
      'advertisers', parameters, 'advertisers');
    return advertisers;
  }

  /**
   * Gets the advertiser.
   * @see https://developers.google.com/display-video/api/reference/rest/v3/advertisers/get
   * @param {string} advertiserId
   * @return {Advertiser}
   */
  getAdvertiser(advertiserId) {
    return this.get(`advertisers/${advertiserId}`);
  }

  /**
   * Lists the campaigns of the given advertiser.
   * @see https://developers.google.com/display-video/api/reference/rest/v3/advertisers.campaigns/list
   * @param {string} advertiserId
   * @param {Object=} parameters
   * @return {Array<Campaign>}
   */
  listCampaigns(advertiserId, parameters = {}) {
    const campaigns = this.getEntities(
      `advertisers/${advertiserId}/campaigns`, parameters, 'campaigns');
    return campaigns;
  }

  /**
   * Lists the insertion orders of the given advertiser.
   * @see https://developers.google.com/display-video/api/reference/rest/v3/advertisers.insertionOrders/list
   * @param {string} advertiserId
   * @param {Object=} parameters
   * @return {Array<InsertionOrder>}
   */
  listInsertionOrders(advertiserId, parameters = {}) {
    const insertionOrders = this.getEntities(
      `advertisers/${advertiserId}/insertionOrders`, parameters, 'insertionOrders');
    return insertionOrders;
  }

  /**
   * Gets the insertion order.
   * @see https://developers.google.com/display-video/api/reference/rest/v3/advertisers.insertionOrders/get
   * @param {string} advertiserId
   * @param {string} insertionOrderId
   * @return {InsertionOrder}
   */
  getInsertionOrder(advertiserId, insertionOrderId) {
    return this.get(
      `advertisers/${advertiserId}/insertionOrders/${insertionOrderId}`);
  }

  /**
   * Lists the line items of the given advertiser.
   * @see https://developers.google.com/display-video/api/reference/rest/v3/advertisers.lineItems/list
   * @param {string} advertiserId
   * @param {Object=} parameters
   * @return {Array<LineItem>}
   */
  listLineItems(advertiserId, parameters = {}) {
    const lineItems = this.getEntities(
      `advertisers/${advertiserId}/lineItems`, parameters, 'lineItems');
    return lineItems;
  }

  /**
   * Lists assigned targeting options for multiple line items across targeting
   * types.
   * @see https://developers.google.com/display-video/api/reference/rest/v3/advertisers.lineItems/bulkListAssignedTargetingOptions
   * @param {string} advertiserId
   * @param {Array<string>} lineItemIds
   * @return {Array<LineItemAssignedTargetingOption>}
   */
  listLineItemAssignedTargetingOptions(advertiserId, lineItemIds) {
    const lineItemAssignedTargetingOptions = this.getEntities(
      `advertisers/${advertiserId}/lineItems:bulkListAssignedTargetingOptions`,
      { lineItemIds }, 'lineItemAssignedTargetingOptions');
    return lineItemAssignedTargetingOptions;
  }

  /**
   * Lists Ad groups for multiple line items.
   * @see https://developers.google.com/display-video/api/reference/rest/v3/advertisers.adGroups/list
   * @param {string} advertiserId
   * @param {Array<string>} lineItemIds
   * @return {Array<AdGroup>}
   */
  listAdGroups(advertiserId, lineItemIds) {
    const adGroups = this.getEntities(
      `advertisers/${advertiserId}/adGroups`,
      { filter: lineItemIds.map((lineItemId) => `lineItemId=${lineItemId}`) },
      'adGroups');
    return adGroups;
  }

  /**
   * Lists assigned targeting options for multiple Ad groups across
   * targeting types.
   * @see https://developers.google.com/display-video/api/reference/rest/v3/advertisers.adGroups/bulkListAdGroupAssignedTargetingOptions#AdGroupAssignedTargetingOption
   * @param {string} advertiserId
   * @param {Array<string>} adGroupIds
   * @return {Array<AdGroupAssignedTargetingOption>}
   */
  listAdGroupAssignedTargetingOptions(advertiserId, adGroupIds) {
    const adGroupAssignedTargetingOptions = this.getEntities(
      `advertisers/${advertiserId}/adGroups:bulkListAdGroupAssignedTargetingOptions`,
      { adGroupIds }, 'adGroupAssignedTargetingOptions');
    return adGroupAssignedTargetingOptions;
  }
}
