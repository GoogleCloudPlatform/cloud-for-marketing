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
    this.version = 'v4';
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /** @override */
  getScope() {
    return 'https://www.googleapis.com/auth/display-video';
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
    const advertisers = this.list('advertisers', 'advertisers', parameters);
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
    const campaigns = this.list(
      'campaigns',
      `advertisers/${advertiserId}/campaigns`,
      parameters,
    );
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
    const insertionOrders = this.list(
      'insertionOrders',
      `advertisers/${advertiserId}/insertionOrders`,
      parameters,
    );
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
      `advertisers/${advertiserId}/insertionOrders/${insertionOrderId}`,
    );
  }

  /**
   * Lists the line items of the given advertiser.
   * @see https://developers.google.com/display-video/api/reference/rest/v3/advertisers.lineItems/list
   * @param {string} advertiserId
   * @param {Object=} parameters
   * @return {Array<LineItem>}
   */
  listLineItems(advertiserId, parameters = {}) {
    const lineItems = this.list(
      'lineItems',
      `advertisers/${advertiserId}/lineItems`,
      parameters,
    );
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
    const lineItemAssignedTargetingOptions = this.list(
      'lineItemAssignedTargetingOptions',
      `advertisers/${advertiserId}/lineItems:bulkListAssignedTargetingOptions`,
      {lineItemIds},
    );
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
    const adGroups = this.list(
      'adGroups',
      `advertisers/${advertiserId}/adGroups`,
      {filter: lineItemIds.map(lineItemId => `lineItemId=${lineItemId}`)},
    );
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
    const adGroupAssignedTargetingOptions = this.list(
      'adGroupAssignedTargetingOptions',
      `advertisers/${
        advertiserId
      }/adGroups:bulkListAdGroupAssignedTargetingOptions`,
      {adGroupIds},
    );
    return adGroupAssignedTargetingOptions;
  }
}
