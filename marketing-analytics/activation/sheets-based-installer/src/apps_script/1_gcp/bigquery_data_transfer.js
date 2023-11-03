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

/** @fileoverview BigQuery Data Transfer API handler class.*/

/**
 * Types of data transfer configuration.
 * @enum {string}
 */
const DATA_TRANSFER_SOURCE = Object.freeze({
  GOOGLE_MERCHANT_CENTER: 'merchant_center',
  GOOGLE_ADS: 'google_ads',
  GOOGLE_ADWORDS: 'adwords',
  SCHEDULED_QUERY: 'scheduled_query',
});

class BigQueryDataTransfer extends ApiBase {

  constructor(projectId, locationId) {
    super();
    this.apiUrl = 'https://bigquerydatatransfer.googleapis.com';
    this.version = 'v1';
    this.projectId = projectId;
    this.locationId = locationId;
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/projects/${this.projectId}`;
  }

  /**
   * Returns the unified config id or job id with the format
   * `locations/locationId/transferConfigs/transferConfigId` because the
   * `getBaseUrl` function has offered the `project/projectId`.
   * @param {string} configId
   * @return {string}
   * @private
   */
  getIdWithoutProject_(configId) {
    const ids = configId.split('/');
    if (configId.startsWith('projects')) return ids.slice(2).join('/');
    if (configId.startsWith('locations')) return configId;
    return `locations/${this.locationId}/transferConfigs/${configId}`;
  }

  /**
   * Returns information about a data transfer config.
   * @see https://cloud.google.com/bigquery/docs/reference/datatransfer/rest/v1/projects.locations.transferConfigs/get
   * @param {string} configId
   * @return {!TransferConfig}
   */
  getTransferConfig(configId) {
    return this.get(this.getIdWithoutProject_(configId));
  }

  /**
   * Returns information about all transfer configs owned by a project in the
   * specified location.
   * @see https://cloud.google.com/bigquery/docs/reference/datatransfer/rest/v1/projects.locations.transferConfigs/list
   * @param {{dataSourceIds:(Array<string>|undefined)}} param
   * @return {!ListTransferConfigsResponse}
   */
  listTransferConfigs(param) {
    if (this.locationId)
      return this.get(`locations/${this.locationId}/transferConfigs`, param);
    return this.get('transferConfigs', param);
  }

  /**
   * Creates a new data transfer configuration.
   * @see https://cloud.google.com/bigquery/docs/reference/datatransfer/rest/v1/projects.locations.transferConfigs/create
   * @param {!TransferConfig} config
   * @param {{authorizationCode: (string|undefined)}} params
   * @return {!TransferConfig}
   */
  createTransferConfig(config, params) {
    const response = this.mutate(
      `locations/${this.locationId}/transferConfigs${this.getQueryString(params)}`,
      config);
    return response;
  }

  /**
   * Updates a data transfer configuration.
   * @see https://cloud.google.com/bigquery/docs/reference/datatransfer/rest/v1/projects.locations.transferConfigs/patch
   * @param {string} configId
   * @param {!TransferConfig} config
   * @param {{updateMask:string}} params
   * @return {!TransferConfig}
   */
  updateTransferConfig(configId, config, params = {}) {
    const id = this.getIdWithoutProject_(configId);
    params.updateMask = Object.keys(config).join(',');
    const response = this.mutate(`${id}${this.getQueryString(params)}`,
      config, 'PATCH');
    return response;
  }

  /**
   * Starts manual transfer runs to be executed now.
   * @see https://cloud.google.com/bigquery/docs/reference/datatransfer/rest/v1/projects.locations.transferConfigs/startManualRuns
   * @param {string} configId
   * @return {!StartManualTransferRunsResponse}
   */
  startManualRuns(configId) {
    const id = this.getIdWithoutProject_(configId);
    return this.mutate(
      `${id}:startManualRuns`, { requestedRunTime: new Date().toISOString() }
    );
  }

  /**
   * Returns information about running and completed transfer runs.
   * @see https://cloud.google.com/bigquery/docs/reference/datatransfer/rest/v1/projects.transferConfigs.runs/list
   * @param {string} configId
   * @return {!ListTransferRunsResponse}
   */
  listRuns(configId) {
    const id = this.getIdWithoutProject_(configId);
    return this.get(`${id}/runs`);
  }

  /**
   * Returns log messages for the transfer run.
   * @see https://cloud.google.com/bigquery/docs/reference/datatransfer/rest/v1/projects.locations.transferConfigs.runs.transferLogs/list
   * @param {string} jobId
   * @return {!ListTransferLogsResponse}
   */
  listRunsLogs(jobId) {
    const id = this.getIdWithoutProject_(jobId);
    return this.get(`${id}/transferLogs`);
  }

  /**
   * Returns true if valid credentials doesn't exist for the given data source
   * and requesting user.
   * @see https://cloud.google.com/bigquery/docs/reference/datatransfer/rest/v1/projects.dataSources/checkValidCreds
   * @param {string} dataSourceId
   * @return {boolean}
   */
  needsAuthorizationCode(dataSourceId) {
    const { hasValidCreds } = this.mutate(
      `locations/${this.locationId}/dataSources/${dataSourceId}:checkValidCreds`);
    return hasValidCreds !== true;
  }

  /**
   * Returns a link for the user to complete OAuth2 authorization and generate
   * the authorization code to use with the transfer configuration.
   * This is required when new credentials are needed, as indicated by
   * the method 'needsAuthorizationCode'.
   * @see https://cloud.google.com/bigquery/docs/reference/datatransfer/rest/v1/projects.transferConfigs/create#query-parameters
   * @param {string} dataSourceId
   * @return {string}
   */
  getAuthUrlForDataSource(dataSourceId) {
    const { clientId, scopes } =
      this.get(`locations/${this.locationId}/dataSources/${dataSourceId}`);
    const params = {
      redirect_uri: 'urn:ietf:wg:oauth:2.0:oob',
      response_type: 'authorization_code',
      client_id: clientId,
      scope: scopes.join(' '),
    };
    const url = 'https://www.gstatic.com/bigquerydatatransfer/oauthz/auth' +
      this.getQueryString(params);
    return url;
  }

}
