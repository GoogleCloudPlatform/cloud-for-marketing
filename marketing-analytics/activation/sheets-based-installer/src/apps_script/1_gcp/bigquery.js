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

/** @fileoverview BigQuery API handler class.*/

/**
 * The list of available BigQuery regions.
 * @see https://cloud.google.com/bigquery/docs/locations
 */
const BIGQUERY_LOCATIONS = [
  { displayName: 'United States', locationId: 'US' },
  { displayName: 'European Union', locationId: 'EU' },
  { displayName: 'Montréal', locationId: 'northamerica-northeast1' },
  { displayName: 'Toronto', locationId: 'northamerica-northeast2' },
  { displayName: 'Iowa', locationId: 'us-central1' },
  { displayName: 'South Carolina', locationId: 'us-east1' },
  { displayName: 'Northern Virginia', locationId: 'us-east4' },
  { displayName: 'Oregon', locationId: 'us-west1' },
  { displayName: 'Los Angeles', locationId: 'us-west2' },
  { displayName: 'Salt Lake City', locationId: 'us-west3' },
  { displayName: 'Las Vegas', locationId: 'us-west4' },
  { displayName: 'São Paulo', locationId: 'southamerica-east1' },
  { displayName: 'Santiago', locationId: 'southamerica-west1' },
  { displayName: 'Taiwan', locationId: 'asia-east1' },
  { displayName: 'Hong Kong', locationId: 'asia-east2' },
  { displayName: 'Tokyo', locationId: 'asia-northeast1' },
  { displayName: 'Osaka', locationId: 'asia-northeast2' },
  { displayName: 'Seoul', locationId: 'asia-northeast3' },
  { displayName: 'Mumbai', locationId: 'asia-south1' },
  { displayName: 'Delhi', locationId: 'asia-south2' },
  { displayName: 'Singapore', locationId: 'asia-southeast1' },
  { displayName: 'Jakarta', locationId: 'asia-southeast2' },
  { displayName: 'Sydney', locationId: 'australia-southeast1' },
  { displayName: 'Melbourne', locationId: 'australia-southeast2' },
  { displayName: 'Warsaw', locationId: 'europe-central2' },
  { displayName: 'Finland', locationId: 'europe-north1' },
  { displayName: 'Madrid', locationId: 'europe-southwest1' },
  { displayName: 'Belgium', locationId: 'europe-west1' },
  { displayName: 'London', locationId: 'europe-west2' },
  { displayName: 'Frankfurt', locationId: 'europe-west3' },
  { displayName: 'Netherlands', locationId: 'europe-west4' },
  { displayName: 'Zürich', locationId: 'europe-west6' },
  { displayName: 'Milan', locationId: 'europe-west8' },
  { displayName: 'Paris', locationId: 'europe-west9' },
];

/** Default configuration for load job. */
const LOAD_CSV_DEFAULT_OPTION = {
  sourceFormat: 'CSV',
  skipLeadingRows: 1,
  autodetect: true,
  writeDisposition: 'WRITE_TRUNCATE',
};

class BigQuery extends ApiBase {

  constructor(projectId, datasetId) {
    super();
    this.apiUrl = 'https://bigquery.googleapis.com/bigquery';
    this.version = 'v2';
    this.projectId = projectId;
    this.datasetId = datasetId;
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/projects/${this.projectId}`;
  }

  /**
   * Gets the specified BigQuery table resource by table ID.
   * @see https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/get
   * @param {string} tableId
   * @return {!Table} https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
   */
  getTable(tableId) {
    return this.get(`datasets/${this.datasetId}/tables/${tableId}`);
  }

  /**
   * Returns whether the specified table exists.
   * @param {string} tableId
   * @return {boolean}
   */
  existTable(tableId) {
    const { error } = this.getTable(tableId);
    if (error) {
      if (error.status === 'NOT_FOUND') return false;
      throw new Error(`Fail to get table ${tableId}, ${error.message}`);
    }
    return true;
  }

  /**
   * Creates a new, empty table in the dataset.
   * @see https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/insert
   * @param {!Table} config
   * @return {!Table}
   */
  createTable(config) {
    const response = this.mutate(`datasets/${this.datasetId}/tables`, config);
    return response;
  }

  /**
   * Updates information in an existing table.
   * @see https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/patch
   * @param {string} tableId
   * @param {!Table} config
   * @return {!Table}
   */
  updateTable(tableId, config) {
    const response = this.mutate(
      `datasets/${this.datasetId}/tables/${tableId}`, config, 'PUT');
    return response;
  }

  /**
   * Deteles an existing table.
   * @see https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/delete
   * @param {string} tableId
   * @return {object}
   */
  deleteTable(tableId) {
    const response = this.mutate(
      `datasets/${this.datasetId}/tables/${tableId}`, {}, 'DELETE');
    return response;
  }

  /**
   * Creates or updates a table.
   * @param {!Table} config
   * @return {!Table}
   */
  createOrUpdateTable(config) {
    const { tableId } = config.tableReference;
    if (this.existTable(tableId)) {
      return this.updateTable(tableId, config);
    }
    return this.createTable(config);
  }

  /**
   * Returns the specified dataset.
   * @see https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/get
   * @return {!Dataset}
   */
  getDataset() {
    return super.get(`datasets/${this.datasetId}`);
  }

  /**
   * Returns whether the specified dataset exists.
   * @return {boolean}
   */
  existDataset() {
    const { error } = this.getDataset();
    if (error) {
      if (error.status === 'NOT_FOUND') return false;
      throw new Error(`Fail to get dataset ${tableId}, ${error.message}`);
    }
    return true;
  }

  /**
   * Creates a new empty dataset.
   * @see https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert
   * @param {string} datasetId
   * @param {!Dataset} options
   * @return {!Dataset}
   */
  createDataset(datasetId = this.datasetId, options = {}) {
    const payload = Object.assign({
      datasetReference: {
        datasetId,
        projectId: this.projectId,
      },
      location: 'US',
    }, options);
    return this.mutate('datasets', payload);
  }

  /**
   * Updates information in an existing dataset.
   * @see https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/update
   * @param {string} datasetId
   * @param {!Dataset} config
   * @return {!Dataset}
   */
  updateDataset(datasetId = this.datasetId, config = {}) {
    const response = this.mutate(`datasets/${datasetId}`, config, 'PUT');
    return response;
  }

  /**
   * Creates or updates a dataset.
   * @param {!Dataset} config
   * @return {!Dataset}
   */
  createOrUpdateDataset(options) {
    if (this.existDataset()) {
      return this.updateDataset(this.datasetId, options);
    }
    return this.createDataset(this.datasetId, options);
  }

  /**
   * Starts a query job and waits until it's done.
   * @see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
   * @param {string} query
   */
  runQueryJob(query) {
    const request = { configuration: { query: { query, useLegacySql: false } } };
    const response = this.mutate('jobs', request);
    return this.waitForJobToComplete_(response);
  }

  /**
   * Returns information about a specific job.
   * @see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/get
   * @param {string} jobId
   * @param {string} location
   * @return {!Job}
   */
  getJob(jobId, location) {
    return this.get(`jobs/${jobId}?location=${location}`);
  }

  /**
   * Gets the results of a query job.
   * @see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults
   * @param {string} jobId
   * @param {string} location
   * @return {Array<object>}
   */
  getQueryResult(jobId, location) {
    const { rows } = this.get(`queries/${jobId}?location=${location}`);
    return rows;
  }

  /**
   * Gets the unique value of a query.
   * @param {string} query
   * @return {string}
   */
  getUniqueValue(query) {
    const { jobId, location } = this.runQueryJob(query);
    const rows = this.getQueryResult(jobId, location);
    return rows[0].f[0].v;
  }

  /**
   * Loads data into a BigQuery table. By default it loads a CVS with auto
   * detected schema and first line skipped.
   * @see LOAD_CSV_DEFAULT_OPTION
   * This function is based on BigQuery resumable upload API.
   * @see https://cloud.google.com/bigquery/docs/reference/api-uploads#resumable
   * @param {string} tableId
   * @param {string} content
   * @param {object} options
   * @return
   */
  loadData(tableId, content, options) {
    const destinationTable = {
      projectId: this.projectId,
      datasetId: this.datasetId,
      tableId,
    };
    const load = Object.assign(
      { destinationTable }, LOAD_CSV_DEFAULT_OPTION, options);
    const param = this.getFetchAppParams();
    param.headers['X-Upload-Content-Type'] = '*/*';
    param.headers['X-Upload-Content-Length'] = content.length;
    param.method = 'POST';
    param.payload = JSON.stringify({ configuration: { load } });
    const url = 'https://www.googleapis.com/upload/bigquery/' +
      `${this.version}/projects/${this.projectId}/jobs?uploadType=resumable`;
    const response = this.fetchAndReturnRawResponse(url, param);
    const { Location: uploadUrl } = response.getHeaders();
    console.log('BQ Load upload url', uploadUrl);
    if (!uploadUrl) {
      throw new Error('Could not get upload Url for BigQuery load job.');
    }
    const uploadResponse = this.mutate(uploadUrl, content, 'PUT');
    return this.waitForJobToComplete_(uploadResponse);
  }

  /**
   * Waits the BigQuery job completed and returns the job reference.
   * @param {Job} response https://cloud.google.com/bigquery/docs/reference/rest/v2/Job
   * @return {!JobReference} A job reference is a fully qualified identifier for
   *   referring to a job.
   * @see https://cloud.google.com/bigquery/docs/reference/rest/v2/JobReference
   */
  waitForJobToComplete_(response) {
    const { jobReference } = response;
    const { jobId, location } = jobReference;
    let status = response.status;
    while (status.state !== 'DONE') {
      console.log(`Job[${jobId}] is running. Wait for 10s before rechecking.`);
      Utilities.sleep(10000);
      status = this.getJob(jobId, location).status;
    }
    if (status.errorResult) {
      throw new Error(`Failed job [${jobId}]: ${status.errorResult.message}`);
    }
    return jobReference;
  }
}
