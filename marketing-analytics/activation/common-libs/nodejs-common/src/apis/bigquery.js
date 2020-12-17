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
 * @fileoverview BigQuery API request wrapper on Google API Client Library.
 */

'use strict';

const {google} = require('googleapis');
const {OAuth2Client, JWT, Compute} = require('google-auth-library');
const AuthClient = require('./auth_client.js');
const {wait} = require('../components/utils.js');

const BIGQUERY_API_SCOPES = ['https://www.googleapis.com/auth/bigquery'];

/**
 * Configuration for a BigQuery Table, should include the Cloud Project Id,
 * Dataset Id, Table Id and possible authentication object.
 * @typedef {{
 *   projectId:(string|undefined),
 *   datasetId:(string|undefined),
 *   tableId:(string|undefined),
 *   auth:(!OAuth2Client|!JWT|!Compute|undefined)
 * }}
 */
let BigQueryOptions;

/**
 * Configuration for a BigQuery Table, should include the Cloud Project Id,
 * Dataset Id, Table Id and possible authentication object.
 * @typedef {{
 *   name:string,
 *   type:string,
 *   mode:string,
 *   fields:(!Array<!TableFieldSchema>|undefined),
 *   description:string
 * }}
 */
let TableFieldSchema;

/**
 * Google BigQuery API v2 request wrapper based on Google API Client Library.
 * Google Cloud Platform offers Cloud Client Library
 * (https://github.com/googleapis/google-cloud-node) is a better choice for
 * developer in most cases. However, it doesn't support OAuth authentication.
 * This class is mainly for OAuth scenarios and based on REST service, see:
 * https://cloud.google.com/bigquery/docs/reference/rest/
 */
class BigQuery {
  /**
   * Init BigQuery API client.
   * @param {?BigQueryOptions=} options
   */
  constructor(options = {}) {
    this.options = options;
    const authClient = new AuthClient(BIGQUERY_API_SCOPES);
    const auth = options.auth || authClient.getDefaultAuth();
    this.instance = google.bigquery({
      version: 'v2',
      auth,
    });
  }

  /**
   * TODO: rename the function; confirm the field type.
   * Gets the table fields.
   * @param {?BigQueryOptions=} options
   * @return {!Promise<!Array<!TableFieldSchema>>} Table fields.
   */
  getSchema(options = {}) {
    const tableOptions = Object.assign({}, this.options, options);
    return this.instance.tables.get(tableOptions)
        .then((response) => response.data.schema.fields);
  }

  /**
   * Checks the job's status and returns the data.rows when the job finished.
   * @param {string} jobId
   * @return {!Promise<!Array<!Object>>} Query results.
   */
  getJob(jobId) {
    const jobOptions = Object.assign({}, this.options, {jobId: jobId});
    return this.instance.jobs.get(jobOptions)
        .then((response) => {
          const status = response.data.status.state;
          if (status === 'DONE') {
            return this.instance.jobs.getQueryResults(jobOptions)
                .then((response) => response.data.rows);
          }
          return wait(5000).then(() => this.getJob(jobId));
        });
  }

  /**
   * Creates a query job and promises to return the result.
   * In case the job doesn't return with the status 'complete', will use the
   * function 'getJob' to promise get the result after jod is done.
   * @param {string} query
   * @param {string=} projectId
   * @return  {!Promise<!Array<!Object>>}
   */
  query(query, projectId = this.options.projectId) {
    console.log(`[QUERY] ${query}`);
    return this.instance.jobs
        .query({
          projectId: projectId,
          requestBody: {
            query: query,
            useLegacySql: false,
            useQueryCache: true,
          }
        })
        .then((response) => {
          const jobId = response.data.jobReference.jobId;
          const jobComplete = response.data.jobComplete;
          if (jobComplete === true) return response.data.rows;
          return this.getJob(jobId);
        })
        .catch((error) => {
          console.log(`[ERROR] ${query}`);
          console.error(error.message);
        });
  }

  /**
   * Gets the count of the query
   * @param {string} query
   * @param {string=} projectId
   * @return  {!Promise<!Array<!Object>>}
   */
  getCount(query, projectId = this.options.projectId) {
    return this.getStats(query);
  }

  /**
   * Gets the numerical statistics results, e.g. count, sum, avg, etc.
   * @param {string} query
   * @param {!Array<string>|string=} stats
   * @param {string=} projectId
   * @return {!Promise<T>}
   * @template T
   */
  getStats(query, stats = 'count', projectId = this.options.projectId) {
    stats = [stats].flat();
    return this.query(query, projectId).then((rows) => {
      if (stats.length === 1) {
        return parseFloat(rows[0].f[0].v);
      }
      const result = {};
      stats.forEach((s, index) => {
        result[s] = parseFloat(rows[0].f[index].v);
      });
      return result;
    });
  }

  /**
   * Change the original result of APPROX_TOP_COUNT from BigQuery into a
   * human readable map and change the number to integers.
   * Before:
   [ { v: { f: [Array] } },
   { v: { f: [Array] } },
   { v: { f: [Array] } },
   { v: { f: [Array] } } ]
   * After:
   [ { value: 'Direct', number: 17 },
   { value: 'Organic Search', number: 8 },
   { value: 'Referral', number: 5 },
   { value: 'Social', number: 4 } ]
   * @param {string} query
   * @param {string=} projectId
   * @return {!Promise<T>}
   */
  getTopCount(query, projectId = this.options.projectId) {
    return this.query(query, projectId).then(rows => {
      try {
        return rows[0].f[0].v.map(v => {
          const f = v.v.f;
          return {value: f[0].v, number: parseInt(f[1].v)};
        });
      } catch (e) {
        console.log(`ERROR: ${query}`);
        console.log(rows);
        console.log(e);
      }
    });
  }

  /**
   * Change the original result of APPROX_QUANTILES from BigQuery into a
   * human readable array of numbers.
   * Before:
   * [ { f : [ v : [ { v : '0' }, { v : '1' } ] ] } ]
   * After:
   * [ 0, 1 ]
   * @param {string} query
   * @param {string=} projectId
   * @return {!Promise<T>}
   */
  getApproxQuantiles(query, projectId = this.options.projectId) {
    return this.query(query, projectId).then(rows => {
      return rows[0].f[0].v.map(v => {
        return parseInt(v.v);
      });
    });
  }

  /**
   * GoogleSQL for APPROX_COUNT_DISTINCT of a column
   * @param {string} column
   * @param {string} table
   * @param {string=} where clause
   * @return {string}
   */
  static approxCountDistinct(column, table, where = '') {
    return `SELECT APPROX_COUNT_DISTINCT(${column}) FROM ${table} ${where}`;
  }

  /**
   * Gets query for GoogleSQL's function APPROX_TOP_COUNT.
   * @param {string} column
   * @param {number} number Number of the top values.
   * @param {string} table Table name.
   * @param {string=} where condition clause for query.
   * @return {string} Google SQL.
   */
  static approxTopCount(column, number, table, where = '') {
    return `SELECT APPROX_TOP_COUNT(${column}, ${number}) FROM ${table} ${
        where}`;
  }

  /**
   * Gets query for GoogleSQL's function APPROX_QUANTILES.
   * @param {string} column
   * @param {number} number
   * @param {string} table Table name.
   * @param {string=} where condition clause for query.
   * @return {string} Google SQL.
   */
  static approxQuantiles(column, number, table, where = '') {
    return `SELECT APPROX_QUANTILES(${column}, ${number}) FROM ${table} ${
        where}`;
  }

  /**
   * Gets query for GoogleSQL's function APPROX_QUANTILES.
   * @param {string} column
   * @param {string} table Table name.
   * @param {string=} where condition clause for query.
   * @param {!Array<string>|string=} stats
   * @return {string} Google SQL.
   */
  static getNumericalStats(column, table, where, stats = 'count') {
    const columns = [stats]
        .flat()
        .map(s => {
          return ` ${s}(${column})`;
        })
        .join(',');
    const query = `SELECT ${columns} FROM ${table} ${where}`;
    return query;
  }
}

exports.BigQuery = BigQuery;
