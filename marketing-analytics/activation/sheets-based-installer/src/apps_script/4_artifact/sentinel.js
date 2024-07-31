// Copyright 2021 Google Inc.
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

/** @fileoverview Sentinel Cloud Functions deployment class.*/

/** @const {string} SENTINEL_PACKAGE package name of Sentinel. */
const SENTINEL_PACKAGE = '@google-cloud/data-tasks-coordinator';

/**
 * Sentinel features based on external APIs.
 * @const {!Array<!SolutionFeature>}
 */
const SENTINEL_FEATURES = Object.freeze([
  {
    desc: 'Batch Prediction on Cloud AutoML API',
    api: 'automl.googleapis.com',
    code: 'AutoML',
  },
  {
    desc: 'Batch Prediction on Vertex AI API',
    api: 'aiplatform.googleapis.com',
    code: 'VertexAI',
  },
  {
    desc: 'Download Google Ads Reports',
    api: 'googleads.googleapis.com',
    code: 'GoogleAds',
  },
  {
    desc: 'Download Campaign Manager 360 Reports',
    api: 'dfareporting.googleapis.com',
    code: 'CM',
  },
  {
    desc: 'Download Display & Video 360 Reports/Data',
    api: [
      'doubleclickbidmanager.googleapis.com',
      'displayvideo.googleapis.com',
    ],
    code: 'DV360',
  },
  {
    desc: 'Download Search Ads 360 Reports',
    api: 'searchads360.googleapis.com',
    code: 'SA360',
  },
  {
    desc: 'Download YouTube Reports',
    api: 'youtube.googleapis.com',
    code: 'YouTube',
  },
  {
    desc: 'BigQuery query external tables based on Google Sheet',
    api: [
      'drive.googleapis.com', //For BigQuery query
      'sheets.googleapis.com', //For checking accessibility
    ],
    code: 'ExternalTableOnSheets',
  },
  {
    desc: 'Run Ads Data Hub Queries',
    api: 'adsdatahub.googleapis.com',
    code: 'ADH',
  },
  {
    desc: 'Run BigQuery Data Transfer queries',
    api: 'bigquerydatatransfer.googleapis.com',
    code: 'DataTransfer',
  },
  {
    desc: '(Legacy) Download Doubleclick Search Reports',
    api: 'doubleclicksearch.googleapis.com',
    code: 'DS',
  },
]);

/**
 * The option object to initialize an object for Sentinel.
 * @typedef {{
 *   projectId:string,
 *   locationId:string,
 *   namespace:string,
 *   bucket:string,
 *   inbound:string,
 *   version:string,
 * }}
 */
let SentinelOption;

/**
 * Cloud Functions of Sentinel.
 */
class Sentinel extends PrimeSolution {

  /**
   * @param {!SentinelOption} options
   */
  constructor(options) {
    super(options);
    this.namespace = options.namespace || 'sentinel';
    this.inbound = options.inbound || 'inbound';
    if (!this.inbound.endsWith('/')) this.inbound = this.inbound + '/';
  }

  /** @override */
  getPackageName() {
    return SENTINEL_PACKAGE;
  }

  /** @override */
  deployCloudFunctions(name) {
    if (name.endsWith('main')) return this.deployMain(name);
    if (name.endsWith('gcs')) return this.deployStorageMonitor(name);
    if (name.endsWith('report')) return this.deployReportWorkflow(name);
    throw new Error(`Unknown Cloud Functions ${name}`);
  }

  /**
   * Deploy Sentinel `main` Cloud Functions.
   *
   * @param {string=} functionName
   * @return {string} The name of operation.
   */
  deployMain(functionName = `${this.namespace}_main`) {
    const entryPoint = 'coordinateTask';
    const eventTrigger = this.getPubSubEventTrigger(
      Sentinel.getMonitorTopicName(this.namespace)
    );
    eventTrigger.failurePolicy = { retry: {} };
    return this.deploySingleCloudFunction(functionName,
      { entryPoint, eventTrigger }, this.getSecretEnv()
    );
  }

  /**
   * Deploy Sentinel `gcs` monitor Cloud Functions.
   *
   * @param {string=} functionName
   * @return {string} The name of operation.
   */
  deployStorageMonitor(functionName = `${this.namespace}_gcs`) {
    const entryPoint = 'monitorStorage';
    const eventTrigger = this.getStorageEventTrigger();
    return this.deploySingleCloudFunction(functionName,
      { entryPoint, eventTrigger }, { SENTINEL_INBOUND: this.inbound }
    );
  }

  /**
   * Deploy Sentinel `reportWorkflow` Cloud Functions.
   *
   * @param {string=} functionName
   * @return {string} The name of operation.
   */
  deployReportWorkflow(functionName = `${this.namespace}_report`) {
    const entryPoint = 'reportWorkflow';
    const httpsTrigger = {};
    return this.deploySingleCloudFunction(functionName,
      { entryPoint, httpsTrigger }
    );
  }
}

/**
 * Returns the Pub/Sub topic name of Sentinel main Cloud Functions.
 * @param {string} namespace
 * @return {string}
 */
Sentinel.getMonitorTopicName = (namespace) => `${namespace}-monitor`;
