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

/** @fileoverview Tentacles Cloud Functions deployment class.*/

/** @const {string} TENTACLES_PACKAGE package name of Tentacles. */
const TENTACLES_PACKAGE = '@google-cloud/gmp-googleads-connector';

/**
 * Api connectors supported by Tentacles.
 * @const {!Array<!SolutionFeature>}
 */
const TENTACLES_CONNECTORS = Object.freeze([
  { code: 'MP', desc: 'Google Analytics Measurement Protocol' },
  { code: 'MP_GA4', desc: 'Google Analytics 4 Measurement Protocol' },
  {
    code: 'GA',
    desc: 'Google Analytics Data Import',
    api: 'analytics.googleapis.com',
  },
  {
    code: 'CM',
    desc: 'Campaign Manager Conversions Upload',
    api: [
      'dfareporting.googleapis.com',
      'doubleclicksearch.googleapis.com',
    ],
  },
  {
    code: 'SA',
    desc: 'Search Ads 360 Conversions Upload',
    api: 'doubleclicksearch.googleapis.com',
  },
  {
    code: 'ACM',
    desc: 'Google Ads Customer Match Upload',
    api: 'googleads.googleapis.com',
  },
  {
    code: 'CALL',
    desc: 'Google Ads Call Conversions Upload',
    api: 'googleads.googleapis.com',
  },
  {
    code: 'ACLC',
    desc: 'Google Ads Click Conversions Upload',
    api: 'googleads.googleapis.com',
  },
  {
    code: 'ACA',
    desc: 'Google Ads Enhanced Conversions Upload',
    api: 'googleads.googleapis.com',
  },
  {
    code: 'AOUD',
    desc: 'Google Ads Offline UserData Upload (OfflineUserDataService)',
    api: 'googleads.googleapis.com',
  },
  {
    code: 'GS',
    desc: 'Google Sheets (Google Ads Conversions Upload through Google Sheets)',
    api: 'sheets.googleapis.com',
  },
  { code: 'SFTP', desc: 'SFTP Upload for Search Ads 360 Business Data Upload' },
  {
    code: 'PB',
    desc: 'Pub/Sub Messages Send',
    api: 'pubsub.googleapis.com',
  },
]);

/**
 * The option object to initialize an object for Tentacles.
 * @typedef {{
 *   projectId:string,
 *   locationId:string,
 *   namespace:string,
 *   bucket:string,
 *   outbound:string,
 *   version:string,
 * }}
 */
let TentaclesOption;

/**
 * Cloud Functions of Tentacles.
 */
class Tentacles extends PrimeSolution {

  /**
   * @param {!TentaclesOption} options
   */
  constructor(options) {
    super(options);
    this.namespace = options.namespace || 'tentacles';
    this.outbound = options.outbound || 'outbound/';
    if (!this.outbound.endsWith('/')) this.outbound = this.outbound + '/';
  }

  /** @override */
  getPackageName() {
    return TENTACLES_PACKAGE;
  }

  /** @override */
  deployCloudFunctions(name) {
    switch (name) {
      case `${this.namespace}_init`:
        return this.deployInitiator(name);
      case `${this.namespace}_tran`:
        return this.deployTransporter(name);
      case `${this.namespace}_api`:
        return this.deployApiRequester(name);
      default:
        throw new Error(`Unknown Cloud Functions ${name}`);
    }
  }

  /**
   * Deploy Tentacles `initiate` Cloud Functions.
   *
   * @param {string=} functionName
   * @return {string} The name of operation.
   */
  deployInitiator(functionName = `${this.namespace}_init`) {
    const entryPoint = 'initiate';
    const eventTrigger = this.getStorageEventTrigger();
    return this.deploySingleCloudFunction(functionName,
      { entryPoint, eventTrigger }, { TENTACLES_OUTBOUND: this.outbound }
    );
  }

  /**
   * Deploy Tentacles `transport` Cloud Functions.
   *
   * @param {string=} functionName
   * @return {string} The name of operation.
   */
  deployTransporter(functionName = `${this.namespace}_tran`) {
    const entryPoint = 'transport';
    const eventTrigger = this.getPubSubEventTrigger(`${this.namespace}-trigger`);
    eventTrigger.failurePolicy = { retry: {} };
    return this.deploySingleCloudFunction(functionName,
      { entryPoint, eventTrigger }
    );
  }

  /**
   * Deploy Tentacles `requestApi` Cloud Functions.
   *
   * @param {string=} functionName
   * @return {string} The name of operation.
   */
  deployApiRequester(functionName = `${this.namespace}_api`) {
    const entryPoint = 'requestApi';
    const eventTrigger = this.getPubSubEventTrigger(`${this.namespace}-push`);
    eventTrigger.failurePolicy = { retry: {} };
    return this.deploySingleCloudFunction(functionName,
      { entryPoint, eventTrigger }, this.getSecretEnv()
    );
  }

}
