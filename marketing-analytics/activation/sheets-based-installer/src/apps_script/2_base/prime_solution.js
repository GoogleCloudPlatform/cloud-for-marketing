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

/** @fileoverview This is the base class of a prime solution. */

/**
 * Definition of features for external system/APIs, e.g. Tetnacles Connectors,
 * or Sentinel external features.
 * @typedef {{
 *   api:(string|!Array<string>|undefined),
 *   desc:string,
 *   code:string,
 * }}
 */
let SolutionFeature;

/**
 * The option object to initialize an object for the solution.
 * @typedef {{
 *   projectId:string,
 *   locationId:string,
 *   version:string,
 *   bucket:string,
 *   namespace:string,
 *   secretName:(string|undefined),
 * }}
 */
let SolutionOption;

/**
 * Default settings for Cloud Functions. In the detailed solution class, these
 * can be reused or overwritten.
 */
const CLOUD_FUNCTIONS_DEFAULT_SETTINGS = {
  runtime: 'nodejs18',
  timeout: '540s',
  availableMemoryMb: 2048,
  dockerRegistry: 'ARTIFACT_REGISTRY',
};

/**
 * Default env variables for Cloud Functions.
 */
const CLOUD_FUNCTIONS_DEFAULT_ENV = {
  DEBUG: 'false',
  IN_GCP: 'true',
};

/**
 * This class is the base of a prime solution that have Cloud Functions to be
 * deployed, e.g. Tentacles. It helps to create the source code to be deployed
 * and generate the source upload url of Cloud Functions.
 * @see https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions/generateUploadUrl
 * @abstract
 */
class PrimeSolution {

  /**
   * @param {!SolutionOption} options
   */
  constructor(options) {
    this.projectId = options.projectId;
    this.locationId = options.locationId || 'us-central1';
    this.version = options.version;
    this.bucket = options.bucket;
    this.databaseId = options.databaseId || DEFAULT_DATABASE;
    this.cloudFunctions = new CloudFunctions(this.projectId, this.locationId);
    this.secretName = options.secretName;
    const firestore = new Firestore(this.projectId, this.databaseId);
    const { type } = firestore.getDatabase();
    this.databaseMode = FIRESTORE_MODE_FOR_DAO[type];
  }

  /**
   * Returns the NodeJS package name.
   * @abstract
   */
  getPackageName() {
    throw new Error('Should be overwrittern in sub class.');
  }

  /**
   * Deploys Cloud Functions of this solution.
   * @abstract
   */
  deployCloudFunctions(name) {
    throw new Error(`Umimplemented BaseSolution: ${name}`);
  }

  /**
   * Deploys a Cloud Functions and returns the operation name.
   *
   * @param {string} functionName
   * @param {Object} options
   * @param {Object|undefined} envVariables
   * @return {string} The name of operation.
   */
  deploySingleCloudFunction(functionName, options, envVariables) {
    const payload = Object.assign({}, CLOUD_FUNCTIONS_DEFAULT_SETTINGS, {
      sourceUploadUrl: this.getSourceUploadUrl(),
      environmentVariables: this.getEnvironmentVariables(envVariables),
    }, this.getCloudFunctionConfig(), options);
    return this.cloudFunctions.createOrUpdate(functionName, payload);
  }

  /**
   * Returns the event trigger of Cloud Pub/Sub.
   * @see https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions#EventTrigger
   * @param {string} topic Pub/Sub topic name.
   * @return {!EventTrigger}
   */
  getPubSubEventTrigger(topic) {
    return {
      eventType: 'google.pubsub.topic.publish',
      resource: `projects/${this.projectId}/topics/${topic}`,
      service: 'pubsub.googleapis.com',
      failurePolicy: {},
    };
  }

  /**
   * Returns the event trigger of Cloud Storage.
   * @see https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions#EventTrigger
   * @param {string=} bucket Cloud Storage bucket name.
   * @return {!EventTrigger}
   */
  getStorageEventTrigger(bucket = this.bucket) {
    return {
      eventType: 'google.storage.object.finalize',
      resource: `projects/_/buckets/${bucket}`,
      service: 'storage.googleapis.com',
      failurePolicy: {},
    };
  }

  /**
   * Returns the content of `index.js` file.
   * @return {string}
   */
  getIndexFile() {
    return `// Copyright 2022 Google Inc.
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
 * @fileoverview Cloud Functions entry point file.
 * Generated by Cyborg automagically.
 */
'use strict';

Object.assign(module.exports, require('${this.getPackageName()}'));
`
  }

  /**
   * Returns `package.json` file content as a JSON string.
   * @return {string}
   */
  getPackageFile() {
    return JSON.stringify({
      name: `${this.constructor.name}-deployed-by-cyborg`,
      version: '0.0.0',
      author: 'Google Inc.',
      license: 'Apache-2.0',
      main: 'index.js',
      dependencies: {
        [this.getPackageName()]: this.getDeployVersion(),
      },
      'cloud-repo-tools': {
        requiresKeyFile: true,
        requiresProjectId: true,
      },
    });
  }

  /**
   * Returns the version of deployed package. If there is no `version` is given,
   * it will return the latest version.
   * @return {string}
   */
  getDeployVersion() {
    if (this.version) return this.version;
    this.version = getNpmVersions(this.getPackageName(), 1)[0];
    return this.version;
  }

  /**
   * Returns a signed URL for uploading a function source code.
   * Cloud Functions in the same solution can share the same URL.
   * @see https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions/generateUploadUrl
   * @return {string} The uploaded source URL.
   */
  getSourceUploadUrl() {
    if (this.sourceUploadUrl) return this.sourceUploadUrl;
    const files = [
      { file: 'index.js', content: this.getIndexFile() },
      { file: 'package.json', content: this.getPackageFile() },
    ]
    this.sourceUploadUrl = this.cloudFunctions.uploadSourceAndReturnUrl(files);
    return this.sourceUploadUrl;
  }

  /**
   * Gets the enviroment variables for Cloud Functions.
   * @param {object} variables Other env variables for Cloud Functions.
   * @return
   * @private
   */
  getEnvironmentVariables(variables = {}) {
    return Object.assign({}, CLOUD_FUNCTIONS_DEFAULT_ENV, {
      GCP_PROJECT: this.projectId,
      PROJECT_NAMESPACE: this.namespace,
      VERSION: this.getDeployVersion(),
      DATABASE_ID: this.databaseId,
      DATABASE_MODE: this.databaseMode,
    }, variables);
  }

  /**
   * Returns the env variable of the secret if it is offered.
   * @return {{SECRET_NAME: (string|undefined)}}
   */
  getSecretEnv() {
    return this.secretName ? { SECRET_NAME: this.secretName } : {};
  }

  /**
   * Returns the Cloud Functions config (memory, runtime) from the pakcage
   * definition file 'package.json'.
   * @return {object}
   */
  getCloudFunctionConfig() {
    const { config } =
      getNodePackageInfo(this.getPackageName(), this.getDeployVersion());
    return config;
  }

}

/**
 * Returns a function to get an array of description from the given array of
 * solution features.
 * @param {!Array<!SolutionFeature>} solutionFeatures
 * @return {function(!Array<string>|undefined):!Array<string>}
 */
PrimeSolution.getFeatureDesc = (solutionFeatures) => {
  return (enabledFeatures) => {
    return solutionFeatures
      .filter(({ code }) => !enabledFeatures || enabledFeatures.indexOf(code) > -1)
      .map(({ desc }) => desc);
  }
}

/**
 * Returns a function to get API(s) of a solution feature and ensure they are
 * enabled in the specified GCP project.
 * @param {!Array<!SolutionFeature>} solutionFeatures
 * @param {string} projectId
 * @return {function(string):(!CheckResult|undefined)}
 */
PrimeSolution.checkAndEnableApiForFeature = (solutionFeatures, projectId) => {
  return (feature) => {
    const features = solutionFeatures.filter(
      ({ desc }) => feature === desc
    );
    if (features.length === 0) {
      return {
        status: RESOURCE_STATUS.ERROR,
        message: `Couldn't find: ${feature}`,
      };
    }
    const serviceUsage = new ServiceUsage(projectId);
    const { api } = features[0];
    if (api) {
      const apiResult = [api].flat()
        .map((singleApi) => serviceUsage.checkOrEnableService(singleApi))
        .every((result) => result);
      if (!apiResult) {
        return {
          status: RESOURCE_STATUS.ERROR,
          message: `Can't enable API: ${api}`,
        };
      }
    }
  };
}
