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

/** @fileoverview Cloud Run Functions API handler class.*/

/**
 * The generation of the Cloud Functions.
 * @see https://cloud.google.com/run/docs/functions/comparison
 * @enum {string}
 */
const CLOUD_FUNCTIONS_GENERATION = Object.freeze({
  GEN_1: 'GEN_1',
  GEN_2: 'GEN_2',
});

const DEFAULT_GENERATION = CLOUD_FUNCTIONS_GENERATION.GEN_2;
/**
 * Default settings for Cloud Functions. In the detailed solution class, these
 * can be reused or overwritten.
 */
const CLOUD_FUNCTIONS_DEFAULT_SETTINGS = {
  generation: DEFAULT_GENERATION,
  runtime: 'nodejs22',
  timeout: 540,
  availableMemory: '2048M',
};

class CloudRunFunction extends GcpApiBase {
  /** @return {string} The canonical GCP asset type for this service. */
  static get ASSET_TYPE() {
    return 'cloudfunctions.googleapis.com/Function';
  }

  /**
   * @param {string=} projectId
   * @param {string=} locationId
   */
  constructor(projectId, locationId) {
    super();
    this.name = 'Cloud Functions API';
    this.api = 'cloudfunctions.googleapis.com';
    this.permissions = [
      'cloudfunctions.functions.create',
      'cloudfunctions.functions.get',
    ];
    this.adminRole = 'Cloud Functions Admin';
    this.runtimeRoles = [
      'roles/cloudfunctions.invoker',
      'roles/eventarc.eventReceiver',
    ];
    this.companionApis = [
      {name: 'Eventarc API', api: 'eventarc.googleapis.com'},
      {name: 'Cloud Build API', api: 'cloudbuild.googleapis.com'},
    ];
    this.apiUrl = `https://${this.api}`;
    this.version = 'v2';
    this.projectId = projectId;
    this.locationId = locationId;
  }

  /**
   * @override
   * @return {string}
   */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  // /**
  //  * Lists information about the supported locations for this service.
  //  * @see https://cloud.google.com/functions/docs/reference/rest/v2/projects.locations/list
  //  * @return {!Array<!Location>} An array of resources that represents Google
  //  *   Cloud Platform location.
  //  * @see https://cloud.google.com/functions/docs/reference/rest/Shared.Types/ListLocationsResponse#Location
  //  */
  // listLocations() {
  //   // const locations = this.list(
  //   //   'locations',
  //   //   `projects/${this.projectId}/locations`,
  //   // );
  //   // return locations;
  //   return super.listLocations('v1');
  // }

  /**
   * Returns the Service Account email configured for a Cloud Run function.
   * If no custom service account was set, falls back to the project's
   * default Compute Engine service account.
   * Throws an error if the function cannot be found.
   * @param {string} functionName Name or ID of the Cloud Run function.
   * @return {string} Service Account email string.
   */
  getServiceAccount(functionName) {
    const response = this.getFunction(functionName);
    if (!response || response.error) {
      const errMsg =
        response && response.error ? response.error.message : 'Not found';
      throw new Error(
        `Cloud Run Function "${functionName}" not found: ${errMsg}`,
      );
    }
    if (response.serviceConfig && response.serviceConfig.serviceAccountEmail) {
      return response.serviceConfig.serviceAccountEmail;
    }
    console.log(
      'No serviceAccount found for Cloud Run Function',
      functionName,
      'Falling back to default Compute Engine service account...',
    );
    const resourceManager = new CloudResourceManager(this.projectId);
    return resourceManager.getDefaultComputeServiceAccount();
  }

  /**
   * Returns the primary endpoint HTTPS URL for a Cloud Run function by name.
   * Throws an error if the function or its URL cannot be resolved.
   * @param {string} functionName Name or ID of the Cloud Run function.
   * @return {string} The public HTTPS URL of the function.
   */
  getServiceUrl(functionName) {
    const response = this.getFunction(functionName);
    if (!response || response.error) {
      const errMsg =
        response && response.error ? response.error.message : 'Not found';
      throw new Error(
        `Cloud Run Function "${functionName}" not found: ${errMsg}`,
      );
    }
    const url =
      (response.serviceConfig && response.serviceConfig.uri) || response.url;
    if (!url) {
      throw new Error(
        `URL for Cloud Run Function "${functionName}" could not be resolved.`,
      );
    }
    return url;
  }

  /**
   * Returns a function with the given name from the requested project.
   * @see https://cloud.google.com/functions/docs/reference/rest/v2/projects.locations.functions/get
   * @param {string} functionName
   * @return {!Function} See:
   *     https://cloud.google.com/functions/docs/reference/rest/v2/projects.locations.functions#Function
   */
  getFunction(functionName) {
    return this.get(
      `projects/${this.projectId}/locations/${this.locationId}/functions/${
        functionName
      }`,
    );
  }

  /**
   * Returns whether a function with the given name exists.
   * @param {string} functionName
   * @return {boolean}
   */
  exist(functionName) {
    const response = this.getFunction(functionName);
    if (response.name) return true;
    if (response.error.code === 404) return false;
    console.error(
      `Unknown status of Cloud Functions ${functionName}`,
      response,
    );
    throw new Error(`Unknown status of Cloud Functions ${functionName}`);
  }

  /**
   * Returns the primary endpoint HTTPS URL for a Cloud Run function by name.
   * Throws an error if the service or its URL cannot be resolved.
   * @param {string} functionName Name or ID of the Cloud Run service.
   * @return {string} The public HTTPS URL of the service.
   */
  getServiceUrl(functionName) {
    const response = this.getFunction(functionName);
    if (!response || response.error) {
      const errMsg =
        response && response.error ? response.error.message : 'Not found';
      throw new Error(
        `Cloud Run Function "${functionName}" not found: ${errMsg}`,
      );
    }
    if (response.serviceConfig && response.serviceConfig.uri) {
      return response.serviceConfig.uri;
    }
    throw new Error(`No uri found for Cloud Run Function "${functionName}"`);
  }

  /**
   * Returns a list of functions that belong to the requested project and
   * location.
   * @see https://cloud.google.com/functions/docs/reference/rest/v2/projects.locations.functions/list
   * @param {string=} locationId
   * @return {{functions:!Array<!CloudFunction>}}
   */
  listFunctions(locationId = this.locationId) {
    const functions = this.list(
      'functions',
      `projects/${this.projectId}/locations/${locationId}/functions`,
    );
    return functions;
  }

  /**
   * Returns a list of functions that belong to the requested project and
   * all locations.
   * @see https://cloud.google.com/functions/docs/reference/rest/v2/projects.locations.functions/list
   * @return {{functions:!Array<!CloudFunction>}}
   */
  listFunctionsForAllLocations() {
    return this.listFunctions('-');
  }

  /**
   * Creates a new function.
   * @see https://cloud.google.com/functions/docs/reference/rest/v2/projects.locations.functions/create
   * @param {string} functionName
   * @param {!CloudFunction} payload
   * @return {!Operation}
   * @see https://cloud.google.com/functions/docs/reference/rest/Shared.Types/ListOperationsResponse#Operation
   */
  createFunction(functionName, payload) {
    return this.mutate(
      `projects/${this.projectId}/locations/${
        this.locationId
      }/functions?functionId=${functionName}`,
      payload,
    );
  }

  /**
   * Updates existing function.
   * @see https://cloud.google.com/functions/docs/reference/rest/v2/projects.locations.functions/patch
   * @param {string} functionName
   * @param {!CloudFunction} payload
   * @return {!Operation}
   */
  updateFunction(functionName, payload) {
    return this.mutate(
      `projects/${this.projectId}/locations/${this.locationId}/functions/${
        functionName
      }`,
      payload,
      'PATCH',
    );
  }

  /**
   * Creates or updates the function.
   * @param {string} functionName
   * @param {!CloudFunction} payload
   * @return {string} The name of operation.
   */
  createOrUpdate(functionName, payload) {
    let response;
    if (this.exist(functionName)) {
      response = this.updateFunction(functionName, payload);
    } else {
      console.log('Not exist, now create', functionName);
      payload.name = `projects/${this.projectId}/locations/${
        this.locationId
      }/functions/${functionName}`;
      response = this.createFunction(functionName, payload);
    }
    if (response.error) throw new Error(response.error.message);
    return response.name;
  }

  /**
   * Returns a signed URL for uploading a function source code.
   * @see https://cloud.google.com/functions/docs/reference/rest/v2/projects.locations.functions/generateUploadUrl
   * @return {{
   *   uploadUrl: string,
   *   storageSource: {
   *     bucket: string,
   *     object: string,
   *   }
   * }}
   */
  generateUploadUrl() {
    return this.mutate(
      `projects/${this.projectId}/locations/${
        this.locationId
      }/functions:generateUploadUrl`,
    );
  }

  /**
   * Uploads Cloud Functions source code and returns the 'StorageSource' object
   * for creating or updating the Cloud Function.
   * @see https://cloud.google.com/functions/docs/reference/rest/v2/projects.locations.functions#StorageSource
   * @param {!Array<string,string>} files A map of file name and content.
   * @return {{storageSource:!StorageSource}}
   */
  uploadSource(files) {
    const {uploadUrl, storageSource, error} = this.generateUploadUrl();
    if (error) {
      console.log(error);
      throw error;
    }
    const blobs = files.map(({file, content}) => {
      return Utilities.newBlob(content, 'text/plain', file);
    });
    UrlFetchApp.fetch(uploadUrl, {
      method: 'put',
      payload: Utilities.zip(blobs),
      contentType: 'application/zip',
    });
    return {storageSource};
  }

  /**
   * Returns a signed URL for downloading deployed function source code.
   * @see https://cloud.google.com/functions/docs/reference/rest/v2/projects.locations.functions/generateDownloadUrl
   * @param {string} functionName
   * @return {{downloadUrl: string}}
   */
  generateDownloadUrl(functionName) {
    return this.mutate(
      `projects/${this.projectId}/locations/${this.locationId}/functions/${
        functionName
      }:generateDownloadUrl`,
    );
  }

  /**
   * Returns the array of source code files of the specified Cloud Function.
   * @param {string} functionName
   * @return {{error:!Object}|!Array<!Blob>}}
   */
  getSourceCode(functionName) {
    const {downloadUrl, error} = this.generateDownloadUrl(functionName);
    if (error) return {error};
    const blob = UrlFetchApp.fetch(downloadUrl).getBlob();
    return Utilities.unzip(blob);
  }

  /**
   * Gets the result of an operation.
   * @see https://cloud.google.com/functions/docs/reference/rest/v2/operations/get
   * @param {string} operationName
   * @return {!Operation}
   */
  getOperation(operationName) {
    return this.get(operationName);
  }

  /**
   * Deletes a function with the given name.
   * @see https://cloud.google.com/functions/docs/reference/rest/v2/projects.locations.functions/delete
   * @param {string} functionName
   * @return {boolean}
   */
  deleteFunction(functionName) {
    let response = this.mutate(
      `projects/${this.projectId}/locations/${this.locationId}/functions/${
        functionName
      }`,
      undefined,
      'DELETE',
    );
    while (!response.error && !response.done) {
      const operation = response.name;
      Utilities.sleep(5000);
      console.log('After 5s, check deleting process', operation);
      response = this.getOperation(operation);
    }
    if (response.error) {
      throw error;
    }
    return response.done;
  }
}

/**
 * Returns a link to the Cloud Function in Google Cloud Console.
 * @param {!Object} response
 * @return {string}
 */
function getLinkToCloudFunction(response) {
  // name: projects/PROJECT_ID/locations/LOCATION_ID/functions/FUNCTION_NAME
  const {name, environment: generation} = response;
  const configs = name.split('/');
  const functionName =
    generation === CLOUD_FUNCTIONS_GENERATION.GEN_1
      ? configs[5]
      : configs[5].replaceAll('_', '-');
  const path =
    generation === CLOUD_FUNCTIONS_GENERATION.GEN_1
      ? 'functions/details'
      : 'run/detail';
  const link =
    'https://console.cloud.google.com/' +
    `${path}/${configs[3]}/${functionName}?project=${configs[1]}`;
  return link;
}

if (typeof ServiceProviderRegistry !== 'undefined') {
  ServiceProviderRegistry.register(CloudRunFunction);
}
