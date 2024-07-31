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

/** @fileoverview Cloud Service Usage API handler class.*/

/**
 * List of Google Cloud services' name and API. This list is used enable APIs
 * when related servcies are required.
 * `gcloud services list --filter "NAME~^[^.]+.googleapis.com$"`
 */
const GOOGLE_CLOUD_SERVICE_LIST = {
  'Cloud Firestore API': 'firestore.googleapis.com',
  'Cloud Functions API': 'cloudfunctions.googleapis.com',
  'Cloud Pub/Sub API': 'pubsub.googleapis.com',
  'Cloud Scheduler API': 'cloudscheduler.googleapis.com',
  'Cloud Build API': 'cloudbuild.googleapis.com',
  'Artifact Registry API': 'artifactregistry.googleapis.com',
  'BigQuery API': 'bigquery.googleapis.com',
  'BigQuery Data Transfer API': 'bigquerydatatransfer.googleapis.com',
  'Google Sheets API': 'sheets.googleapis.com',
  'Google Drive API': 'drive.googleapis.com',
  'IAM Service Account Credentials API': 'iamcredentials.googleapis.com',
  'Secret Manager API': 'secretmanager.googleapis.com',
  'Vertex AI API': 'aiplatform.googleapis.com',
  'Generative Language API': 'generativelanguage.googleapis.com',
  'API Keys API': 'apikeys.googleapis.com',
};

class ServiceUsage extends ApiBase {

  constructor(projectId) {
    super();
    this.apiUrl = 'https://serviceusage.googleapis.com';
    this.version = 'v1';
    this.projectId = projectId;
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /**
   * Returns the service configuration and enabled state for a given service.
   * @see https://cloud.google.com/service-usage/docs/reference/rest/v1/services/get
   * @param {string} service
   * @return {!Service}
   * @see https://cloud.google.com/service-usage/docs/reference/rest/v1/services#Service
   */
  getService(service) {
    return super.get(`projects/${this.projectId}/services/${service}`);
  }

  /**
   * Enables a service so that it can be used with a project.
   * @see https://cloud.google.com/service-usage/docs/reference/rest/v1/services/enable
   * @param {string} service
   * @return {!Operation} Examples:
   *  {
   *    name: 'operations/noop.DONE_OPERATION',
   *    done: true,
   *    response: {
   *      '@type': 'type.googleapis.com/google.api.serviceusage.v1.EnableServiceResponse',
   *      service: {
   *        state: 'ENABLED',
   *      }
   *    }
   *  }
   * OR:
   *  {
   *    name: 'operations/acf.p2-306683888052-7254ecf4-9b49-4fee-8f7b-0e72f8d8d40a'
   *  }
   * @see https://cloud.google.com/service-usage/docs/reference/rest/Shared.Types/ListOperationsResponse#Operation
   */
  enableService(service) {
    return this.mutate(`projects/${this.projectId}/services/${service}:enable`);
  }

  /**
   * Gets the latest state of a long-running operation.
   * @see https://cloud.google.com/service-usage/docs/reference/rest/v1/operations/get
   * @param {string} operationName
   * @return {!Operation}
   * @see https://cloud.google.com/service-usage/docs/reference/rest/Shared.Types/ListOperationsResponse#Operation
   */
  getOperation(operationName) {
    return super.get(operationName);
  }

  /**
   * Checks the state for a given service and enables it if it is disabled.
   * @param {string} service
   * @return {boolean} Whether the service is enabled.
   */
  checkOrEnableService(service) {
    const { state } = this.getService(service);
    if (state === 'ENABLED') return true;
    let rawResponse = this.enableService(service);
    while (rawResponse.done !== true && !rawResponse.error) {
      Utilities.sleep(5000);
      rawResponse = this.getOperation(rawResponse.name);
      console.log('Wait and get operation', rawResponse.name);
    }
    console.log('Enable API response', rawResponse);
    const { response, error } = rawResponse;
    if (error) {
      console.error(error);
      return false;
    }
    return response.service.state === 'ENABLED';
  }

}
