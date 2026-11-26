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

/** @fileoverview Cloud Storage JSON API handler class.*/

/**
 * The status of a Cloud Storage bucket.
 * @enum {string}
 */
const BUCKET_STATUS = Object.freeze({
  EXIST: 'EXIST',
  NON_EXIST: 'NON_EXIST',
  NOT_EXIST_IN_THIS_GCP: 'NOT_EXIST_IN_THIS_GCP',
  NO_ACCESS: 'NO_ACCESS',
  UNKNOWN: 'UNKNOWN',
});

/**
 * The list of available Cloud Storage locations.
 * @see https://cloud.google.com/storage/docs/locations#available-locations
 */
const REGION_NORTH_AMERICA = [
  {displayName: 'Montréal', locationId: 'northamerica-northeast1'},
  {displayName: 'Toronto', locationId: 'northamerica-northeast2'},
  {displayName: 'Iowa', locationId: 'us-central1'},
  {displayName: 'South Carolina', locationId: 'us-east1'},
  {displayName: 'Northern Virginia', locationId: 'us-east4'},
  {displayName: 'Columbus', locationId: 'us-east5'},
  {displayName: 'Dallas', locationId: 'us-south1'},
  {displayName: 'Oregon', locationId: 'us-west1'},
  {displayName: 'Los Angeles', locationId: 'us-west2'},
  {displayName: 'Salt Lake City', locationId: 'us-west3'},
  {displayName: 'Las Vegas', locationId: 'us-west4'},
];
const REGION_SOUTH_AMERICA = [
  {displayName: 'São Paulo', locationId: 'southamerica-east1'},
  {displayName: 'Santiago', locationId: 'southamerica-west1'},
];
const REGION_EUROPE = [
  {displayName: 'Warsaw', locationId: 'europe-central2'},
  {displayName: 'Finland', locationId: 'europe-north1'},
  {displayName: 'Madrid', locationId: 'europe-southwest1'},
  {displayName: 'Belgium', locationId: 'europe-west1'},
  {displayName: 'London', locationId: 'europe-west2'},
  {displayName: 'Frankfurt', locationId: 'europe-west3'},
  {displayName: 'Netherlands', locationId: 'europe-west4'},
  {displayName: 'Zürich', locationId: 'europe-west6'},
  {displayName: 'Milan', locationId: 'europe-west8'},
  {displayName: 'Paris', locationId: 'europe-west9'},
];
const REGION_ASIA = [
  {displayName: 'Taiwan', locationId: 'asia-east1'},
  {displayName: 'Hong Kong', locationId: 'asia-east2'},
  {displayName: 'Tokyo', locationId: 'asia-northeast1'},
  {displayName: 'Osaka', locationId: 'asia-northeast2'},
  {displayName: 'Seoul', locationId: 'asia-northeast3'},
  {displayName: 'Mumbai', locationId: 'asia-south1'},
  {displayName: 'Delhi', locationId: 'asia-south2'},
  {displayName: 'Singapore', locationId: 'asia-southeast1'},
  {displayName: 'Jakarta', locationId: 'asia-southeast2'},
];
const REGION_AUSTRALIA = [
  {displayName: 'Sydney', locationId: 'australia-southeast1'},
  {displayName: 'Melbourne', locationId: 'australia-southeast2'},
];
const DUAL_REGION = [
  {displayName: 'Iowa and South Carolina', locationId: 'nam4'},
  {displayName: 'Tokyo and Osaka', locationId: 'asia1'},
  {displayName: 'Finland and Netherlands', locationId: 'eur4'},
];
const MULTI_REGION = [
  {displayName: 'Asia', locationId: 'asia'},
  {displayName: 'European Union', locationId: 'eu'},
  {displayName: 'United States', locationId: 'us'},
];

class Storage extends ApiBase {
  /**
   * @param {string=} projectId
   */
  constructor(projectId) {
    super();
    this.name = 'Cloud Storage JSON API';
    this.api = 'storage.googleapis.com';
    this.permissions = ['storage.buckets.create'];
    this.adminRole = 'Storage Admin';
    this.runtimeRoles = ['roles/storage.objectAdmin'];
    this.apiUrl = `https://${this.api}/storage`;
    this.version = 'v1';
    this.projectId = projectId;
  }

  /**
   * @override
   * @return {string}
   */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /**
   * Gets information about an application.
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/get
   * @param {string} bucket
   * @return {!Buckets}
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets#resource
   */
  getBucket(bucket) {
    return this.get(`b/${bucket}`);
  }

  /**
   * Gets the state of the bucket.
   * @param {string} bucket
   * @return {!BUCKET_STATUS}
   */
  getBucketStatus(bucket) {
    const {error, projectNumber} = this.getBucket(bucket);
    if (error) {
      if (error.code === 404) return BUCKET_STATUS.NON_EXIST;
      if (error.code === 403) return BUCKET_STATUS.NO_ACCESS;
      console.error(`Unknown status of Storage bucket ${bucket}`, error);
      return BUCKET_STATUS.UNKNOWN;
    }
    const resourceManager = new CloudResourceManager(this.projectId);
    const {projectNumber: ownerProjectNumber} = resourceManager.getProject();
    return projectNumber === ownerProjectNumber
      ? BUCKET_STATUS.EXIST
      : BUCKET_STATUS.NOT_EXIST_IN_THIS_GCP;
  }

  /**
   * Creates a bucket for a Google Cloud Storage. The 'locationType' would be
   * determined based on the 'location', e.g. 'NAM4' for the Dual-Region of
   * 'Iowa and South Carolina', 'US' for the Multi-Region in US. By default, the
   * location is 'US-CENTRAL1'
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/insert
   * @param {string} name Bucket name
   * @param {!Object=} options
   * @return {!Bucket}
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets#resource
   */
  createBucket(name, options = {}) {
    const payload = Object.assign(
      {
        name,
        storageClass: 'STANDARD',
        location: 'US-CENTRAL1',
      },
      options,
    );
    return this.mutate(`b?project=${this.projectId}`, payload);
  }

  /**
   * Updates a bucket.
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/patch
   * @param {string} bucket
   * @param {!Object=} options
   * @return {!Bucket}
   */
  updateBucket(bucket, options = {}) {
    return this.mutate(`b/${bucket}`, options, 'PATCH');
  }

  /**
   * Retrieves object metadata.
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/get
   * @param {string} bucket
   * @param {string} object
   * @return {!Object}
   */
  getObjectMetadata(bucket, object) {
    return this.get(`b/${bucket}/o/${encodeURIComponent(object)}`);
  }

  /**
   * Gets the url to uploads a file to the bucket.
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/insert
   * @param {string} bucket
   * @param {string} fileName
   * @return {string}
   */
  getUploadUrl_(bucket, fileName) {
    const apiUrl = 'https://storage.googleapis.com/upload/storage';
    const parameters = {
      uploadType: 'media',
      name: fileName,
    };
    return (
      `${apiUrl}/${this.version}/b/${bucket}/o` +
      this.getQueryString(parameters)
    );
  }

  /**
   * Uploads a file to the bucket. The uploaded object replaces any existing
   * object with the same name.
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/insert
   * @param {string} fileName
   * @param {string} bucket
   * @param {string} content
   * @return {!Object}
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects#resource
   */
  uploadFile(fileName, bucket, content) {
    const url = this.getUploadUrl_(bucket, fileName);
    return this.mutate(url, content);
  }

  /**
   * Uploads a zip file based on the content.
   * Use case: Docker storage source can only be a zip file. So a zip file will
   * be created with the given content and the fileName. In the zip file, the
   * file has no path information, but just the file name. The file path is used
   * to upload the zip file.
   *
   * @param {string} fileName
   * @param {string} bucket
   * @param {string} content
   * @return {!Object}
   */
  uploadZip(fileName, bucket, content) {
    const data = content.split('').map(c => c.charCodeAt(0));
    const blob = Utilities.newBlob(data);
    const file = fileName.substring(
      fileName.lastIndexOf('/') + 1,
      fileName.toLowerCase().lastIndexOf('.zip'),
    );
    blob.setName(file);
    const zipContent = Utilities.zip([blob]);

    const url = this.getUploadUrl_(bucket, fileName);
    const param = this.getFetchAppParams();
    param.method = 'POST';
    param.contentType = 'application/zip';
    param.payload = zipContent;
    const response = this.fetchAndReturnRawResponse(url, param);
    const result = JSON.parse(response.getContentText() || '{}');
    return result;
  }

  /**
   * Uploads a raw Blob directly to Cloud Storage.
   * @param {string} fileName Object path in bucket.
   * @param {string} bucket Bucket name.
   * @param {!GoogleAppsScript.Base.Blob} blob Blob to upload.
   * @return {!Object}
   */
  uploadBlob(fileName, bucket, blob) {
    const url = this.getUploadUrl_(bucket, fileName);
    const param = this.getFetchAppParams();
    param.method = 'POST';
    param.contentType = blob.getContentType() || 'application/octet-stream';
    param.payload = blob.getBytes();
    const response = this.fetchAndReturnRawResponse(url, param);
    const result = JSON.parse(response.getContentText() || '{}');
    return result;
  }

  /**
   * Downloads a file from Google Cloud Storage given its gs:// URI.
   *
   * @param {string} gsUri - The Google Cloud Storage URI (e.g.,
   *     gs://bucket/path/to/object).
   * @param {number} [sizeLimit] - Optional maximum number of bytes to download.
   * @returns {!GoogleAppsScript.URL_Fetch.HTTPResponse|!Object} The raw HTTP
   *     response object.
   * @throws {Error} If the gsUri is malformed or if the fetch operation fails.
   */
  downloadFile(gsUri, sizeLimit) {
    const match = gsUri.match(/^gs:\/\/([^\/]+)\/(.+)$/);
    if (!match) {
      throw new Error(`Malformed gs:// URI: ${gsUri}`);
    }
    const bucket = match[1];
    const objectPath = match[2];
    const url =
      this.getBaseUrl() +
      `/b/${bucket}/o/${encodeURIComponent(objectPath)}?alt=media`;
    try {
      const param = this.getFetchAppParams();
      param.method = 'GET';
      if (sizeLimit) {
        param.headers.Range = `bytes=0-${sizeLimit - 1}`;
      }
      const response = this.fetchAndReturnRawResponse(url, param);
      return response;
    } catch (e) {
      console.log('Failed to fetch GCS file: ' + e.toString());
      throw e;
    }
  }

  /**
   * Returns a list of buckets for a given project.
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/list
   * @return {!Array<!Bucket>}
   */
  listBucket() {
    const buckets = this.list('items', 'b', {project: this.projectId});
    return buckets;
  }

  /**
   * Returns a link to the Cloud Storage bucket in Google Cloud Console.
   * @param {string} bucket
   * @return {string}
   */
  getLinkToBucket(bucket) {
    const link = `https://console.cloud.google.com/storage/browser/${bucket}`;
    return link;
  }

  /**
   * Returns a link to the file on Cloud Storage bucket.
   * @param {*} bucket
   * @param {*} fileName
   * @returns {string}
   */
  getLinkToFile(bucket, fileName) {
    const link = `https://console.cloud.google.com/storage/browser/_details/${
      bucket
    }/${fileName}`;
    return link;
  }
}
