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
})

/**
 * The list of available Cloud Storage locations.
 * @see https://cloud.google.com/storage/docs/locations#available-locations
 */
const REGION_NORTH_AMERICA = [
  { displayName: 'Montréal', locationId: 'northamerica-northeast1' },
  { displayName: 'Toronto', locationId: 'northamerica-northeast2' },
  { displayName: 'Iowa', locationId: 'us-central1' },
  { displayName: 'South Carolina', locationId: 'us-east1' },
  { displayName: 'Northern Virginia', locationId: 'us-east4' },
  { displayName: 'Columbus', locationId: 'us-east5' },
  { displayName: 'Dallas', locationId: 'us-south1' },
  { displayName: 'Oregon', locationId: 'us-west1' },
  { displayName: 'Los Angeles', locationId: 'us-west2' },
  { displayName: 'Salt Lake City', locationId: 'us-west3' },
  { displayName: 'Las Vegas', locationId: 'us-west4' },
];
const REGION_SOUTH_AMERICA = [
  { displayName: 'São Paulo', locationId: 'southamerica-east1' },
  { displayName: 'Santiago', locationId: 'southamerica-west1' },
];
const REGION_EUROPE = [
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
const REGION_ASIA = [
  { displayName: 'Taiwan', locationId: 'asia-east1' },
  { displayName: 'Hong Kong', locationId: 'asia-east2' },
  { displayName: 'Tokyo', locationId: 'asia-northeast1' },
  { displayName: 'Osaka', locationId: 'asia-northeast2' },
  { displayName: 'Seoul', locationId: 'asia-northeast3' },
  { displayName: 'Mumbai', locationId: 'asia-south1' },
  { displayName: 'Delhi', locationId: 'asia-south2' },
  { displayName: 'Singapore', locationId: 'asia-southeast1' },
  { displayName: 'Jakarta', locationId: 'asia-southeast2' },
];
const REGION_AUSTRALIA = [
  { displayName: 'Sydney', locationId: 'australia-southeast1' },
  { displayName: 'Melbourne', locationId: 'australia-southeast2' },
];
const DUAL_REGION = [
  { displayName: 'Iowa and South Carolina', locationId: 'nam4' },
  { displayName: 'Tokyo and Osaka', locationId: 'asia1' },
  { displayName: 'Finland and Netherlands', locationId: 'eur4' },
];
const MULTI_REGION = [
  { displayName: 'Asia', locationId: 'asia' },
  { displayName: 'European Union', locationId: 'eu' },
  { displayName: 'United States', locationId: 'us' },
];

class Storage extends ApiBase {

  constructor(projectId) {
    super();
    this.apiUrl = 'https://storage.googleapis.com/storage';
    this.version = 'v1';
    this.projectId = projectId;
  }

  /** @override */
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
    return super.get(`b/${bucket}`);
  }

  /**
   * Gets the state of the bucket.
   * @param {string} bucket
   * @return {!BUCKET_STATUS}
   */
  getBucketStatus(bucket) {
    const { error, projectNumber } = this.getBucket(bucket);
    if (error) {
      if (error.code === 404) return BUCKET_STATUS.NON_EXIST;
      if (error.code === 403) return BUCKET_STATUS.NO_ACCESS;
      console.error(`Unknow status of Storage bucket ${bucket}`, error);
      return BUCKET_STATUS.UNKNOWN;
    }
    const resourceManager = new CloudResourceManager(this.projectId);
    const { projectNumber: ownerProjectNumber } = resourceManager.getProject();
    return projectNumber === ownerProjectNumber ? BUCKET_STATUS.EXIST
      : BUCKET_STATUS.NOT_EXIST_IN_THIS_GCP;
  }

  /**
   * Creates a bucket for a Google Cloud Storage. The 'locationType' would be
   * determined based on the 'location', e.g. 'NAM4' for the Dual-Region of
   * 'Iowa and South Carolina', 'US' for the Multi-Region in US. By default, the
   * location is 'US-CENTRAL1'
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/insert
   * @param {string} name Bucket name
   * @param {Object} options
   * @return {!Bucket}
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets#resource
   */
  createBucket(name, options = {}) {
    const payload = Object.assign({
      name,
      storageClass: 'STANDARD',
      location: 'US-CENTRAL1',
    }, options);
    return this.mutate(`b?project=${this.projectId}`, payload);
  }

  /**
   * Updates a bucket.
   * @see https://cloud.google.com/storage/docs/json_api/v1/buckets/patch
   * @param {string} bucket
   * @param {Object} options
   * @return {!Bucket}
   */
  updateBucket(bucket, options = {}) {
    return this.mutate(`b/${bucket}`, options, 'PATCH');
  }

  /**
   * Uploads a file to the bucket. The uploaded object replaces any existing
   * object with the same name.
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects/insert
   * @param {string} fileName
   * @param {string} bucket
   * @param {string} content
   * @return
   * @see https://cloud.google.com/storage/docs/json_api/v1/objects#resource
   */
  uploadFile(fileName, bucket, content) {
    const apiUrl = 'https://storage.googleapis.com/upload/storage';
    const parameters = {
      uploadType: 'media',
      name: fileName,
    };
    const url = `${apiUrl}/${this.version}/b/${bucket}/o` +
      super.getQueryString(parameters);
    return this.mutate(url, content);
  }

}
