// Copyright 2026 Google Inc.
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

MojoFunctions.getBucketAndFilename_ = bucketAndFilename => {
  const regex = /^gs:\/\/([^\/]+)\/(.+)$/;
  const match = bucketAndFilename.match(regex);
  if (!match) {
    throw new Error(
      'The target storage file should in this format: gs://bucket/filename',
    );
  }
  const bucket = match[1];
  const fileName = match[2];
  return {bucket, fileName};
};

/** Checks the folder name. */
MojoFunctions.checkFolderName = folder => {
  const regex = /^\w+$/;
  const result = regex.test(folder);
  if (result) {
    return {status: RESOURCE_STATUS.OK};
  }
  const message = 'Expected a string of letters or digits.';
  return {status: RESOURCE_STATUS.ERROR, message};
};
/**
 * Checks Cloud Storage bucket. If it needs to be created or updated, it
 * returns a status `TO_APPLY` to ask users to continue with the menu
 * `Apply changes` to make those changes.
 * Used property: projectId, locationId.
 *
 * @param {string} bucket Bucket name.
 * @param {{attributeValue:(string|undefined)}} resource The `attributeValue`
 *   is the days of the object lifecycle of the bucket.
 * @return {!CheckResult}
 */
MojoFunctions.checkBucket = (bucket, resource) => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const storage = new Storage(projectId);
  const status = storage.getBucketStatus(bucket);
  const {attributeValue} = resource;
  const age = (attributeValue || 0) - 0;
  const targetLifecycle = age
    ? {rule: [{action: {type: 'Delete'}, condition: {age: age - 0}}]}
    : {rule: []};
  switch (status) {
    case BUCKET_STATUS.NON_EXIST:
      const lifecycleInfo = `Files will be automatically removed after ${age} days.`;
      return {
        status: RESOURCE_STATUS.TO_APPLY,
        message: `Will create bucket in [${locationId}]. ${
          age ? lifecycleInfo : ''
        }`,
      };
    case BUCKET_STATUS.EXIST:
      const result = {
        status: RESOURCE_STATUS.OK,
        status_link: storage.getLinkToBucket(bucket),
        message: '',
      };
      const {location, lifecycle} = storage.getBucket(bucket);
      if (age) {
        if (
          !lifecycle ||
          JSON.stringify(lifecycle) !== JSON.stringify(targetLifecycle)
        ) {
          result.status = RESOURCE_STATUS.TO_APPLY;
          result.message +=
            'Will update lifecycle rules based on the setting. ';
        }
      } else if (lifecycle) {
        result.status = RESOURCE_STATUS.TO_APPLY;
        result.message += 'Will remove lifecycle rules based on the setting. ';
      }
      if (location.toLowerCase() !== locationId.toLowerCase()) {
        result.message += `Bucket has a different location[${
          location
        }] to the Cloud Functions.`;
      }
      return result;
    default:
      return {
        status: RESOURCE_STATUS.ERROR,
        message: `Bucket[${bucket}] status ${status}. Change the name and retry`,
      };
  }
};

/**
 * Creates or updates the Cloud Storage bucket.
 * Used property: projectId, locationId.
 *
 * @param {string} bucket Bucket name.
 * @param {{attributeValue:(string|undefined)}} resource The `attributeValue`
 *   is the days of the object lifecycle of the bucket.
 * @return {!CheckResult}
 */
MojoFunctions.createOrUpdateBucket = (bucket, resource) => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const storage = new Storage(projectId);
  const status = storage.getBucketStatus(bucket);
  const {attributeValue} = resource;
  const age = (attributeValue || 0) - 0;
  const lifecycle = age
    ? {rule: [{action: {type: 'Delete'}, condition: {age}}]}
    : {rule: []};
  let response;
  if (status === BUCKET_STATUS.NON_EXIST) {
    const options = {location: locationId, lifecycle};
    response = storage.createBucket(bucket, options);
  } else {
    response = storage.updateBucket(bucket, {lifecycle});
  }
  const {error} = response;
  if (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
  return {
    status: RESOURCE_STATUS.OK,
    status_link: storage.getLinkToBucket(bucket),
    message: `Bucket[${bucket}] was created/updated.`,
  };
};

/**
 * Checks if the target GCS file exists and matches the MD5 checksum of
 * EMBEDDED_SOURCE[attributeValue]. If the file is missing or has updates,
 * uploads it to Google Cloud Storage.
 *
 * @param {string} bucketAndName Target file name (e.g. 'gs://bucket/data.csv' ).
 * @param {!Object} resources MOJO element resources containing attributeValue.
 * @return {!CheckResult} Standardized status, descriptive outcome message, and
 *     console link.
 */
MojoFunctions.checkOrUploadFileToGcs = (bucketAndName, resources) => {
  const {bucket, fileName} = MojoFunctions.getBucketAndFilename_(bucketAndName);
  const projectId = getDocumentProperty('projectId');
  const embeddedKey = resources.attributeValue;
  const rawData = EMBEDDED_SOURCE ? EMBEDDED_SOURCE[embeddedKey] : '';
  if (!rawData) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: `Fail to find expected source code as ${embeddedKey}`,
    };
  }
  let blob;
  let bytes;

  // If fileName is a zip or rawData is a base64-encoded zip
  if (
    fileName.toLowerCase().endsWith('.zip') ||
    (typeof rawData === 'string' && rawData.startsWith('UEsDB'))
  ) {
    try {
      bytes = Utilities.base64Decode(rawData);
      blob = Utilities.newBlob(bytes, 'application/zip', fileName);
    } catch (e) {
      blob = Utilities.newBlob(rawData, 'application/zip', fileName);
      bytes = blob.getBytes();
    }
  } else {
    blob = Utilities.newBlob(rawData, 'text/plain', fileName);
    bytes = blob.getBytes();
  }
  // Compute local MD5 hash (Base64-encoded digest matching GCS md5Hash)
  const localMd5 = Utilities.base64Encode(
    Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, bytes),
  );

  const storage = new Storage(projectId);
  if (storage.getBucketStatus(bucket) === BUCKET_STATUS.NON_EXIST) {
    return {
      status: RESOURCE_STATUS.PENDING,
      message: `Bucket [${bucket}] is not ready yet.`,
    };
  }
  const statusLink = storage.getLinkToFile(bucket, fileName);

  try {
    const remoteMeta = storage.getObjectMetadata(bucket, fileName);
    if (!remoteMeta.error && remoteMeta.md5Hash === localMd5) {
      return {
        status: RESOURCE_STATUS.OK,
        message: `File [${fileName}] on gs://${bucket} is up-to-date (MD5: ${
          localMd5
        }).`,
        status_link: statusLink,
      };
    }
    const uploadResult = storage.uploadBlob(fileName, bucket, blob);
    if (uploadResult.error) {
      return {
        status: RESOURCE_STATUS.ERROR,
        message: `Failed to upload [${fileName}] to gs://${bucket}: ${
          uploadResult.error.message || JSON.stringify(uploadResult.error)
        }`,
        status_link: statusLink,
      };
    }
    return {
      status: RESOURCE_STATUS.OK,
      message: `Uploaded [${fileName}] to gs://${bucket} (${bytes.length} bytes).`,
      status_link: statusLink,
    };
  } catch (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: `Failed to check or upload [${fileName}] to gs://${bucket}: ${
        error.message || error.toString()
      }`,
      status_link: statusLink,
    };
  }
};

MojoTemplateRegistry.register('cloudStorage', {
  category: 'Google Cloud',
  resource: 'Cloud Storage',
  value: '${namespace}-${projectId|normalized}',
  attributeName: 'Lifecycle (days before delete)',
  propertyName: 'bucket',
  checkFn: MojoFunctions.checkBucket,
  enableFn: MojoFunctions.createOrUpdateBucket,
});

MojoTemplateRegistry.register('storageFile', {
  category: 'Google Cloud',
  resource: 'Cloud Storage File',
  attributeName: 'Embedded',
  attributeValue: 'SOURCE_CODE',
  checkFn: MojoFunctions.checkOrUploadFileToGcs,
});
