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

/**
 * Gets the Cloud Storage bucket and file name from the given configuration.
 * 1. If it is a gsutil path, then get bucket and file name from it directly;
 * 2. If it starts with the default bucket, then get the file name after the
 *    bucket;
 * 3. Otherwise, use the default bucket as 'bucket' and the configuration item
 *    as file name.
 * @param {string} storageSourceRaw
 * @return {{
 *   sourceBucket: string,
 *   sourceObject: string,
 * }}
 */
MojoFunctions.getStorageSource_ = storageSourceRaw => {
  const properties = getDocumentProperties();
  const storageSource = replaceVariables(storageSourceRaw, properties);
  const {bucket} = properties;
  let sourceBucket, sourceObject;
  if (storageSource.startsWith('gs://') || storageSource.startsWith(bucket)) {
    const regex = /^(?:gs:\/\/)?([^/]+)\/(.+)$/;
    const match = storageSource.match(regex);
    sourceBucket = match[1];
    sourceObject = match[2];
  } else {
    sourceBucket = bucket;
    sourceObject = storageSource;
  }
  return {sourceBucket, sourceObject};
};

/**
 * Checks whether the docker image has been created. If it was created, it will
 * also check if it is newer than the configure file. If not, it will allow to
 * recreate the image.
 *
 * @param {string} imageRepoAndName
 * @param {!Object} resource
 * @return {!CheckResult}
 */
MojoFunctions.checkDockerImage = (imageRepoAndName, resource) => {
  if (!imageRepoAndName) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: 'Input the Docker image name and tag',
    };
  }
  const {attributesMap} = resource;
  const attributeNames = CloudBuild.ATTRIBUTE_NAMES;
  const storageSourceRaw =
    attributesMap[attributeNames.storageSource].attributeValue;
  if (!storageSourceRaw) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: 'Input the file path to this resource in Cloud Storage',
    };
  }
  const properties = getDocumentProperties();
  const {projectId, locationId} = properties;
  const {repositoryId, imageNameAndLabel} =
    MojoFunctions.getRepoAndImageName_(imageRepoAndName);
  const {sourceBucket, sourceObject} =
    MojoFunctions.getStorageSource_(storageSourceRaw);

  const storage = new Storage(projectId);
  const bucketStatus = storage.getBucketStatus(sourceBucket);
  if (bucketStatus === BUCKET_STATUS.NON_EXIST) {
    return {
      status: RESOURCE_STATUS.PENDING,
      message: `Bucket '${sourceBucket}' is not ready yet.`,
    };
  }
  if (bucketStatus !== BUCKET_STATUS.EXIST) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: `Can not find bucket named '${sourceBucket}' (status: ${bucketStatus})`,
    };
  }
  const file = storage.getObjectMetadata(sourceBucket, sourceObject);
  if (file.error) {
    if (file.error.code === 404 || file.error.status === 'NOT_FOUND') {
      return {
        status: RESOURCE_STATUS.PENDING,
        message: `Source file '${sourceObject}' in bucket '${sourceBucket}' is not ready yet.`,
      };
    }
    return {
      status: RESOURCE_STATUS.ERROR,
      message: file.error.message,
    };
  }
  const {updated: configUpdated} = file;

  const registry = new ArtifactRegistry(projectId, locationId);
  try {
    const image = registry.getDockerImage(repositoryId, imageNameAndLabel);
    if (!image) {
      return {
        status: RESOURCE_STATUS.TO_APPLY,
        message: 'Not built yet.',
      };
    } else {
      const {updateTime, uri, tags = []} = image;
      const result = {};
      if (new Date(updateTime) - new Date(configUpdated) < 0) {
        result.status = RESOURCE_STATUS.TO_APPLY;
        result.message = `The image[${updateTime}] is older than the configuration file[${
          configUpdated
        }]. Will build a new one.`;
      } else {
        const expectedDockerFile =
          attributesMap[attributeNames.dockerFile]?.attributeValue ||
          'DockerFile';
        const expectedBaseImage =
          attributesMap[attributeNames.baseImage]?.attributeValue || '';
        const expectedConfigTag = CloudBuild.getConfigTag(
          expectedDockerFile,
          expectedBaseImage,
        );
        if (!tags.includes(expectedConfigTag)) {
          const currentConfigTag =
            tags.find(t => t.startsWith(CloudBuild.CONFIG_TAG_PREFIX)) ||
            'untagged';
          result.status = RESOURCE_STATUS.TO_APPLY;
          result.message = `The image build config tag [${
            currentConfigTag
          }] does not match expected [${expectedConfigTag}]. Will build a new one.`;
        } else {
          result.status = RESOURCE_STATUS.OK;
          result.message = `The image has been built and was updated at ${updateTime}.`;
        }
      }
      result.status_link = registry.getLinkToDockerImageFromUri(uri);
      return result;
    }
  } catch (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
};

/**
 * Returns the result (CheckResult) of an operation of a Cloud Build.
 * @param {string} buildId Operation name of deploying a Cloud Functions.
 * @return {!CheckResult}
 */
MojoFunctions.getCloudBuildStatus = buildId => {
  const properties = getDocumentProperties();
  const {projectId, locationId} = properties;
  const cloudBuild = new CloudBuild(projectId, locationId);
  const rawResponse = cloudBuild.getBuild(buildId);
  const {status, finishTime, failureInfo, results} = rawResponse;
  if (status === 'FAILURE') {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: failureInfo ? failureInfo.detail : 'Failed: Unknown reason.',
    };
  } else if (status === 'SUCCESS') {
    const image = results && results.images && results.images[0];
    if (!image) {
      return {
        status: RESOURCE_STATUS.ERROR,
        message: 'Build succeeded but no image was found in the results.',
      };
    }
    const registry = new ArtifactRegistry(projectId, locationId);
    // name: {locationId}-docker.pkg.dev/{projectId}/{repositoryId}/{imageId}
    // digest: sha256:xxx
    // @see ArtifactRegistry.getLinkToDockerImageFromUri for more details.
    const {name, digest} = image;
    return {
      status: RESOURCE_STATUS.OK,
      message: `Docker image was created at ${finishTime}`,
      status_link: registry.getLinkToDockerImageFromUri(`${name}@${digest}`),
    };
  }
  return {
    status: '',
    message: 'Building ',
    refreshFn: () => {
      return MojoFunctions.getCloudBuildStatus(buildId);
    },
  };
};

/**
 * Builds the docker image and returns status.
 * @param {string} imageRepoAndName
 * @param {!Object} resource
 * @return {!CheckResult}
 */
MojoFunctions.buildDockerImage = (imageRepoAndName, resource) => {
  const {attributesMap} = resource;
  const attributeNames = CloudBuild.ATTRIBUTE_NAMES;
  const storageSourceRaw =
    attributesMap[attributeNames.storageSource].attributeValue;
  const dockerFile = attributesMap[attributeNames.dockerFile].attributeValue;
  const baseImage = attributesMap[attributeNames.baseImage]?.attributeValue;
  const properties = getDocumentProperties();
  const {projectId, locationId, serviceAccount} = properties;
  const {sourceBucket, sourceObject} =
    MojoFunctions.getStorageSource_(storageSourceRaw);
  const source = {
    storageSource: {
      bucket: sourceBucket,
      object: sourceObject,
    },
  };
  const build = new CloudBuild(projectId, locationId);
  const imageUri = `${locationId}-${ArtifactRegistry.DOMAIN}/${projectId}/${imageRepoAndName}`;
  const buildResponse = build.buildDockerImage(
    source,
    imageUri,
    dockerFile,
    serviceAccount,
    baseImage,
  );
  const {metadata, error} = buildResponse;
  if (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
  return MojoFunctions.getCloudBuildStatus(metadata.build.id);
};

MojoTemplateRegistry.register('dockerImage', {
  category: 'Google Cloud',
  resource: 'Docker Image',
  value: 'default:latest',
  attributes: [
    {
      attributeName: CloudBuild.ATTRIBUTE_NAMES.storageSource,
    },
    {
      attributeName: CloudBuild.ATTRIBUTE_NAMES.dockerFile,
      attributeValue: 'Dockerfile',
    },
    {
      attributeName: CloudBuild.ATTRIBUTE_NAMES.baseImage,
    },
  ],
  propertyName: 'dockerImageName',
  alwaysCheck: true,
  checkFn: MojoFunctions.checkDockerImage,
  enableFn: MojoFunctions.buildDockerImage,
});
