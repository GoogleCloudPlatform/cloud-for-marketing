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

/** @fileoverview Cloud Build API handler class.*/

/**
 * This class is used to create docker image through Cloud Build.
 */
class CloudBuild extends ApiBase {
  /**
   * @param {string=} projectId
   * @param {string=} locationId
   */
  constructor(projectId, locationId) {
    super();
    this.name = 'Cloud Build API';
    this.api = 'cloudbuild.googleapis.com';
    this.permissions = [
      'cloudbuild.builds.create',
      'iam.serviceAccounts.actAs',
    ];
    this.adminRole = 'Cloud Build Editor';
    this.runtimeRoles = [
      'roles/artifactregistry.writer',
      'roles/logging.logWriter',
      'roles/storage.objectAdmin',
    ];
    this.apiUrl = `https://${this.api}`;
    this.version = 'v1';
    this.projectId = projectId;
    this.locationId = locationId;
  }

  /**
   * @override
   * @return {string}
   */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}/projects/${
      this.projectId
    }/locations/${this.locationId}`;
  }

  /**
   * Generates a deterministic Docker metadata tag encoding the normalized
   * base image name (after the last '/') and an 8-character MD5 config hash,
   * e.g. `cyborg-node-24.22.1-slim-4a8b9c12`.
   *
   * @param {string=} dockerFile
   * @param {string=} baseImage
   * @return {string}
   */
  static getConfigTag(dockerFile = 'DockerFile', baseImage = '') {
    const rawBaseName = (baseImage || '').split('/').pop().split('@')[0];
    const normalizedBase =
      rawBaseName
        .replace(/[^a-zA-Z0-9._-]+/g, '-')
        .replace(/^-+|-+$/g, '')
        .slice(0, 64) || 'default';
    const rawConfig = `${dockerFile}|${baseImage || ''}`;
    const digestBytes = Utilities.computeDigest(
      Utilities.DigestAlgorithm.MD5,
      rawConfig,
    );
    const hash8 = digestBytes
      .map(b => ((b < 0 ? b + 256 : b) + 256).toString(16).slice(1))
      .join('')
      .slice(0, 8);
    return `${CloudBuild.CONFIG_TAG_PREFIX}${normalizedBase}-${hash8}`;
  }

  /**
   * Starts a build to create a docker image.
   * @see https://cloud.google.com/build/docs/api/reference/rest/v1/projects.builds/create
   * @param {!Object} source The source code to build.
   * @param {string} imageUri The URI of the image to build.
   * @param {string=} dockerFile The name of the docker file. Default is
   *     'DockerFile'.
   * @param {string=} serviceAccount Optional service account email or resource
   *     name to execute the build.
   * @param {string=} baseImage Optional base image passed as `--build-arg
   *     BASE_IMAGE=<baseImage>`.
   * @return {!Object}
   */
  buildDockerImage(
    source,
    imageUri,
    dockerFile = 'DockerFile',
    serviceAccount = undefined,
    baseImage = undefined,
  ) {
    console.log('Start building docker image', imageUri);
    const lastSlash = imageUri.lastIndexOf('/');
    const colonIndex = imageUri.indexOf(':', lastSlash);
    const baseImageUri =
      colonIndex > -1 ? imageUri.slice(0, colonIndex) : imageUri;
    const configTag = CloudBuild.getConfigTag(dockerFile, baseImage);
    const configImageUri = `${baseImageUri}:${configTag}`;

    const args = ['build', '-f', dockerFile];
    if (baseImage) {
      args.push('--build-arg', `BASE_IMAGE=${baseImage}`);
    }
    args.push('-t', imageUri, '-t', configImageUri, '.');
    const payload = {
      source,
      steps: [
        {
          name: 'gcr.io/cloud-builders/docker',
          args,
        },
      ],
      images: [imageUri, configImageUri],
    };
    if (serviceAccount) {
      payload.serviceAccount = serviceAccount.startsWith('projects/')
        ? serviceAccount
        : `projects/${this.projectId}/serviceAccounts/${serviceAccount}`;
      payload.options = {
        logging: 'CLOUD_LOGGING_ONLY',
      };
    }
    const response = this.mutate('builds', payload);
    return response;
  }

  /**
   * Returns information about a previously requested build.
   *
   * @see https://cloud.google.com/build/docs/api/reference/rest/v1/projects.builds/get
   * @param {string} buildId ID of the build.
   * @return {!Build}
   */
  getBuild(buildId) {
    return this.get(`builds/${buildId}`);
  }
}

// Attributes used in Cyborg sheet to manage this resource.
CloudBuild.ATTRIBUTE_NAMES = Object.freeze({
  storageSource: 'Storage Source (zip file)',
  dockerFile: 'Docker File',
  baseImage: 'Docker Base Image',
});

// Prefix for the secondary Docker tag tracking build configuration.
CloudBuild.CONFIG_TAG_PREFIX = 'cyborg-';
