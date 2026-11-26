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

/** @fileoverview Artifact Registry API handler class.*/

/**
 * This class is for Artifact Registry.
 */
class ArtifactRegistry extends GcpApiBase {
  /**
   * @param {string} projectId
   * @param {string} locationId
   */
  constructor(projectId, locationId) {
    super();
    this.name = 'Artifact Registry API';
    this.api = 'artifactregistry.googleapis.com';
    this.permissions = [
      'artifactregistry.repositories.create',
      'artifactregistry.repositories.get',
    ];
    this.adminRole = 'Artifact Registry Administrator';
    this.runtimeRoles = [];
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
   * Gets a repository.
   * @see https://cloud.google.com/artifact-registry/docs/reference/rest/v1/projects.locations.repositories/get
   * @param {string} repositoryId
   * @return {!Repository}
   */
  getRepository(repositoryId) {
    return this.get(`repositories/${repositoryId}`);
  }

  /**
   * Checks if a repository exists.
   * @param {string} repositoryId
   * @return {boolean}
   */
  existRepository(repositoryId) {
    const response = this.getRepository(repositoryId);
    if (response.name) return true;
    if (
      response.error &&
      (response.error.code === 404 || response.error.status === 'NOT_FOUND')
    ) {
      return false;
    }
    console.error(
      `Unknown status of Artifact Registry Repository ${repositoryId}`,
      response,
    );
    throw new Error(
      `Unknown status of Artifact Registry Repository ${repositoryId}`,
    );
  }

  /**
   * Creates a repository.
   * @see https://cloud.google.com/artifact-registry/docs/reference/rest/v1/projects.locations.repositories/create
   * @param {string} repositoryId
   * @param {!Object=} payload
   * @return {!Repository}
   */
  createRepository(repositoryId, payload = {}) {
    const createPayload = Object.assign({}, payload);
    const uri = `repositories?repositoryId=${repositoryId}`;
    const operation = this.mutate(uri, createPayload);
    return this.waitAndResolveOperation(
      operation,
      `Artifact Registry Repository ${repositoryId}`,
    );
  }

  /**
   * Updates an existing repository.
   * @see https://cloud.google.com/artifact-registry/docs/reference/rest/v1/projects.locations.repositories/patch
   * @param {string} repositoryId
   * @param {!Object=} payload Repository fields to update.
   * @return {!Repository}
   */
  updateRepository(repositoryId, payload = {}) {
    const updatePayload = Object.assign({}, payload);
    delete updatePayload.format;
    delete updatePayload.mode;
    delete updatePayload.name;
    const updateMask = encodeURIComponent(Object.keys(updatePayload).join(','));
    const uri = `repositories/${repositoryId}?updateMask=${updateMask}`;
    console.log(
      'Updating Artifact Registry Repository',
      repositoryId,
      JSON.stringify(updatePayload),
    );
    return this.mutate(uri, updatePayload, 'PATCH');
  }

  /**
   * Creates or updates a repository idempotently.
   * @param {string} repositoryId
   * @param {!Object=} payload
   * @return {!Repository}
   */
  createOrUpdateRepository(repositoryId, payload = {}) {
    if (this.existRepository(repositoryId)) {
      console.log(
        'Updating existing Artifact Registry repository',
        repositoryId,
      );
      return this.updateRepository(repositoryId, payload);
    }
    console.log(
      'Repository not found, creating new Artifact Registry repository',
      repositoryId,
    );
    return this.createRepository(repositoryId, payload);
  }

  /**
   * Lists docker images.
   * @see https://cloud.google.com/artifact-registry/docs/reference/rest/v1/projects.locations.repositories.dockerImages/list
   * @param {string} repositoryId
   * @return {!Array<!DockerImage>}
   */
  listDockerImages(repositoryId) {
    const images = this.list(
      'dockerImages',
      `repositories/${repositoryId}/dockerImages`,
    );
    return images;
  }

  /**
   * Gets the latest docker image with the given name. If there is no tag in the
   * `imageNameAndTag`, then the 'latest' will be used as the tag.
   * @param {string} repositoryId
   * @param {string} imageNameAndTag
   * @return {!DockerImage}
   */
  getDockerImage(repositoryId, imageNameAndTag) {
    const images = this.listDockerImages(repositoryId);
    let imageName, tag;
    if (imageNameAndTag.indexOf(':') > 0) {
      [imageName, tag] = imageNameAndTag.trim().split(':');
    } else {
      imageName = imageNameAndTag.trim();
      tag = 'latest';
    }
    const imageUri = `dockerImages/${encodeURIComponent(imageName)}`;
    const image = images.filter(({name, tags}) => {
      return name.indexOf(imageUri) > 0 && tags && tags.indexOf(tag) > -1;
    });
    console.log(
      `Find docker image ${repositoryId}/${imageNameAndTag}: `,
      image.length,
    );
    return image[0];
  }

  /**
   * Returns a link to the artifact repository in Google Cloud Console.
   * @param {string} repositoryId
   * @return {string}
   */
  getLinkToCloudConsole(repositoryId) {
    const link = `https://console.cloud.google.com/artifacts/docker/${
      this.projectId
    }/${this.locationId}/${repositoryId}`;
    return link;
  }

  /**
   * Returns a link to the docker image in Google Cloud Console.
   * @param {string} uri `uri` in response of listDockerImages, which is
   * like
   * `{locationId}-docker.pkg.dev/{projectId}/{repositoryId}/{imageId}@sha256:xxx`
   * @return {string}
   */
  getLinkToDockerImageFromUri(uri) {
    const elements = uri.split('/');
    const repositoryId = elements[elements.length - 2];
    const link = this.getLinkToCloudConsole(repositoryId);
    const imageUri = elements[elements.length - 1].replace('@', '/');
    return link + '/' + imageUri;
  }
}

/**
 * Default cleanup policy: Retains the 3 most recent versions for safe
 * rollbacks, and automatically deletes orphaned (untagged) images older than 30
 * days to optimize storage.
 */
ArtifactRegistry.DEFAULT_CLEANUP_POLICIES = Object.freeze({
  'keep-recent-3': {
    action: 'KEEP',
    mostRecentVersions: {
      keepCount: 3,
    },
  },
  'delete-untagged-old': {
    action: 'DELETE',
    condition: {
      tagState: 'UNTAGGED',
      olderThan: '2592000s',
    },
  },
});

/**
 * Package formats supported by Artifact Registry.
 * @enum {string}
 * @see https://docs.cloud.google.com/artifact-registry/docs/reference/rest/v1/projects.locations.repositories#format
 */
ArtifactRegistry.FORMAT = Object.freeze({
  Docker: 'DOCKER',
  Maven: 'MAVEN',
  NPM: 'NPM',
  APT: 'APT',
  YUM: 'YUM',
  GooGet: 'GOOGET',
  Python: 'PYTHON',
  'Kubeflow Pipelines': 'KFP',
  Go: 'GO',
  Generic: 'GENERIC',
  Ruby: 'RUBY',
});

/**
 * The mode configures the repository to serve artifacts from different sources.
 * @enum {string}
 * @see https://docs.cloud.google.com/artifact-registry/docs/reference/rest/v1/projects.locations.repositories#mode
 */
ArtifactRegistry.MODE = Object.freeze({
  Standard: 'STANDARD_REPOSITORY',
  Remote: 'REMOTE_REPOSITORY',
  Virtual: 'VIRTUAL_REPOSITORY',
  'Assured OSS': 'ASSURED_OSS_REPOSITORY',
});

// Attributes used in Cyborg sheet to manage this resource.
ArtifactRegistry.ATTRIBUTE_NAMES = Object.freeze({
  format: 'Repository Format',
  mode: 'Mode',
  uri: 'Repository URI',
});

// Build PAYLOAD_MAPPINGS inside an IIFE (Immediately Invoked Function
// Expression) to encapsulate local construction variables. This prevents global
// scope pollution in Apps Script and avoids attaching temporary properties to
// the ArtifactRegistry class.
ArtifactRegistry.PAYLOAD_MAPPINGS = (() => {
  const baseMapping = {
    format: ArtifactRegistry.ATTRIBUTE_NAMES.format,
    mode: ArtifactRegistry.ATTRIBUTE_NAMES.mode,
  };
  const remoteUriAttr = ArtifactRegistry.ATTRIBUTE_NAMES.uri;

  const remoteMappings = Object.values(ArtifactRegistry.FORMAT).reduce(
    (mappings, format) => {
      mappings[format] = Object.assign({}, baseMapping, {
        [`remoteRepositoryConfig.${format.toLowerCase()}Repository.customRepository.uri`]:
          remoteUriAttr,
      });
      return mappings;
    },
    {},
  );

  return Object.freeze({
    [ArtifactRegistry.MODE.Standard]: baseMapping,
    [ArtifactRegistry.MODE.Virtual]: baseMapping,
    [ArtifactRegistry.MODE.Remote]: remoteMappings,
  });
})();

ArtifactRegistry.DOMAIN = 'docker.pkg.dev';
