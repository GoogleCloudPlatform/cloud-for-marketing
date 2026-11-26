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
 *
 * @param {string} imageRepoAndName
 * @return
 */
MojoFunctions.getRepoAndImageName_ = imageRepoAndName => {
  const splitter = imageRepoAndName.indexOf('/');
  const imageNameAndLabel = imageRepoAndName.slice(splitter + 1);
  const repositoryId = imageRepoAndName.slice(0, splitter);
  return {repositoryId, imageNameAndLabel};
};

/**
 * Combined one-step check and idempotent update/create for Artifact Registry
 * Repositories.
 * @param {string} repositoryId The identifier for the Artifact Registry
 *     Repository.
 * @param {!Object} resource - The resources object containing the
 *     attributesMap.
 * @return {!CheckResult} Standardized status, descriptive outcome message, and
 *     console link.
 */
MojoFunctions.checkOrUpdateArtifactRepository = (repositoryId, resource) => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const artifactRegistry = new ArtifactRegistry(projectId, locationId);
  const formatAttr =
    resource.attributesMap[ArtifactRegistry.ATTRIBUTE_NAMES.format];
  const modeAttr =
    resource.attributesMap[ArtifactRegistry.ATTRIBUTE_NAMES.mode];
  const format = formatAttr
    ? formatAttr.attributeValue
    : ArtifactRegistry.FORMAT.Docker;
  const mode = modeAttr
    ? modeAttr.attributeValue
    : ArtifactRegistry.MODE.Standard;

  const modeMappings = ArtifactRegistry.PAYLOAD_MAPPINGS[mode];
  const targetMapping =
    mode === ArtifactRegistry.MODE.Remote ? modeMappings[format] : modeMappings;

  const defaultPayload = {
    format: ArtifactRegistry.FORMAT.Docker,
    mode: ArtifactRegistry.MODE.Standard,
    cleanupPolicies: ArtifactRegistry.DEFAULT_CLEANUP_POLICIES,
  };

  const expectedPayload = Object.assign(
    defaultPayload,
    MojoFunctions.buildPayloadFromAttributes(
      resource.attributesMap,
      targetMapping,
    ),
  );
  return MojoFunctions.reconcileDeclarativeResource(
    'Artifact Registry Repository',
    artifactRegistry.getRepository(repositoryId),
    payload => artifactRegistry.createOrUpdateRepository(repositoryId, payload),
    expectedPayload,
    artifactRegistry.getLinkToCloudConsole(repositoryId),
  );
};

/**
 * Transforms Cloud Run attribute values based on property path conventions.
 * @param {string} dotPath Dotted property path.
 * @param {*} rawValue Cell value string.
 * @return {*} Transformed value.
 */
MojoFunctions.transformCloudRunValue = (dotPath, rawValue) => {
  if (dotPath.endsWith('image')) {
    let imgVal = String(rawValue).trim();
    const projectId = getDocumentProperty('projectId');
    const locationId = getDocumentProperty('locationId');
    const {repositoryId, imageNameAndLabel} =
      MojoFunctions.getRepoAndImageName_(imgVal);
    const registry = new ArtifactRegistry(projectId, locationId);
    const image = registry.getDockerImage(repositoryId, imageNameAndLabel);
    if (image) {
      imgVal = image.uri;
      console.log(`Using the latest image: ${imgVal}`);
    } else {
      // If the repo is 'REMOTE', then return the complete uri for the first
      // time
      const repository = registry.getRepository(repositoryId);
      if (repository.mode === ArtifactRegistry.MODE.Remote) {
        imgVal = `${repository.registryUri}/${imageNameAndLabel}`;
        console.log(`Using the complete uri for REMOTE repo: ${imgVal}`);
      } else {
        console.warn(
          `No latest image found for ${imgVal}, using the original value.`,
        );
        throw new Error(`No image found for ${imgVal}.`);
      }
    }
    return imgVal;
  }
  if (dotPath.endsWith('memory')) {
    return String(rawValue)
      .trim()
      .replace(/\s*s?$/i, 'Gi');
  }
  if (dotPath.endsWith('timeout')) {
    return String(rawValue)
      .trim()
      .replace(/\s*s?$/i, 's');
  }
  if (
    dotPath.endsWith('maxRetries') ||
    dotPath.endsWith('parallelism') ||
    dotPath.endsWith('maxInstanceRequestConcurrency')
  ) {
    return Number(rawValue);
  }
  if (dotPath.endsWith('env')) {
    return String(rawValue)
      .trim()
      .split(',')
      .map(item => {
        const splitIndex = item.indexOf('=');
        const key = (
          splitIndex > -1 ? item.slice(0, splitIndex) : item
        ).trim();
        const value = (
          splitIndex > -1 ? item.slice(splitIndex + 1) : ''
        ).trim();
        if (!value) {
          throw new Error(`Environment variable "${key}" is not ready yet.`);
        }
        return {name: key, value};
      });
  }
  if (dotPath.endsWith('ingress')) {
    const str = String(rawValue).trim().toLowerCase();
    if (str === 'all' || str === 'ingress_traffic_all') {
      return 'INGRESS_TRAFFIC_ALL';
    }
    if (str === 'internal' || str === 'ingress_traffic_internal_only') {
      return 'INGRESS_TRAFFIC_INTERNAL_ONLY';
    }
    if (
      str === 'internal-and-cloud-load-balancing' ||
      str === 'ingress_traffic_internal_load_balancer'
    ) {
      return 'INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER';
    }
    return String(rawValue).trim();
  }
  return rawValue;
};

MojoTemplateRegistry.register('artifactRepository', {
  category: 'Google Cloud',
  resource: 'Artifact Repository',
  value: '${namespace|hyphened}-repo',
  groupType: GROUP_TYPE.COLLAPSE,
  attributes: [
    {
      attributeName: ArtifactRegistry.ATTRIBUTE_NAMES.format,
      attributeValue: ArtifactRegistry.FORMAT.Docker,
      attributeValue_datarange: Object.values(ArtifactRegistry.FORMAT),
    },
    {
      attributeName: ArtifactRegistry.ATTRIBUTE_NAMES.mode,
      attributeValue: ArtifactRegistry.MODE.Standard,
      attributeValue_datarange: Object.values(ArtifactRegistry.MODE),
    },
    // Only for remote repo
    // {
    //   attributeName: ArtifactRegistry.ATTRIBUTE_NAMES.uri,
    // },
  ],
  propertyName: 'repositoryId',
  checkFn: MojoFunctions.checkOrUpdateArtifactRepository,
});
