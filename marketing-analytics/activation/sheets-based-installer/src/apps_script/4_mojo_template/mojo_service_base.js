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

/** @fileoverview MojoTemplate for service account role resource. */

/**
 * Gets the possible assets for the given project and namespace.
 * Used property: projectId, namespace.
 * @return {!Array<!Object>}
 */
MojoFunctions.getPossibleAssets = () => {
  const projectId = getDocumentProperty('projectId');
  const namespace = getDocumentProperty('namespace');
  const cloudAsset = new CloudAsset(projectId);
  const assets = cloudAsset.listAssets(
    ServiceProviderRegistry.getRegisteredAssetTypes(),
  );
  const regex = new RegExp(
    `/projects/${projectId}/locations/([\\w-]+)/(services|jobs|functions)/(${
      namespace
    }[\\w-_]*)$`,
  );
  const targetAssets = [];
  assets.forEach(({name, assetType}) => {
    if (regex.test(name)) {
      const [fullName, locationId, _, assetName] = regex.exec(name);
      targetAssets.push({
        fullName,
        locationId,
        assetType,
        assetName,
      });
    }
  });
  return targetAssets;
};

/**
 * Gets the service provider for the given asset.
 * Used property: projectId.
 * @param {!Object} asset
 * @return {!GcpApiBase}
 */
MojoFunctions.getServiceProvider = asset => {
  const {assetType, locationId} = asset;
  const projectId = getDocumentProperty('projectId');
  return ServiceProviderRegistry.createProvider(
    assetType,
    projectId,
    locationId,
  );
};

/**
 * Gets the service account based on default configuration.
 * @return {string}
 */
MojoFunctions.getServiceAccount = () => {
  const assets = MojoFunctions.getPossibleAssets();
  let serviceAccount;
  if (!assets || assets.length === 0) {
    const projectId = getDocumentProperty('projectId');
    console.log('There is no relevant asset. Get the default service account.');
    const resourceManager = new CloudResourceManager(projectId);
    serviceAccount = resourceManager.getDefaultComputeServiceAccount();
  } else {
    const provider = MojoFunctions.getServiceProvider(assets[0]);
    console.log('Get the service account from the first asset', assets[0]);
    serviceAccount = provider.getServiceAccount(assets[0].assetName);
    console.log('The service account is:', serviceAccount);
  }
  return serviceAccount;
};

/**
 * Returns whether the default service account of Cloud Functions has expected
 * roles.
 * Used property: projectId.
 * @param {?string} sa Service account.
 * @param {!Object} resources
 * @return {!CheckResult}
 */
MojoFunctions.checkServiceAccountRole = (sa, resources) => {
  if (!sa) {
    const serviceAccount = MojoFunctions.getServiceAccount();
    if (!serviceAccount)
      return {
        status: RESOURCE_STATUS.ERROR,
        message: 'Failed to get the default service account.',
      };
    sa = serviceAccount;
  }
  const expectedRoles = resources.attributeValue.split('\n').filter(Boolean);
  const projectId = getDocumentProperty('projectId');
  const resourceManager = new CloudResourceManager(projectId);
  const roles = resourceManager.getIamPolicyForMember(`serviceAccount:${sa}`);
  const status_link = resourceManager.getLinkToIam();
  const result = {
    value: sa,
    status_link,
  };
  if (!expectedRoles.every(expected => roles.indexOf(expected) > -1)) {
    return Object.assign(result, {
      status: RESOURCE_STATUS.TO_APPLY,
      message: 'There are missing roles for the service account. ',
    });
  }
  return Object.assign(result, {
    status: RESOURCE_STATUS.OK,
    message: 'All required roles are present for the service account.',
  });
};

/**
 * Sets the Cloud Functions' default service account with expected roles.
 * Used property: projectId.
 * @param {string} sa Service account.
 * @param {!Object} resources
 * @return {!CheckResult}
 */
MojoFunctions.setServiceAccountRole = (sa, resources) => {
  const projectId = getDocumentProperty('projectId');
  const resourceManager = new CloudResourceManager(projectId);
  const roles = resources.attributeValue.split('\n');
  const updateIamPolicy = resourceManager.addIamPolicyBinding(
    `serviceAccount:${sa}`,
    roles,
  );
  return {
    status: updateIamPolicy ? RESOURCE_STATUS.OK : RESOURCE_STATUS.ERROR,
    message: `Operation of granting roles to this service account is ${
      updateIamPolicy ? 'succeeded' : 'failed'
    }.`,
  };
};

/**
 * Checks and standardizes the global deployment location based on namespace.
 *
 * Used property: projectId, namespace, locationId.
 * @param {string} value User-selected location string from sheet.
 * @param {!Object} resources
 * @return {!CheckResult}
 */
MojoFunctions.checkLocationByNamespace = (value, resources) => {
  const selectedLocationId = getLocationId(resources.attributeValue);

  const assets = MojoFunctions.getPossibleAssets();
  const existingLocationIds = new Set();
  let locationProvider;
  if (assets.length > 0) {
    locationProvider = MojoFunctions.getServiceProvider(assets[0]);
    assets.forEach(({locationId}) => existingLocationIds.add(locationId));
  } else {
    const projectId = getDocumentProperty('projectId');
    locationProvider = ServiceProviderRegistry.getFirstAvailable(projectId);
    if (!locationProvider) {
      return {
        status: RESOURCE_STATUS.ERROR,
        message: 'No compute engine API included.',
      };
    }
  }
  const rawLocations = locationProvider.listLocations();
  const locations = rawLocations.map(getLocationListName);

  const result = {
    attributeValue_datarange: locations,
  };
  if (existingLocationIds.size === 0) {
    if (selectedLocationId) {
      result.status = RESOURCE_STATUS.OK;
    } else {
      result.status = RESOURCE_STATUS.ERROR;
      result.message =
        'Select a location for Cloud Run and related Google Cloud resources';
    }
  } else {
    if (selectedLocationId && !existingLocationIds.has(selectedLocationId)) {
      result.status = RESOURCE_STATUS.ERROR;
      result.message =
        `Existing Cloud Run deployment found in [${[
          ...existingLocationIds,
        ].join(', ')}].` +
        ` Change location to one of them or delete old resources before continuing.`;
    } else {
      result.status = RESOURCE_STATUS.OK;
      const location = [...existingLocationIds][0];
      const locationName = getLocationListName(
        rawLocations.find(({locationId}) => locationId === location),
      );
      result.message = `Updated location to existing Cloud Run deployment in [${
        locationName
      }]`;
      if (existingLocationIds.size > 1) {
        result.message += ` [WARNING] Multiple locations exist (${[
          ...existingLocationIds,
        ].join(', ')}). Please consider unifying them before continuing.`;
      }
      result.attributeValue = locationName;
    }
  }
  return result;
};

MojoTemplateRegistry.register('serviceAccountRole', {
  category: 'Google Cloud',
  resource: 'Service Account',
  value: '',
  editType: RESOURCE_EDIT_TYPE.DEFAULT_VALUE,
  attributeName: 'Required Roles',
  propertyName: 'serviceAccount',
  checkFn: MojoFunctions.checkServiceAccountRole,
  enableFn: MojoFunctions.setServiceAccountRole,
});

MojoTemplateRegistry.register('location', {
  category: 'Google Cloud',
  resource: 'Deployment Location',
  value: '=IFERROR(TRIM(INDEX(SPLIT(R[0]C[2], "|"), 2)), R[0]C[2])',
  editType: RESOURCE_EDIT_TYPE.READONLY,
  attributeName: 'Region Picker',
  attributeName_link: 'https://cloud.withgoogle.com/region-picker/',
  propertyName: 'locationId',
  checkFn: MojoFunctions.checkLocationByNamespace,
});
