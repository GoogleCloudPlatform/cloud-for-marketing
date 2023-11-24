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

/** @fileoverview Google Cloud Platform utlities functions. */

/**
 * This object is used to contain `check` and `enable` functions that
 * are sitting between GCP API classes and Mojo class. They are used to check or
 * operate on GCP resources defined in a `Mojo`. The return value of these
 * functions are `CheckResult` (@see CheckResult).
 *
 * Functions here can be used directly with the default naming convention.
 *
 * The parameters for `check` functions are:
 *  1. the first one is the `value` of the 'Value' column of that row.
 *  2. the whole row as the second optional parameter.
 *
 * The parameters for `enable` functions are:
 *  1. the first one is the `value` of the 'Value' column of that row.
 *  2. the whole row as the second optional parameter.
 *  3. the `sheet` object to the current Google sheet.
 *
 * If a solution needs customized property names, the solution should have
 * those adapter functions in it and call `gcloud` functions there.
 */
const gcloud = {};

/**
 * Checks GCP existence and billing status.
 * @param {string} projectId
 * @return {!CheckResult}
 */
gcloud.checkProject = (projectId) => {
  const resourceManager = new CloudResourceManager(projectId);
  const { error, lifecycleState } = resourceManager.getProject();
  if (error) {
    // If `Cloud Resource Manager API` is not enabled in the backend GCP, enable
    // it and try again.
    const result = gcloud.enableBackendProjectAPIs_(error, resourceManager);
    if (result) return result;
    return gcloud.checkProject(projectId);
  }
  let status = RESOURCE_STATUS.ERROR;
  let message = '';
  if (!lifecycleState) {
    message = `Project doesn't exist`;
  } else if (lifecycleState !== 'ACTIVE') {
    message = `Wrong project status ${lifecycleState}`;
  } else {
    const cloudBilling = new CloudBilling(projectId);
    const { error, billingEnabled } = cloudBilling.getBillingInfo();
    if (error) {
      const result = gcloud.enableBackendProjectAPIs_(error, cloudBilling);
      if (result) return result;
      return gcloud.checkProject(projectId);
    }
    if (!billingEnabled) {
      message = 'No billing account.';
    } else {
      status = RESOURCE_STATUS.OK;
    }
  }
  return {
    status,
    message,
    value_link: `https://console.cloud.google.com/welcome?project=${projectId}`,
  };
}

/**
 * Enable APIs for the backend GCP project if there is an related error caused
 * by no enabled APIs. If any error happened, it returns the check result with
 * error status.
 * This function uses `gcloud.enableApi` to enable APIs.
 *
 * @param {!Error} error
 * @param {Object} apiClient
 * @return {!CheckResult|undefined}
 * @private
 */
gcloud.enableBackendProjectAPIs_ = (error, apiClient) => {
  const { name, api } = apiClient;
  if (error.code === 403
    && error.message.indexOf(`${name} has not been used in project`) > -1) {
    const regex = /not been used in project ([0-9]*) before or it is disabled/;
    const projectId = regex.exec(error.message)[1];
    if (projectId) {
      const result = gcloud.enableApi(api, projectId);
      if (result.status !== RESOURCE_STATUS.OK) {
        return result;
      }
      console.log(`Enabled ${name} for the backend project.`);
      return;
    } else {
      console.error('Can not get project Id from the error message', error.message);
    }
  }
  return {
    status: RESOURCE_STATUS.ERROR,
    message: error.message,
  };
}

/**
 * Checks current user's permssion.
 * Used property: projectId.
 * @param {string} permission
 * @return {!CheckResult}
 */
gcloud.checkPermission = (permission) => {
  const projectId = getDocumentProperty('projectId');
  const resourceManager = new CloudResourceManager(projectId);
  const result = resourceManager.testPermissions([permission]);
  if (result) return { status: RESOURCE_STATUS.OK };
  return { status: RESOURCE_STATUS.ERROR, message: 'MISSING' };
};

/**
 * Checks and enables the API if it is not enabled.
 * Used property: projectId.
 * @param {string} apiName
 * @return  {!CheckResult}
 */
gcloud.checkOrEnableApi = (apiName) => {
  const api = GOOGLE_CLOUD_SERVICE_LIST[apiName];
  const projectId = getDocumentProperty('projectId');
  const serviceUsage = new ServiceUsage(projectId);
  const { error, state } = serviceUsage.getService(api);
  if (error) {
    return { status: RESOURCE_STATUS.ERROR, message: error.message };
  }
  if (state !== 'ENABLED') return gcloud.enableApi(api);
  return { status: RESOURCE_STATUS.OK };
};

/**
 * Enables an Api for the specified project. If the project is not given, it
 * will use the `projectId` from document property.
 * It will wait until the operation completes.
 * @param {string} api
 * @param {string|undefined} explicitProjectId
 * @return  {!CheckResult}
 */
gcloud.enableApi = (api, explicitProjectId) => {
  const projectId = explicitProjectId || getDocumentProperty('projectId');
  const serviceUsage = new ServiceUsage(projectId);
  let rawResponse = serviceUsage.enableService(api);
  let counterDown = 24;
  while (rawResponse.done !== true) {
    if (rawResponse.error) {
      return {
        status: RESOURCE_STATUS.ERROR,
        message: rawResponse.error.message,
      };
    }
    if (counterDown-- < 0) {
      return {
        status: RESOURCE_STATUS.ERROR,
        message: `Enable API timeout: ${api}`,
      };
    }
    Utilities.sleep(5000);
    rawResponse = serviceUsage.getOperation(rawResponse.name);
    console.log('Wait and get operation', rawResponse);
  }
  const { response } = rawResponse;
  return {
    status: response.service.state === 'ENABLED'
      ? RESOURCE_STATUS.OK : RESOURCE_STATUS.ERROR,
    message: response.service.state,
  };
};

/**
 * Returns a default location for Firestore based on the property 'locationId'.
 * The property 'locationId' is selected by users for Cloud Functions/Storage.
 * For better performance, a default Firestore location is decided to be the
 * same or close to that location.
 * @param {string} locationId
 * @param {!Array<string>} locations
 * @return {{displayName:string, locationId: string}}
 * @private
 */
gcloud.getFirestoreDefaultLocationObject_ = (locationId, locations) => {
  let id;
  if (locationId.startsWith('europe-')) {
    id = 'eur3';
  } else if (locationId.startsWith('us-')) {
    id = 'nam5';
  } else {
    const target = locations.filter(
      ({ locationId: id }) => locationId === id)[0];
    id = target ? target.locationId : 'nam5';
  }
  return getLocationObject(locations, id);
};

/**
 * Checks Firstore status. If it exists, return status `OK` and its location;
 * if it doesn't exist, return `READY_TO_INSTALL` to ask the use to continue to
 * create the Firestore at the 'selected location' (from the `attributeValue` of
 * the `resource`) or the default location (from the document property).
 * Used property: projectId, locationId.
 * @param {string} name
 * @param {Object} resource
 * @return {!CheckResult}
 */
gcloud.checkFirestore = (name, resource) => {
  const projectId = getDocumentProperty('projectId');
  const databaseId = name;

  const { attributes, attributesMap } = resource;
  const attrrbuteNames = ATTRIBUTE_NAMES.firestore;
  const modeAttr = attributesMap[attrrbuteNames.mode];
  const locationAttr = attributesMap[attrrbuteNames.location];

  const selectedMode = modeAttr.attributeValue;
  const selectedLocation = locationAttr.attributeValue;

  const firestore = new Firestore(projectId, databaseId);

  const { locations = [], error: listLocationError } = firestore.listLocations();
  locationAttr.attributeValue_datarange = locations.map(getLocationListName);
  if (listLocationError) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: listLocationError.message,
      attributes,
    };
  }
  const { error, type, locationId } = firestore.getDatabase();
  if (error) {
    if (error.status === 'NOT_FOUND') {
      const locationId = getDocumentProperty('locationId');
      const targetMode = selectedMode || FIRESTORE_MODE.FIRESTORE_NATIVE;
      const targetLocation = selectedLocation ||
        getLocationListName(
          gcloud.getFirestoreDefaultLocationObject_(locationId, locations));
      modeAttr.attributeValue = targetMode;
      locationAttr.attributeValue = targetLocation;
      return {
        status: RESOURCE_STATUS.READY_TO_INSTALL,
        message: `Will create a ${targetMode} Firestore[${databaseId}] in ${targetLocation}`,
        attributes,
      };
    }
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
      attributes,
    };
  }
  const currentLocation =
    getLocationListName(getLocationObject(locations, locationId));
  modeAttr.attributeValue = FIRESTORE_MODE[type];
  locationAttr.attributeValue = currentLocation;
  return {
    status: RESOURCE_STATUS.OK,
    message:
      selectedMode === FIRESTORE_MODE[type] && selectedLocation === currentLocation
        ? '' : 'Updated mode and location to the existing Firestore in this GCP',
    attributes,
  };
};

/**
 * Creates a Firestore instance.
 * Used property: projectId.
 * @param {string} databaseId
 * @param {object} resource
 * @return {!CheckResult}
 */
gcloud.createFirestore = (databaseId, resource) => {
  const projectId = getDocumentProperty('projectId');

  const { attributesMap } = resource;
  const modeDesc = attributesMap['Mode'].attributeValue;
  const selectedLocation = attributesMap['Location'].attributeValue;

  const locationId = getLocationId(selectedLocation);
  const mode = Object.keys(FIRESTORE_MODE).filter(
    (key) => FIRESTORE_MODE[key] === modeDesc)[0];
  const firestore = new Firestore(projectId, databaseId);
  const response = firestore.createDatabase(locationId, mode, databaseId);
  const { done, error } = response;
  if (error) return { status: RESOURCE_STATUS.ERROR, message: error.message };
  if (!done) console.warn('Uncomplete create Firestore response', response);
  return { status: RESOURCE_STATUS.OK, message: 'Firestore instance created.' };
};

/**
 * Saves entities to Firestore for both native and datastore modes.
 * @param {string} kind Entity name.
 * @param {Object<string,object>} entities
 * @return {!Firestore.Commit}
 */
gcloud.saveEntitiesToFirestore = (kind, entities) => {
  const projectId = getDocumentProperty('projectId');
  const databaseId = getDocumentProperty('databaseId');
  const firestore = new Firestore(projectId, databaseId);
  const { error, type } = firestore.getDatabase();
  if (error) {
    throw new Error(error.message);
  }
  const namespace = getDocumentProperty('namespace');
  if (type === 'DATASTORE_MODE') {
    const datastore = new Datastore(projectId, databaseId, namespace, kind);
    const response = datastore.txSave(entities);
    return response.mutationResults;
  } else {
    const firestore =
      new Firestore(projectId, databaseId, `${namespace}/database`, kind);
    const response = firestore.txSave(entities);
    return response.writeResults;
  }
};

/**
 * Checks Cloud Functions location based on the namespace. It will get location
 * of Cloud Functions whose name starts with the namespace as the defulat value.
 * Used property: projectId, namespace.
 * @param {string} location
 * @return {!CheckResult}
 */
gcloud.checkLocationByNamespace = (location) => {
  const selectedLocationId = getLocationId(location);
  const projectId = getDocumentProperty('projectId');
  const namespace = getDocumentProperty('namespace');
  const cloudFunctions = new CloudFunctions(projectId);
  const { functions = [] } = cloudFunctions.listFunctionsForAllLocations();
  const locations = cloudFunctions.listLocations().map(getLocationListName);
  const regex = new RegExp(
    `^projects/${projectId}/locations/([\\w-]+)/functions/${namespace}[\\w-]+$`);
  const locationIds = functions.filter(({ name }) => regex.test(name))
    .map(({ name }) => regex.exec(name)[1]);
  if (locationIds.length === 0) {
    if (selectedLocationId) return { status: RESOURCE_STATUS.OK };
    return {
      status: RESOURCE_STATUS.ERROR,
      message: 'Select a location for Cloud Functions and Cloud Storage',
      value_datarange: locations,
    };
  }
  const existingLocation = locationIds[0];
  const inconsistLocations =
    locationIds.some((locationId) => locationId !== existingLocation);
  if (inconsistLocations) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message:
        'Related Cloud Functions in different locations. Delete them before continue.',
      value_datarange: locations,
    };
  }
  if (selectedLocationId) {
    if (selectedLocationId === existingLocation) {
      return { status: RESOURCE_STATUS.OK };
    }
    return {
      status: RESOURCE_STATUS.ERROR,
      message:
        `Existing Cloud Functions in ${existingLocation}.` +
        ` Change the location or delete Cloud Functions before continue.`,
      value_datarange: locations,
    };
  }
  const [locationObject] = cloudFunctions.listLocations()
    .filter(({ locationId }) => locationId === existingLocation);
  return {
    status: RESOURCE_STATUS.OK,
    message: 'Updated location to the existing Cloud Functions in this GCP',
    value: getLocationListName(locationObject),
    value_datarange: locations,
  };
}

/**
 * Checks Cloud Storage bucket. If it needs to be created or updated, it
 * returns a status `READY_TO_INSTALL` to ask users to continue with the menu
 * `Apply changes` to make those changes.
 * Used property: projectId, locationId.
 *
 * @param {string} bucket Bucket name.
 * @param {{attributeValue:(string|undefined)}} resource The `attributeValue`
 *   is the days of the object lifecycle of the bucket.
 * @return {!CheckResult}
 */
gcloud.checkBucket = (bucket, resource) => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const storage = new Storage(projectId);
  const status = storage.getBucketStatus(bucket);
  const { attributeValue } = resource;
  const age = (attributeValue || 0) - 0;
  const targetLifecycle = age
    ? { rule: [{ action: { type: 'Delete' }, condition: { age: age - 0 } }] }
    : { rule: [] };
  switch (status) {
    case BUCKET_STATUS.NON_EXIST:
      const lifecycleInfo =
        `Files will be automatically removed after ${age} days.`;
      return {
        status: RESOURCE_STATUS.READY_TO_INSTALL,
        message:
          `Will create bucket in [${locationId}]. ${age ? lifecycleInfo : ''}`,
      };
    case BUCKET_STATUS.EXIST:
      const result = { status: RESOURCE_STATUS.OK, message: '' };
      const { location, lifecycle } = storage.getBucket(bucket);
      if (age) {
        if (!lifecycle
          || JSON.stringify(lifecycle) !== JSON.stringify(targetLifecycle)) {
          result.status = RESOURCE_STATUS.READY_TO_INSTALL;
          result.message += 'Will update lifecycle rules based on the setting. ';
        }
      } else if (lifecycle) {
        result.status = RESOURCE_STATUS.READY_TO_INSTALL;
        result.message += 'Will remove lifecycle rules based on the setting. ';
      }
      if (location.toLowerCase() !== locationId.toLowerCase()) {
        result.message +=
          `Bucket has a different location[${location}] to the Cloud Funcionts.`;
      }
      return result;
    default:
      return {
        status: RESOURCE_STATUS.ERROR,
        message: `Bucket[${bucket}] status ${status}. Change the name and retry`,
      };
  }
}

/**
 * Creates or updates the Cloud Storage bucket.
 * Used property: projectId, locationId.
 *
 * @param {string} bucket Bucket name.
 * @param {{attributeValue:(string|undefined)}} resource The `attributeValue`
 *   is the days of the object lifecycle of the bucket.
 * @return {!CheckResult}
 */
gcloud.createOrUpdateBucket = (bucket, resource) => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const storage = new Storage(projectId);
  const status = storage.getBucketStatus(bucket);
  const { attributeValue } = resource;
  const age = (attributeValue || 0) - 0;
  const lifecycle = age
    ? { rule: [{ action: { type: 'Delete' }, condition: { age } }] }
    : { rule: [] };
  let response;
  if (status === BUCKET_STATUS.NON_EXIST) {
    const options = { location: locationId, lifecycle };
    response = storage.createBucket(bucket, options);
  } else {
    response = storage.updateBucket(bucket, { lifecycle });
  }
  const { error } = response;
  if (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
  return {
    status: RESOURCE_STATUS.OK,
    message: `Bucket[${bucket}] was created/updated.`,
  };
}

/**
 * Gets the default location for a BigQuery dataset based on the property
 * 'locationId'. The property 'locationId' is selected by users for Cloud
 * Functions/Storage. For better performance, a default BigQquery dataset
 * location is decided to be the same or close to that location.
 * @param {string} locationId
 * @return {{displayName:string, locationId: string}}
 * @private
 */
gcloud.getBigQueryDefaultLocationObject_ = (locationId) => {
  let id;
  if (locationId.startsWith('europe-')) {
    id = 'EU';
  } else if (locationId.startsWith('us-')) {
    id = 'US';
  } else {
    const target = BIGQUERY_LOCATIONS.filter(
      ({ locationId: id }) => id === locationId)[0];
    id = target ? target.locationId : 'US';
  }
  return getLocationObject(BIGQUERY_LOCATIONS, id);
}

/**
 * Gets other properties for a BigQuery dataset from the document properties.
 * This would be used to check whether a dataset needs update and how to update
 * the dataset.
 *
 * The list of properties are:
 * 1) partitionExpiration -> defaultPartitionExpirationMs
 * 2) tableExpiration -> defaultTableExpirationMs
 *
 * O
 * @see https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#Dataset
 * @return {object}
 * @private
 * @deprecated
 */
gcloud.getBigQueryOptions_ = (value = 0) => {
  const valueMs = ((value - 0) * 24 * 3600 * 1000)//.toString();
  const options = {};
  ['defaultPartitionExpirationMs']//, 'defaultTableExpirationMs'
    .forEach((key) => { options[key] = valueMs; });
  return options;
};

/**
 * Checks BigQuery dataset. If it needs to be created or updated, it
 * returns a status `READY_TO_INSTALL` to ask users to continue with the menu
 * `Apply changes` to make those changes.
 * Used property: projectId, locationId.
 *
 * @param {string} datasetId Dataset name.
 * @param {Object} resource
 * @return {!CheckResult}
 */
gcloud.checkDataset = (datasetId, resource) => {
  const projectId = getDocumentProperty('projectId');

  const { attributes, attributesMap } = resource;
  const attrrbuteNames = ATTRIBUTE_NAMES.bigquery;
  const { attributeValue: partitionExpirationDay }
    = attributesMap[attrrbuteNames.partitionExpiration];
  const locationAttr = attributesMap[attrrbuteNames.location];

  const partitionExpirationMs =
    ((partitionExpirationDay - 0) * 24 * 3600 * 1000).toString();
  const selectedLocation = locationAttr.attributeValue;

  if (partitionExpirationMs <= 0) {
    const message = 'Parition Expiration should be a positive number.';
    return { status: RESOURCE_STATUS.ERROR, message };
  }

  const bigquery = new BigQuery(projectId, datasetId);
  const dataset = bigquery.getDataset();
  const { error, location } = dataset;
  if (error) {
    if (error.status === 'NOT_FOUND') {
      const locationId = getDocumentProperty('locationId');
      if (!selectedLocation && !locationId) {
        return {
          status: RESOURCE_STATUS.READY_TO_INSTALL,
          message: `Select the location of dataset[${datasetId}] and click 'TO_APPLY' to create it.`,
        };
      }
      const targetLocation = selectedLocation ||
        getLocationListName(gcloud.getBigQueryDefaultLocationObject_(locationId));
      locationAttr.attributeValue = targetLocation;
      return {
        status: RESOURCE_STATUS.READY_TO_INSTALL,
        attributes,
        message: `Will create a dataset[${datasetId}] at ${targetLocation}`,
      };
    } else {
      return {
        status: RESOURCE_STATUS.ERROR,
        message: `Dataset[${datasetId}] status: ${error.status}`,
      };
    }
  }
  updateDcoumentPropertyValue(`${resource.propertyName}Location`, location);
  const currentLocation =
    getLocationListName(getLocationObject(BIGQUERY_LOCATIONS, location));
  locationAttr.attributeValue = currentLocation;
  const result = {
    status: RESOURCE_STATUS.OK,
    attributes,
    message: currentLocation !== selectedLocation ?
      'Updated location to the existing dataset.\n' : '',
  };
  if (partitionExpirationMs !== dataset['defaultPartitionExpirationMs']) {
    result.status = RESOURCE_STATUS.READY_TO_INSTALL;
    result.message +=
      `Apply to update partition expiration time (only affects for new time-partitioned tables).`;
    const currentMs = dataset['defaultPartitionExpirationMs'];
    if (!currentMs) {
      result.message += ' There is not partition expiration setting now.';
    } else {
      result.message +=
        ` Currently it is ${(currentMs - 0) / 1000 / 3600 / 24} days.`;
    }
  }
  return result;
}

/**
 * Creates or updates the BigQuery dataset.
 * Used property: projectId.
 *
 * @param {string} datasetId Dataset name.
 * @param {{attributeValue:(string|undefined)}} resource The `attributeValue`
 *   is the location of dataset.
 * @return {!CheckResult}
 */
gcloud.createOrUpdateDataset = (datasetId, resource) => {
  const projectId = getDocumentProperty('projectId');

  const { attributesMap } = resource;
  const attrrbuteNames = ATTRIBUTE_NAMES.bigquery;
  const partitionExpirationDayAttr
    = attributesMap[attrrbuteNames.partitionExpiration];
  const locationAttr = attributesMap[attrrbuteNames.location];

  const partitionExpirationMs =
    (partitionExpirationDayAttr.attributeValue - 0) * 24 * 3600 * 1000;
  const selectedLocation = locationAttr.attributeValue;

  const locationId = getLocationId(selectedLocation);
  if (!locationId) {
    return {
      status: RESOURCE_STATUS.READY_TO_INSTALL,
      message: 'Select location of the dataset before continue.',
    };
  }
  const bigquery = new BigQuery(projectId, datasetId);
  const response = bigquery.createOrUpdateDataset({
    location: locationId,
    defaultPartitionExpirationMs: partitionExpirationMs,
  });
  const { error, id } = response;
  if (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
  updateDcoumentPropertyValue(`${resource.propertyName}Location`, locationId);
  return {
    status: RESOURCE_STATUS.OK,
    message: `Dataset[${id}] was created/updated.`,
  };
}

/**
 * Returns the status of a Cloud Functions.
 * @param {*} response Response of get or deploy the Cloud Functions.
 * @param {*} expectedVersion Expected version of the Cloud Functions. The value
 *   is from the Cloud Functions enviroment variable 'VERSION'.
 * @return {!CheckResult}
 * @private
 */
gcloud.getCloudFunctionsResult_ = (response, expectedVersion) => {
  const { status, updateTime, environmentVariables, serviceAccountEmail: sa }
    = response;
  const { VERSION: currentVersion = '' } = environmentVariables;
  const message = `v${currentVersion} was deployed at ${updateTime}.`;
  if (status !== 'ACTIVE') {
    return {
      status: RESOURCE_STATUS.READY_TO_INSTALL,
      attributeValue: '',
      message: `Wrong status: ${status}. ${message}`,
    };
  }
  if (typeof expectedVersion === 'undefined'
    || expectedVersion === currentVersion) {
    return {
      status: RESOURCE_STATUS.OK,
      attributeValue: currentVersion,
      message: `${message}`,
    };
  }
  return {
    status: RESOURCE_STATUS.READY_TO_INSTALL,
    attributeValue: currentVersion,
    message: `${message} Will deploy v${expectedVersion}`,
  };
}

/**
 * Checks the status and version of a Cloud Functions.
 * This is not a 'default' gcloud funtion as a Mojo checking function because
 * the expected version can not get by default. This is used in 'Tentacles' and
 * 'Sentinel' Mojo sheet.
 * @param {string} name The name of a Cloud Functions.
 * @param {string} expectedVersion The extpected version.
 * @return {!CheckResult}
 */
gcloud.checkCloudFunctions = (name, expectedVersion) => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const cloudFunctions = new CloudFunctions(projectId, locationId);
  const response = cloudFunctions.getFunction(name);
  const { error } = response;
  if (error) {
    if (error.status === 'NOT_FOUND') {
      return {
        status: RESOURCE_STATUS.READY_TO_INSTALL,
        attributeValue: '',
        message: `Not installed yet.`,
      };
    }
    return {
      status: RESOURCE_STATUS.ERROR,
      attributeValue: '',
      message: error.message,
    };
  }
  return gcloud.getCloudFunctionsResult_(response, expectedVersion);
}

/**
 * Returns the result (CheckResult) of an operation of Cloud Functions
 * deployment.
 * Deploying a Cloud Functions depends on the solution (Prime Solution), so
 * there is not a general 'enable' function for the resource 'Cloud Functions'.
 * Class 'Tentacles' and 'Sentinel' offer the functions to deploy related
 * Cloud Functions. After deployment started, this function is used to generate
 * a unified result based on the operation name.
 * @param {string} operationName Operation name of deploying a Cloud Functions.
 * @param {string} locationId
 * @param {string} projectId
 * @return {!CheckResult}
 */
gcloud.getCloudFunctionDeployStatus = (operationName, locationId, projectId) => {
  const cloudFunctions = new CloudFunctions(projectId, locationId);
  const rawResponse = cloudFunctions.getOperation(operationName);
  const { done, error, response } = rawResponse;
  if (done) {
    if (error) {
      return {
        status: RESOURCE_STATUS.ERROR,
        attributeValue: '',
        message: error.message,
      };
    }
    return gcloud.getCloudFunctionsResult_(response);
  }
  return {
    status: '',
    message: `Deploying `,
    refreshFn: () => {
      return gcloud.getCloudFunctionDeployStatus(
        operationName, locationId, projectId
      );
    },
  };
}

/**
 * Checks or creates/updates Logs Router Sink.
 * There is not a general 'enable' function for the resource 'Logs Router' as
 * the options and roles might differ. This is used by the Mojo sheets of
 * 'Tentacles' and 'Sentinel'.
 * Used property: projectId.
 * @param {string} name Logs Router Sink name.
 * @param {object} options
 * @param {string} role The role that Router Sink's service account needs.
 * @return {!CheckResult}
 */
gcloud.checkOrCreateLogsRouter = (name, options, role) => {
  const projectId = getDocumentProperty('projectId');
  const logging = new Logging(projectId);
  const { dataset, topic } = options;
  let checkedTarget;
  if (dataset) {
    checkedTarget = `bigquery.googleapis.com/projects/${projectId}/datasets/${dataset}`;
  } else if (topic) {
    checkedTarget = `pubsub.googleapis.com/projects/${projectId}/topics/${topic}`;
  } else {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: `Failed to create/update log sink: ${options}`,
    };
  }
  let sink = logging.getSink(name);
  const { error } = sink;
  if (error) {
    if (error.status !== 'NOT_FOUND') {
      return {
        status: RESOURCE_STATUS.ERROR,
        message: `Status: ${error.status}`,
      };
    }
    sink = logging.createSink(name, options);
  } else {
    const { destination, filter } = sink;
    if (filter !== options.filter || destination !== checkedTarget) {
      sink = logging.updateSink(name, options);
    }
  }
  if (sink.error) {//Failed to create or update.
    return {
      status: RESOURCE_STATUS.ERROR,
      message: `Failed to create/update log sink: ${sink.error.message}`,
    };
  }
  const { writerIdentity } = sink;
  const resourceManager = new CloudResourceManager(projectId);
  const roles = resourceManager.getIamPolicyForMember(writerIdentity);
  if (roles.indexOf(role) > -1) {
    return {
      status: RESOURCE_STATUS.OK,
      attributeValue: writerIdentity,
    };
  }
  const updateIamPolicy =
    resourceManager.addIamPolicyBinding(writerIdentity, role);
  if (updateIamPolicy) {
    return {
      status: RESOURCE_STATUS.OK,
      attributeValue: writerIdentity,
      message: `Granted ${role} to the default service account.`,
    };
  }
  return {
    status: RESOURCE_STATUS.ERROR,
    attributeValue: writerIdentity,
    message: `Failed to grant ${role} to the default service account.`,
  };
}

/**
 * Gets the service account information based on default properties.
 * Used property: projectId, locationId, namespace.
 * @return {!CheckResult}
 * @private
 */
gcloud.getServiceAccount_ = () => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const namespace = getDocumentProperty('namespace');
  const cloudFunctions = new CloudFunctions(projectId);
  const { functions = [] } = cloudFunctions.listFunctions(locationId);
  const regex = new RegExp(
    `^projects/${projectId}/locations/([\\w-]+)/functions/${namespace}[\\w-]+$`);
  const cloudFunction = functions.filter(({ name }) => regex.test(name))[0];
  if (!cloudFunction) {
    return {
      status: RESOURCE_STATUS.READY_TO_INSTALL,
      value: '',
      attributeValue: '',
      message: `Cloud Functions hasn't been deployed.`,
    };
  }
  const { serviceAccountEmail } = cloudFunction;
  let sa = serviceAccountEmail;
  if (!sa) {
    console.warn('There is no service account. Cloud Functions bug? ');
    return {
      status: RESOURCE_STATUS.ERROR,
      value: sa,
      message: 'There is no service account. Cloud Functions bug?' +
        ' Delete the Cloud Functions in Google Cloud Console and try again.'
    };
  }
  return {
    status: RESOURCE_STATUS.OK,
    value: sa,
  };
}

/**
 * Returns the roles that Cloud Functions' service account needs. To simplify
 * the result, it expects the service account has 'editor' role and 'Secret
 * Manager Secret Accessor' role if 'Secret Manager' was enrolled in this
 * solution.
 * @return {!Array<string>}
 * @private
 */
gcloud.getServiceAccountRoles_ = () => {
  if (SOLUTION_MENUS.some(({ sheetName }) => sheetName === 'Secret Manager')) {
    return [
      'roles/editor',
      'roles/secretmanager.secretAccessor',
    ];
  }
  return ['roles/editor'];
}

/**
 * Returns whether the default service account of Cloud Functions has expected
 * roles.
 * Used property: projectId.
 * @return {!CheckResult}
 */
gcloud.checkServiceAccountRole = () => {
  const result = gcloud.getServiceAccount_();
  if (result.status !== RESOURCE_STATUS.OK) return result;
  const expectedRoles = gcloud.getServiceAccountRoles_();
  const sa = result.value;
  const projectId = getDocumentProperty('projectId');
  const resourceManager = new CloudResourceManager(projectId);
  const roles = resourceManager.getIamPolicyForMember(`serviceAccount:${sa}`);
  if (!expectedRoles.every((expected) => roles.indexOf(expected) > -1)) {
    return {
      status: RESOURCE_STATUS.READY_TO_INSTALL,
      value: sa,
      message: 'There are missing roles for the service account. ',
    };
  }
  return result;
}

/**
 * Sets the Cloud Functions' default service account with expected roles.
 * Used property: projectId.
 * @param {string} sa Service account.
 * @return {!CheckResult}
 */
gcloud.setServiceAccountRole = (sa) => {
  if (!sa) return gcloud.checkServiceAccountRole();
  const projectId = getDocumentProperty('projectId');
  const resourceManager = new CloudResourceManager(projectId);
  const roles = gcloud.getServiceAccountRoles_();;
  const updateIamPolicy =
    resourceManager.addIamPolicyBinding(`serviceAccount:${sa}`, roles);
  return {
    status: updateIamPolicy ? RESOURCE_STATUS.OK : RESOURCE_STATUS.ERROR,
    message: `Operation is ${updateIamPolicy ? 'succeeded' : 'failed'}.`,
  };
};

/**
 * Checks the status of a specified secret of Secret Manager.
 * @param {string} name Name of secret.
 * @return {!CheckResult}
 */
gcloud.checkSecretManager = (name) => {
  const projectId = getDocumentProperty('projectId');
  const secretManager = new SecretManager(projectId);
  const { error, state } = secretManager.getSecretVersion(name);
  if (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
  if (state !== 'ENABLED') {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: `Secret state ${state}.`,
    };
  }
  return {
    status: RESOURCE_STATUS.OK,
  }
};

/**
 *
 * Used property: projectId, dataset.
 * @param {string} tables Table names with ',' as the splitter.
 * @param {strint=} datasetId Dataset Id, by default is the value of property
 *   'dataset'.
 * @return {!CheckResult}
 */
gcloud.checkExpectedTables = (tables,
  datasetId = getDocumentProperty('dataset')) => {
  const projectId = getDocumentProperty('projectId');
  const bigquery = new BigQuery(projectId, datasetId);
  const missedTables = tables.split(',').filter((table) => !!table.trim())
    .filter((table) => !bigquery.existTable(table.trim()));
  if (missedTables.length > 0) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: `Expected table(s) do not exist: ${missedTables.join(', ')}`,
    };
  }
  return {
    status: RESOURCE_STATUS.OK,
    message: `Expected table(s) exist.`,
  };
}

/**
 * Create the BigQuery views based on a given sql. It will check whehter the
 * expected tables (resource.attributeValue) exist and replace the parameters in
 * the sql.
 * @param {string} viewsSql
 * @param {!Object} resource
 * @param {string=} datasetId By default, it is the value of property 'dataset'.
 * @param {function=} parameterReplacer A function to replace parameters in the
 *   sql.
 * @param {Object=} parameters The parameter map object.
 * @returns
 */
gcloud.createBigQueryViews = (viewsSql, resource,
  datasetId = getDocumentProperty('dataset'),
  parameterReplacer = replaceParameters,
  parameters = PropertiesService.getDocumentProperties().getProperties()) => {
  const projectId = getDocumentProperty('projectId');
  const bigquery = new BigQuery(projectId, datasetId);
  const result = gcloud.checkExpectedTables(resource.attributeValue || '',
    datasetId);
  if (result.status !== RESOURCE_STATUS.OK) return result;
  const query = getExecutableSql(viewsSql, parameters, parameterReplacer);
  try {
    bigquery.runQueryJob(query);
    return {
      status: RESOURCE_STATUS.OK,
    };
  } catch (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
};

/**
 * Returns the status (CheckResult) of a data transfer config. After creating or
 * updating a data transfer config, this functions will check the jobs status of
 * the config.
 * Used property: projectId.
 * @param {string} transferConfigId
 * @return {!CheckResult}
 */
gcloud.getTransferStatus = (transferConfigId) => {
  const projectId = getDocumentProperty('projectId');
  const dataTransfer = new BigQueryDataTransfer(projectId);
  const { updateTime, displayName } = dataTransfer.getTransferConfig(transferConfigId);
  const { transferRuns = [] } = dataTransfer.listRuns(transferConfigId);
  const postUpdatedRuns = transferRuns.filter(({ startTime }) => {
    return Date.parse(startTime) > Date.parse(updateTime);
  }).sort(({ startTime }) => 0 - startTime);
  console.log(displayName, postUpdatedRuns.map(({ startTime }) => startTime));
  if (postUpdatedRuns.some(({ state }) => state === 'SUCCEEDED')) {
    return {
      status: RESOURCE_STATUS.OK,
      message: `Data Transfer ended successfully.`,
    };
  }
  if (postUpdatedRuns.length > 0) {
    const { name: jobId, state } = transferRuns[0];
    if (state === 'FAILED') {
      const { transferMessages = [] } = dataTransfer.listRunsLogs(jobId);
      const message =
        transferMessages.filter(({ severity }) => severity === 'ERROR')
          .map(({ messageText }) => messageText).join('\n');
      return {
        status: RESOURCE_STATUS.ERROR,
        message,
      }
    }
  }
  return {
    status: '',
    message: `Data Transfer is running.`,
    refreshFn: () => {
      return gcloud.getTransferStatus(transferConfigId);
    },
  };
};

/**
 * Creates or updates a data transfer config. A config will run automatically
 * when it is created here. The function will run the transfer if it is updated.
 * Used property: projectId.
 * @param {object} targetConfig Transfer config to be created or updated.
 * @param {string} datasetId BigQuery dataset, it is also used to determine the
 *   location.
 * @param {function} filterFn The function that is used to filter out the data
 *   transfer config. If there was a config found, it will be updated.
 * @param {string?} authorizationCode Optional OAuth2 authorization code.
 * @return {!CheckResult}
 */
gcloud.createOrUpdateDataTransfer =
  (targetConfig, datasetId, filterFn, authorizationCode) => {
    const projectId = getDocumentProperty('projectId');
    const bigquery = new BigQuery(projectId, datasetId);
    const { error, location } = bigquery.getDataset();
    if (error) {
      throw new Error(error.message);
    }
    const dataTransfer = new BigQueryDataTransfer(projectId, location);
    const { dataSourceId } = targetConfig;
    let existingTransferConfig;
    const { transferConfigs = [] } = dataTransfer.listTransferConfigs(
      { dataSourceIds: dataSourceId });
    existingTransferConfig = transferConfigs.filter(filterFn)[0];
    let transferConfig;
    let updatedConfig;
    let needManualRuns = false; //Manually trigger data transfer after update.
    if (existingTransferConfig && existingTransferConfig.name) {
      updatedConfig = diffObjects(existingTransferConfig, targetConfig);
      if (!updatedConfig) { // No need to update.
        return gcloud.getTransferStatus(existingTransferConfig.name);
      }
    }
    let queryString;
    if (dataTransfer.needsAuthorizationCode(dataSourceId)) {
      if (!authorizationCode) {
        const url = dataTransfer.getAuthUrlForDataSource(dataSourceId)
        throw new Error('Visit following link and complete authorization ' +
          'process, copy the authorization code to the cell "Attribute Value"' +
          ' and try again:\n\n' + url);
      }
      queryString = { authorizationCode };
    }
    if (updatedConfig) {
      // This API does't support partially update `params`.
      if (updatedConfig.params) updatedConfig.params = targetConfig.params;
      console.log(`Need update ${existingTransferConfig.name}`, updatedConfig);
      transferConfig = dataTransfer.updateTransferConfig(
        existingTransferConfig.name, updatedConfig, queryString);
      needManualRuns = true;
    } else {
      console.log('Create Data Transfer');
      transferConfig = dataTransfer.createTransferConfig(
        targetConfig, queryString);
    }
    if (transferConfig.error) {
      throw new Error(transferConfig.error.message);
    }
    const transferConfigId = transferConfig.name;
    if (needManualRuns) {
      console.log('Manually run Data Transfer', transferConfigId);
      dataTransfer.startManualRuns(transferConfigId);
    }
    return gcloud.getTransferStatus(transferConfigId);
  };

/**
 * Creates or updates a scheduled query which is a kind of data transfer config.
 * This is a facade function based on `gcloud.createOrUpdateDataTransfer` to
 * help create/update scheduled queries.
 * Used property: projectId.
 * @param {string} displayName
 * @param {string} datasetId
 * @param {string} query
 * @param {string} authorizationCode
 * @return {!CheckResult}
 */
gcloud.createOrUpdateScheduledQuery = (displayName, datasetId, query,
  authorizationCode) => {
  const config = {
    dataSourceId: DATA_TRANSFER_SOURCE.SCHEDULED_QUERY,
    displayName,
    params: { query },
  };
  const filterFn = (transferConfig) => {
    const { dataSourceId, displayName } = transferConfig;
    return dataSourceId === config.dataSourceId
      && displayName === config.displayName;
  };
  return gcloud.createOrUpdateDataTransfer(
    config, datasetId, filterFn, authorizationCode);
}

/**
 * Loads data into a BigQuery table.
 * @see LOAD_CSV_DEFAULT_OPTION for the default loading options.
 * @param {string} tableId
 * @param {string} csvContent
 * @param {string} datasetId
 * @param {Object|undefined} options BigQuery load job options.
 * @return {!CheckResult}
 */
gcloud.loadDataToBigQuery = (tableId, csvContent, datasetId, options) => {
  const projectId = getDocumentProperty('projectId');
  const bigquery = new BigQuery(projectId, datasetId);
  const { error } = bigquery.getDataset();
  if (error) {
    throw new Error(error.message);
  }
  const bigQuery = new BigQuery(projectId, datasetId, options);
  try {
    const response = bigQuery.loadData(tableId, csvContent);
    return {
      status: RESOURCE_STATUS.OK,
      message: `Job Id: ${response.jobId}`,
    };
  } catch (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
};

/**
 * Gets an ExternalApiAuthOption based on the given secret name.
 * If the secretName was given, the OAuth token will be loaded from Secret
 * Manager. Otherwise, it will try to get the default service account.
 * This is used for external Apis to do authorization.
 * Used property: projectId.
 * @param {string|undefined} secretName
 * @return {!ExternalApiAuthOption}
 */
gcloud.getAuthOption = (secretName) => {
  const option = {};
  if (!secretName) {
    const { value: serviceAccount } = gcloud.getServiceAccount_();
    if (!serviceAccount) {
      throw new Error('Can not find service account.');
    }
    option.serviceAccount = serviceAccount;
  } else {
    const projectId = getDocumentProperty('projectId');
    const secretManager = new SecretManager(projectId);
    const { error, data } = secretManager.accessSecret(secretName);
    if (error) {
      throw new Error(`Fail to access secret: ${secretName}`);
    }
    option.oauth = JSON.parse(data);
  }
  return option;
};

/**
 * Returns the Api access checking result in a unified way. This is used by
 * Tentacles Api config sheet and Sentinel access checking sheet.
 * @param {string|undefined} secretName
 * @param {function(!ExternalApiAuthOption):!VerifyResult} getCheckResult
 * @return {!VerifyResult}
 */
gcloud.getAccessCheckResult = (secretName, getCheckResult) => {
  const label = secretName
    ? `Auth with Secret Manage [name: ${secretName}]`
    : 'Auth with the default service account';
  try {
    const option = gcloud.getAuthOption(secretName);
    const result = getCheckResult(option);
    result.label = label;
    return result;
  } catch (error) {
    const result = {
      valid: false,
      label,
      reason: error.message,
    };
    return result;
  }
};

/**
 * Checks the access to the specified Google Sheets for the given service
 * account. Returns CheckResult if there was an error.
 *
 * @param {string} serviceAccount
 * @param {string} spreadsheetUrl
 * @return {!CheckResult}
 */
gcloud.checkSaAccessToSheet = (serviceAccount, spreadsheetUrl) => {
  const spreadsheetId = getSpreadsheetIdFromUrl(spreadsheetUrl);
  try {
    const option = gcloud.getAuthOption();
    const { valid, reason } = new Sheets(option).verifySpreadsheet(spreadsheetId);
    if (!valid) {
      const message = reason === 'The caller does not have permission'
        ? `Add ${serviceAccount} as a viewer of the Google Sheets` : reason;
      return {
        status: RESOURCE_STATUS.ERROR,
        message,
      };
    }
  } catch (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
};

/**
 * Creates or updates the BigQuery external table based on given Google Sheets
 * information.
 * It will use the default property 'projectId' and 'dataset'.
 *
 * @param {string} sheetsUrl
 * @param {string} sheetName
 * @param {!Array<{name:string, type:string}>} schema BigQuery table schema.
 * @param {string} tableName
 * @return {!CheckResult}
 */
gcloud.updateExternalTable = (sheetsUrl, sheetName, schema, tableName) => {
  const projectId = getDocumentProperty('projectId');
  const datasetId = getDocumentProperty('dataset');
  const bigquery = new BigQuery(projectId, datasetId);
  const tableId = tableName || snakeize(sheetName);
  const tableConfig = {
    type: 'EXTERNAL',
    tableReference: { tableId },
    externalDataConfiguration: {
      sourceUris: [sheetsUrl],
      sourceFormat: 'GOOGLE_SHEETS',
      schema: { fields: schema },
      googleSheetsOptions: {
        skipLeadingRows: '1',
        range: sheetName,
      },
    },
  };
  const { error: bigQueryError } = bigquery.createOrUpdateTable(tableConfig);
  if (bigQueryError) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: bigQueryError.error,
    };
  }
  return {
    status: RESOURCE_STATUS.OK,
  };
};

/**
 * Checks the target Google Sheets with given url and sheet name. Initializes it
 * if it doesn't exist. Then creates or updates the definition of external table
 * in BigQuery.
 *
 * @param {!ExternalTableSheet} externalTableSheet
 * @param {string} sheetsUrl
 * @return {!CheckResult}
 */
gcloud.checkOrInitializeExternalTable = (externalTableSheet, sheetsUrl) => {
  const { sheetName } = externalTableSheet;
  const targetSheet =
    SpreadsheetApp.openByUrl(sheetsUrl).getSheetByName(sheetName);
  let message;
  if (!targetSheet) {
    externalTableSheet.initialize(sheetsUrl);
    message = `Initialized target sheet: ${sheetName}`;
  } else {
    message = `Target sheet: ${sheetName} exists.`;
  }
  const schema = getBigQuerySchema(externalTableSheet.fields);
  const externalTableResult = gcloud.updateExternalTable(
    sheetsUrl, sheetName, schema, externalTableSheet.getTableName());
  return Object.assign({ message }, externalTableResult);
};
