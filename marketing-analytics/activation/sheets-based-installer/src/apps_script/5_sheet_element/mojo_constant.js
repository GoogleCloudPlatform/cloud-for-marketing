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

/** @fileoverview MojoResource check functions (non GCP part) and templates. */

const ATTRIBUTE_NAMES = {
  firestore: {
    mode: 'Mode',
    location: 'Location',
  },
  bigquery: {
    location: 'Location',
    partitionExpiration: 'Partition Expiration (days)',
  },
}

/** Checks the namespace. */
const checkNamespace = (namespace) => {
  const regex = /^[a-z][a-z0-9_]{2,15}$/;
  const result = regex.test(namespace);
  if (result) return { status: RESOURCE_STATUS.OK };
  const message =
    'Expected 3 to 16 of lower letters, digits or underscores with a leading letter.';
  return { status: RESOURCE_STATUS.ERROR, message };
};

/** Checks the number is a positive number. */
const checkNonNegativeNumber = (number) => {
  if (number - 0 >= 0) return { status: RESOURCE_STATUS.OK };
  const message = 'Expected a positive number.';
  return { status: RESOURCE_STATUS.ERROR, message };
};

/** Checks the time zone. */
const checkTimeZone = (timeZone) => {
  const result = TIMEZONES.indexOf(timeZone) > -1;
  if (result) return { status: RESOURCE_STATUS.OK };
  const message = 'Could not find the time zone.';
  return { status: RESOURCE_STATUS.ERROR, message };
};

/** Checks the parameter. */
const checkParameter = (value, resource) => {
  if (value) return { status: RESOURCE_STATUS.OK };
  const message = `Input the value for parameter ${resource.attributeValue}`;
  return { status: RESOURCE_STATUS.ERROR, message };
};

/** Cleans the account number. */
const cleanAccountNumber = (value, resource) => {
  const regex = /^[0-9][0-9-]*[0-9]$/;
  const result = regex.test(value);
  if (result) {
    return {
      status: RESOURCE_STATUS.OK,
      value: value.replaceAll('-', ''),
    };
  }
  const message = `Only digits and dash(-) are allowed in account ID.`;
  return { status: RESOURCE_STATUS.ERROR, message };
};

/** Checks the foler name. */
const checkFolderName = (folder) => {
  const regex = /^\w+$/;
  const result = regex.test(folder);
  if (result) {
    return { status: RESOURCE_STATUS.OK };
  }
  const message = 'Expected a string of letters or digits.';
  return { status: RESOURCE_STATUS.ERROR, message };
};

/** Checks whether it is a valid Sheets URL. */
const checkSheetsUrl = (sheetUrl) => {
  if (!sheetUrl) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: 'Make a copy of the template and enter the URL of your copy.',
    };
  }
  if (!getSpreadsheetIdFromUrl(sheetUrl)) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: 'Can not get the Spreadsheet Id from the URL',
    };
  }
};

/**
 * Returns the function to get the versions of the specified package and return
 * the check results.
 *
 * @param {string} pacakgeName
 * @return {function(string):!CheckResult}
 */
const getCheckFunctionForVersion = (pacakgeName) => {
  return (version) => {
    const versions = getNpmVersions(pacakgeName);
    const latest = versions[0];
    let message = '';
    if (!version) {
      message = 'By default, will install the latest version.';
    } else if (version !== latest) {
      message = `New version ${latest} is available.`;
    }
    return {
      status: RESOURCE_STATUS.OK,
      value: version ? version : latest,
      value_datarange: versions,
      message,
    };
  }
};

/**
 * Templates for Mojo resources.
 * @const {!Object<string,!MojoResource>}
 */
const MOJO_CONFIG_TEMPLATE = {
  namespace: {
    category: 'General',
    resource: 'Namespace',
    propertyName: 'namespace',
    editType: RESOURCE_EDIT_TYPE.DEFAULT_VALUE,
    checkFn: checkNamespace,
  },
  timeZone: {
    category: 'General',
    resource: 'Time Zone',
    propertyName: 'timeZone',
    editType: RESOURCE_EDIT_TYPE.DEFAULT_VALUE,
    value_datarange: TIMEZONES,
    checkFn: checkTimeZone,
  },
  parameter: {
    category: 'General',
    resource: 'Parameter',
    editType: RESOURCE_EDIT_TYPE.USER_INPUT,
    checkFn: checkParameter,
  },
  projectId: {
    category: 'Google Cloud',
    resource: 'Project Id',
    propertyName: 'projectId',
    editType: RESOURCE_EDIT_TYPE.USER_INPUT,
    checkFn: gcloud.checkProject,
  },
  permissions: {
    category: 'Google Cloud',
    resource: 'Permissions',
    editType: RESOURCE_EDIT_TYPE.READONLY,
    attributeName: 'Predefined role',
    attributeValue: getPredefinedRole,
    checkFn: gcloud.checkPermission,
  },
  apis: {
    category: 'Google Cloud',
    resource: 'APIs',
    editType: RESOURCE_EDIT_TYPE.READONLY,
    checkFn: gcloud.checkOrEnableApi,
  },
  firestore: {
    category: 'Google Cloud',
    resource: 'Firestore',
    editType: RESOURCE_EDIT_TYPE.READONLY,
    value: DEFAULT_DATABASE,
    attributes: [
      {
        attributeName: ATTRIBUTE_NAMES.firestore.mode,
        attributeValue_datarange: Object.values(FIRESTORE_MODE),
      },
      {
        attributeName: ATTRIBUTE_NAMES.firestore.location,
      }
    ],
    propertyName: 'databaseId',
    checkFn: gcloud.checkFirestore,
    enableFn: gcloud.createFirestore,
  },
  location: {
    category: 'Google Cloud',
    resource: 'Location',
    propertyName: 'location',
    checkFn: gcloud.checkLocationByNamespace,
  },
  cloudStorage: {
    category: 'Google Cloud',
    resource: 'Cloud Storage',
    value: '${namespace}-${projectId_normalized}',
    attributeName: 'Lifecycle (days before delete)',
    propertyName: 'bucket',
    checkFn: gcloud.checkBucket,
    enableFn: gcloud.createOrUpdateBucket,
  },
  cloudFunctions: {
    category: 'Google Cloud',
    resource: 'Cloud Functions',
  },
  datasetRetention: {
    category: 'Google Cloud',
    resource: 'Dataset Expiration Days(0 for never)',
    checkFn: checkNonNegativeNumber,
  },
  bigQueryDataset: {
    category: 'Google Cloud',
    resource: 'BigQuery Dataset',
    value: '${namespace}',
    attributes: [
      {
        attributeName: ATTRIBUTE_NAMES.bigquery.location,
        attributeValue_datarange: BIGQUERY_LOCATIONS.map(getLocationListName),
      },
      {
        attributeName: ATTRIBUTE_NAMES.bigquery.partitionExpiration,
        attributeValue: '720'
      }
    ],
    propertyName: 'datasetId',
    checkFn: gcloud.checkDataset,
    enableFn: gcloud.createOrUpdateDataset,
  },
  serviceAccountRole: {
    category: 'Google Cloud',
    resource: 'Service Account',
    value: '',
    editType: RESOURCE_EDIT_TYPE.READONLY,
    checkFn: gcloud.checkServiceAccountRole,
    enableFn: gcloud.setServiceAccountRole,
    propertyName: 'serviceAccount',
  },
  secretManager: {
    category: 'Google Cloud',
    resource: 'Secret Manager',
    optionalType: OPTIONAL_TYPE.DEFAULT_UNCHECKED,
    checkFn: gcloud.checkSecretManager,
    propertyName: 'defaultSecretName',
  },
  externalTable: {
    category: 'Configuration',
    resource: 'Sheet URL',
    editType: RESOURCE_EDIT_TYPE.USER_INPUT,
    attributeName: 'Template',
  },
};
