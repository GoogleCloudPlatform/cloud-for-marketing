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
 * Gets the default location for a BigQuery dataset based on the property
 * 'locationId'. The property 'locationId' is selected by users for Cloud
 * Functions/Storage. For better performance, a default BigQuery dataset
 * location is decided to be the same or close to that location.
 * @param {string} locationId
 * @return {{displayName:string, locationId: string}}
 * @private
 */
MojoFunctions.getBigQueryDefaultLocationObject_ = locationId => {
  let id;
  if (locationId.startsWith('europe-')) {
    id = 'EU';
  } else if (locationId.startsWith('us-')) {
    id = 'US';
  } else {
    const target = BIGQUERY_LOCATIONS.filter(
      ({locationId: id}) => id === locationId,
    )[0];
    id = target ? target.locationId : 'US';
  }
  return getLocationObject(BIGQUERY_LOCATIONS, id);
};

/**
 * Checks BigQuery dataset. If it needs to be created or updated, it
 * returns a status `TO_APPLY` to ask users to continue with the menu
 * `Apply changes` to make those changes.
 * Used property: projectId, locationId.
 *
 * @param {string} datasetId Dataset name.
 * @param {!Object} resource
 * @return {!CheckResult}
 */
MojoFunctions.checkDataset = (datasetId, resource) => {
  const projectId = getDocumentProperty('projectId');

  const {attributes, attributesMap} = resource;
  const attributeNames = BigQuery.ATTRIBUTE_NAMES;
  const locationAttr = attributesMap[attributeNames.location];
  const selectedLocation = locationAttr.attributeValue;

  const bigquery = new BigQuery(projectId, datasetId);
  const dataset = bigquery.getDataset();
  const {error, location} = dataset;
  if (error) {
    if (error.status === 'NOT_FOUND') {
      const locationId = getDocumentProperty('locationId');
      if (!selectedLocation && !locationId) {
        return {
          status: RESOURCE_STATUS.TO_APPLY,
          message: `Select the location of dataset[${
            datasetId
          }] and click 'TO_APPLY' to create it.`,
        };
      }
      const targetLocation =
        selectedLocation ||
        getLocationListName(
          MojoFunctions.getBigQueryDefaultLocationObject_(locationId),
        );
      locationAttr.attributeValue = targetLocation;
      return {
        status: RESOURCE_STATUS.TO_APPLY,
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
  updateDocumentPropertyValue(`${resource.propertyName}Location`, location);
  const currentLocation = getLocationListName(
    getLocationObject(BIGQUERY_LOCATIONS, location),
  );
  locationAttr.attributeValue = currentLocation;
  const result = {
    status: RESOURCE_STATUS.OK,
    status_link: bigquery.getLinkToCloudConsole(),
    attributes,
    message:
      currentLocation !== selectedLocation
        ? 'Updated location to the existing dataset.\n'
        : '',
  };
  /**
   * Based on the expiration days' setting and current value, updates the
   * message for the user to tell what kind of change will be applied.
   */
  const updateResultForExpiration = expirationType => {
    if (!attributesMap[attributeNames[expirationType]]) {
      console.log('There is no BQ expire setting for', expirationType);
      return;
    }
    const {attributeValue: expectedExpirationDay} =
      attributesMap[attributeNames[expirationType]];
    const expectedMs = (expectedExpirationDay - 0) * 24 * 3600 * 1000;
    if (expectedMs < 0) {
      result.message = `${expirationType} should be a non-negative number.`;
      result.status = RESOURCE_STATUS.ERROR;
      return;
    }
    // Change the first letter to upper case for expiration to generate the
    // property name in 'Dataset'
    const typeName =
      expirationType.charAt(0).toUpperCase() + expirationType.slice(1);
    const propertyName = `default${typeName}Ms`;
    const currentMs = dataset[propertyName] || 0;
    const currentDays = currentMs / 1000 / 3600 / 24;
    if (expectedMs > 0) {
      if (expectedMs !== currentMs - 0) {
        result.status = RESOURCE_STATUS.TO_APPLY;
        result.message += `Apply to update ${expirationType} (only affects for new tables).`;
        if (currentDays === 0) {
          result.message += ' There is no expiration setting now.';
        } else {
          result.message += ` Currently it is ${currentDays} days.`;
        }
      }
    } else if (currentMs > 0) {
      result.status = RESOURCE_STATUS.TO_APPLY;
      result.message += `Will remove ${expirationType} setting.`;
      result.message += ` Currently it is ${currentDays} days.`;
    }
    result.message += '\n';
  };
  updateResultForExpiration('partitionExpiration');
  updateResultForExpiration('tableExpiration');
  if (result.message.endsWith('\n')) {
    result.message = result.message.slice(0, -1);
  }
  return result;
};

/**
 * Creates or updates the BigQuery dataset.
 * Used property: projectId.
 *
 * @param {string} datasetId Dataset name.
 * @param {{attributeValue:(string|undefined)}} resource The `attributeValue`
 *   is the location of dataset.
 * @return {!CheckResult}
 */
MojoFunctions.createOrUpdateDataset = (datasetId, resource) => {
  const projectId = getDocumentProperty('projectId');

  const {attributesMap} = resource;
  const attributeNames = BigQuery.ATTRIBUTE_NAMES;
  const locationAttr = attributesMap[attributeNames.location];
  const selectedLocation = locationAttr.attributeValue;
  const locationId = getLocationId(selectedLocation);
  if (!locationId) {
    return {
      status: RESOURCE_STATUS.TO_APPLY,
      message: 'Select location of the dataset before continue.',
    };
  }
  const config = {location: locationId};

  /**
   * Based on the expiration days' setting and current value, updates the
   * config to create or update the dataset.
   */
  const updateConfigWithExpiration = expirationType => {
    if (!attributesMap[attributeNames[expirationType]]) {
      console.log('There is no BQ expire setting for', expirationType);
      return;
    }
    const {attributeValue: expectedExpirationDay} =
      attributesMap[attributeNames[expirationType]];
    const expectedMs = (expectedExpirationDay - 0) * 24 * 3600 * 1000;
    // Change the first letter to upper case for expiration to generate the
    // property name in 'Dataset'
    const typeName =
      expirationType.charAt(0).toUpperCase() + expirationType.slice(1);
    const propertyName = `default${typeName}Ms`;
    // '0' is not allowed by BigQuery API ATM.
    config[propertyName] = expectedMs === 0 ? null : expectedMs.toString();
    console.log(
      `update ${propertyName} to ${expectedExpirationDay} days`,
      expectedMs,
    );
  };
  updateConfigWithExpiration('partitionExpiration');
  updateConfigWithExpiration('tableExpiration');
  const bigquery = new BigQuery(projectId, datasetId);
  const response = bigquery.createOrUpdateDataset(config);
  const {error, id} = response;
  if (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
  updateDocumentPropertyValue(`${resource.propertyName}Location`, locationId);
  return {
    status: RESOURCE_STATUS.OK,
    status_link: bigquery.getLinkToCloudConsole(),
    message: `Dataset[${id}] was created/updated.`,
  };
};

/**
 * Loads data into a BigQuery table from an HTTPS URL or a Cloud Storage path.
 * If the resource specifies an HTTPS URL, it fetches the content and loads it
 * directly. Otherwise, it treats the resource value as a relative path in the
 * default Cloud Storage bucket and creates a BigQuery load job.
 *
 * @param {string} tableId The target BigQuery table ID.
 * @param {{attributeValue: string}} resource The resource object containing the
 *     data source URL or GCS path.
 * @return {!CheckResult} The result of the load operation.
 */
MojoFunctions.loadDataToBigQuery = (tableId, resource) => {
  const projectId = getDocumentProperty('projectId');
  const dataset = getDocumentProperty('dataset');
  const bigQuery = new BigQuery(projectId, dataset);
  const {error} = bigQuery.getDataset();
  if (error) {
    if (error.status === 'NOT_FOUND' || error.code === 404) {
      return {
        status: RESOURCE_STATUS.PENDING,
        message: `Dataset [${dataset}] is not ready yet.`,
      };
    }
    throw new Error(error.message);
  }
  const {attributesMap} = resource;
  const attributeNames = BigQuery.LOAD_JOB_ATTRIBUTE_NAMES;
  // const url = resource.attributeValue;
  const url = attributesMap[attributeNames.source].attributeValue;
  const skipLeadingRows =
    attributesMap[attributeNames.skipLeadingRows].attributeValue || 1;
  const sourceFormat =
    attributesMap[attributeNames.sourceFormat].attributeValue || 'CSV';
  const loadOption = {
    skipLeadingRows,
    sourceFormat,
  };
  const statusLink = bigQuery.getLinkToTable(tableId);

  // Check whether the target table already exists and its last modified
  // timestamp
  const tableResponse = bigQuery.getTable(tableId);
  const tableExists = !tableResponse.error && tableResponse.lastModifiedTime;
  const tableModifiedTime = tableExists
    ? Number(tableResponse.lastModifiedTime)
    : 0;

  try {
    if (url.toLowerCase().startsWith('https://')) {
      const dataResponse = UrlFetchApp.fetch(url);
      const status = dataResponse.getResponseCode();
      if (status >= 400) {
        return {
          status: RESOURCE_STATUS.ERROR,
          message: `Failed to get resource, HTTP status code: ${status}`,
          status_link: statusLink,
        };
      }

      // Check HTTP Last-Modified header if table exists
      const headers = dataResponse.getHeaders();
      const lastModifiedHeader =
        headers['Last-Modified'] || headers['last-modified'];
      if (tableExists && lastModifiedHeader) {
        const sourceModifiedTime = new Date(lastModifiedHeader).getTime();
        if (
          !isNaN(sourceModifiedTime) &&
          tableModifiedTime >= sourceModifiedTime
        ) {
          return {
            status: RESOURCE_STATUS.OK,
            message: `Table [${
              tableId
            }] is already up-to-date (data was loaded previously).`,
            status_link: statusLink,
          };
        }
      }

      const data = dataResponse.getContentText();
      const response = bigQuery.loadDirectData(tableId, data, loadOption);
      if (response && response.error) {
        return {
          status: RESOURCE_STATUS.ERROR,
          message: response.error.message,
          status_link: statusLink,
        };
      }
      return {
        status: RESOURCE_STATUS.OK,
        message: `Job Id: ${response.jobId}`,
        status_link: statusLink,
      };
    } else {
      let bucket, fileName;
      if (url.startsWith('gs://')) {
        const paths = MojoFunctions.getBucketAndFilename_(url);
        bucket = paths.bucket;
        fileName = paths.fileName;
      } else {
        bucket = getDocumentProperty('bucket');
        fileName = url;
      }

      // Check GCS source file modification timestamp
      const storage = new Storage(projectId);
      const objectMeta = storage.getObjectMetadata(bucket, fileName);
      if (
        objectMeta.error &&
        (objectMeta.error.code === 404 || objectMeta.error.status === 'NOT_FOUND')
      ) {
        return {
          status: RESOURCE_STATUS.PENDING,
          message: `Source file gs://${bucket}/${fileName} is not ready yet.`,
        };
      }
      if (tableExists && !objectMeta.error && objectMeta.updated) {
        const sourceModifiedTime = new Date(objectMeta.updated).getTime();
        if (tableModifiedTime >= sourceModifiedTime) {
          return {
            status: RESOURCE_STATUS.OK,
            message: `Table [${
              tableId
            }] is already up-to-date (last updated at ${new Date(
              tableModifiedTime,
            ).toISOString()}, source updated at ${objectMeta.updated}).`,
            status_link: statusLink,
          };
        }
      }

      const response = bigQuery.loadStorageFile(
        tableId,
        `gs://${bucket}/${fileName}`,
        loadOption,
      );
      if (response && response.error) {
        return {
          status: RESOURCE_STATUS.ERROR,
          message: response.error.message,
          status_link: statusLink,
        };
      }
      return {
        status: RESOURCE_STATUS.OK,
        message: `Job Id: ${response.jobId}`,
        status_link: statusLink,
      };
    }
  } catch (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
      status_link: statusLink,
    };
  }
};

/**
 * Used property: projectId, dataset.
 * @param {string} tables Table names with ',' as the splitter.
 * @param {string=} datasetId Dataset Id, by default is the value of property
 *   'dataset'.
 * @return {!CheckResult}
 */
MojoFunctions.checkExpectedTables = (
  tables,
  datasetId = getDocumentProperty('dataset'),
) => {
  const projectId = getDocumentProperty('projectId');
  const bigquery = new BigQuery(projectId, datasetId);
  const missedTables = tables
    .split(',')
    .filter(table => !!table.trim())
    .filter(table => !bigquery.existTable(table.trim()));
  if (missedTables.length > 0) {
    return {
      status: RESOURCE_STATUS.PENDING,
      message: `Expected table(s) do not exist: ${missedTables.join(', ')}`,
    };
  }
  return {
    status: RESOURCE_STATUS.OK,
    message: `Expected table(s) exist.`,
  };
};

/**
 * Create the BigQuery views based on a given sql. It will check whether the
 * expected tables (resource.attributeValue) exist and replace the parameters in
 * the sql.
 * @param {string} viewsSql
 * @param {!Object} resource
 * @param {string=} datasetId By default, it is the value of property 'dataset'.
 * @param {!Function=} parameterReplacer A function to replace parameters in the
 *   sql.
 * @param {!Object=} parameters The parameter map object.
 * @return {!CheckResult}
 */
MojoFunctions.createBigQueryViews = (
  viewsSql,
  resource,
  datasetId = getDocumentProperty('dataset'),
  parameterReplacer = replaceParameters,
  parameters = PropertiesService.getDocumentProperties().getProperties(),
) => {
  const projectId = getDocumentProperty('projectId');
  const bigquery = new BigQuery(projectId, datasetId);
  const result = MojoFunctions.checkExpectedTables(
    resource.attributeValue || '',
    datasetId,
  );
  if (result.status !== RESOURCE_STATUS.OK) return result;
  const query = getExecutableSql(viewsSql, parameters, parameterReplacer);
  try {
    const jobReference = bigquery.runQueryJob(query);
    const result = {
      status: RESOURCE_STATUS.OK,
    };
    // Extract view/table name(s) from SQL to generate descriptive message and
    // status link.
    const viewMatches = Array.from(
      query.matchAll(
        /CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+`?[^`.\s;]+(?:\.([^`.\s;]+))+`?/gi,
      ),
    );
    const viewNames = Array.from(new Set(viewMatches.map(m => m[1])));
    const jobIdStr =
      jobReference && jobReference.jobId
        ? ` (Job Id: ${jobReference.jobId})`
        : '';

    if (viewNames.length > 0) {
      result.message = `View(s) [${viewNames.join(
        ', ',
      )}] created/verified successfully.${jobIdStr}`;
      result.status_link = bigquery.getLinkToTable(viewNames[0]);
    } else {
      result.message = `BigQuery views created/verified successfully.${jobIdStr}`;
      result.status_link = bigquery.getLinkToCloudConsole();
    }
    return result;
  } catch (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
};

/**
 * Checks whether the expected BigQuery tables/views exist. If yes, generates
 * the link to make a copy of given dashboard.
 * @param {string} reportId Id of Data Studio dashboard.
 * @param {!Object} resource
 * @param {string=} datasetId By default, it is the value of property 'dataset'.
 * @return {!CheckResult}
 */
MojoFunctions.generateDashboardLink = (
  reportId,
  resource,
  datasetId = getDocumentProperty('dataset'),
) => {
  const projectId = getDocumentProperty('projectId');
  const result = MojoFunctions.checkExpectedTables(
    resource.attributeValue,
    datasetId,
  );
  if (result.status !== RESOURCE_STATUS.OK) {
    result.status = RESOURCE_STATUS.PENDING;
    return result;
  }
  const parameters = {
    'c.reportId': reportId,
    'c.mode': 'view',
    'ds.*.projectId': projectId,
    'ds.*.datasetId': datasetId,
    'ds.*.connector': 'bigQuery',
    'c.explain': 'true',
  };
  const dashboardLink =
    'https://datastudio.google.com/reporting/create?' +
    Object.keys(parameters)
      .map(key => `${key}=${encodeURIComponent(parameters[key])}`)
      .join('&');
  return {
    status: RESOURCE_STATUS.OK,
    message: `Click 'OK' to make a copy of the dashboard`,
    status_link: dashboardLink,
  };
};

MojoTemplateRegistry.register('bigQueryDataset', {
  category: 'Google Cloud',
  resource: 'BigQuery Dataset',
  value: '${namespace|underscored}',
  attributes: [
    {
      attributeName: BigQuery.ATTRIBUTE_NAMES.location,
      attributeValue_datarange: BIGQUERY_LOCATIONS.map(getLocationListName),
    },
    {
      attributeName: BigQuery.ATTRIBUTE_NAMES.partitionExpiration,
      attributeValue: '720',
    },
    // This should not be used in most cases. Keep it as comments for reference.
    // {
    //   attributeName: BigQuery.ATTRIBUTE_NAMES.tableExpiration,
    //   attributeValue: '0'
    // },
  ],
  propertyName: 'dataset',
  checkFn: MojoFunctions.checkDataset,
  enableFn: MojoFunctions.createOrUpdateDataset,
});

MojoTemplateRegistry.register('bigQueryDataTable', {
  category: 'Solution',
  resource: 'BigQuery Data Table',
  editType: RESOURCE_EDIT_TYPE.READONLY,
  attributes: [
    {
      attributeName: BigQuery.LOAD_JOB_ATTRIBUTE_NAMES.source,
    },
    {
      attributeName: BigQuery.LOAD_JOB_ATTRIBUTE_NAMES.skipLeadingRows,
      attributeValue: 1,
    },
    {
      attributeName: BigQuery.LOAD_JOB_ATTRIBUTE_NAMES.sourceFormat,
      attributeValue: 'CSV',
    },
  ],
  groupType: GROUP_TYPE.COLLAPSE,
  checkFn: MojoFunctions.loadDataToBigQuery,
});

MojoTemplateRegistry.register('bigQueryQueryJob', {
  category: 'Solution',
  resource: 'BigQuery Query Job',
  editType: RESOURCE_EDIT_TYPE.READONLY,
  attributeName: 'Expected table(s)',
  checkFn: MojoFunctions.createBigQueryViews,
});

MojoTemplateRegistry.register('dashboard', {
  category: 'Solution',
  resource: 'Dashboard',
  editType: RESOURCE_EDIT_TYPE.READONLY,
  attributeName: 'Expected table(s)',
  checkFn: MojoFunctions.generateDashboardLink,
});
