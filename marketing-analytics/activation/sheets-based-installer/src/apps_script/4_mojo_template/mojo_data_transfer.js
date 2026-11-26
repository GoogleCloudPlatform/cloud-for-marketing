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
 * Returns the status (CheckResult) of a data transfer config. After creating or
 * updating a data transfer config, this functions will check the jobs status of
 * the config.
 * Used property: projectId.
 * @param {string} transferConfigId
 * @return {!CheckResult}
 */
MojoFunctions.getTransferStatus = transferConfigId => {
  const projectId = getDocumentProperty('projectId');
  const dataTransfer = new BigQueryDataTransfer(projectId);
  const {updateTime, displayName} =
    dataTransfer.getTransferConfig(transferConfigId);
  const transferRuns = dataTransfer.listRuns(transferConfigId);
  const postUpdatedRuns = transferRuns
    .filter(({startTime}) => {
      return Date.parse(startTime) > Date.parse(updateTime);
    })
    .sort(({startTime}) => 0 - startTime);
  console.log(
    `Data Transfer '${displayName}' updated at ${updateTime}, has runs:`,
    postUpdatedRuns.map(({startTime}) => startTime),
  );
  const transferLink = dataTransfer.getLinkToCloudConsole(transferConfigId);
  if (postUpdatedRuns.some(({state}) => state === 'SUCCEEDED')) {
    return {
      status: RESOURCE_STATUS.OK,
      message: `Data Transfer ended successfully.`,
      status_link: transferLink,
    };
  }
  if (postUpdatedRuns.length > 0) {
    const {name: jobId, state} = transferRuns[0];
    if (state === 'FAILED') {
      const transferMessages = dataTransfer.listRunsLogs(jobId);
      const message = transferMessages
        .filter(({severity}) => severity === 'ERROR')
        .map(({messageText}) => messageText)
        .join('\n');
      return {
        status: RESOURCE_STATUS.ERROR,
        message,
        status_link: transferLink,
      };
    }
  }
  return {
    status: '',
    message: `Data Transfer is running.`,
    refreshFn: () => {
      return MojoFunctions.getTransferStatus(transferConfigId);
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
 * @param {string?} versionInfo Optional version info.
 * @return {!CheckResult}
 */
MojoFunctions.createOrUpdateDataTransfer_ = (
  targetConfig,
  datasetId,
  filterFn,
  versionInfo,
) => {
  const projectId = getDocumentProperty('projectId');
  const bigquery = new BigQuery(projectId, datasetId);
  const {error, location} = bigquery.getDataset();
  if (error) {
    if (error.status === 'NOT_FOUND' || error.code === 404) {
      return {
        status: RESOURCE_STATUS.PENDING,
        message: `Dataset [${datasetId}] is not ready yet.`,
      };
    }
    throw new Error(error.message);
  }
  const dataTransfer = new BigQueryDataTransfer(projectId, location);
  const {dataSourceId} = targetConfig;
  let existingTransferConfig;
  const transferConfigs = dataTransfer.listTransferConfigs({
    dataSourceIds: dataSourceId,
  });
  existingTransferConfig = transferConfigs.filter(filterFn)[0];
  let transferConfig;
  let updatedConfig;
  let needManualRuns = false; // Manually trigger data transfer after update.
  if (existingTransferConfig && existingTransferConfig.name) {
    updatedConfig = diffApiResponseObjects(
      existingTransferConfig,
      targetConfig,
    );
    if (!updatedConfig) {
      // No need to update.
      console.log('No need to update', existingTransferConfig.name);
      return MojoFunctions.getTransferStatus(existingTransferConfig.name);
    }
  }
  let queryString;
  if (dataTransfer.doesNotExistValidCredentials(dataSourceId)) {
    if (!versionInfo) {
      const url = dataTransfer.getAuthUrlForDataSource(dataSourceId);
      throw new Error(
        'Visit following link and complete authorization ' +
          'process, copy the version info to the cell "Attribute Value"' +
          ' and try again:\n\n' +
          url,
      );
    }
    queryString = {versionInfo};
  }
  if (updatedConfig) {
    // This API does't support partially update `params`.
    if (updatedConfig.params) updatedConfig.params = targetConfig.params;
    console.log(`Need update ${existingTransferConfig.name}`, updatedConfig);
    transferConfig = dataTransfer.updateTransferConfig(
      existingTransferConfig.name,
      updatedConfig,
      queryString,
    );
    needManualRuns = true;
  } else {
    console.log('Create Data Transfer');
    transferConfig = dataTransfer.createTransferConfig(
      targetConfig,
      queryString,
    );
  }
  if (transferConfig.error) {
    throw new Error(transferConfig.error.message);
  }
  const transferConfigId = transferConfig.name;
  if (needManualRuns) {
    console.log('Manually run Data Transfer', transferConfigId);
    dataTransfer.startManualRuns(transferConfigId);
  }
  return MojoFunctions.getTransferStatus(transferConfigId);
};

/**
 * Creates or updates a scheduled query which is a kind of data transfer config.
 * This is a facade function based on
 * `MojoFunctions.createOrUpdateDataTransfer_` to help create/update scheduled
 * queries.
 * TODO: need to align with other data transfer configs.
 * @param {string} displayName
 * @param {string} datasetId
 * @param {string} query
 * @param {string} versionInfo
 * @return {!CheckResult}
 */
MojoFunctions.createOrUpdateScheduledQuery = (
  displayName,
  datasetId,
  query,
  versionInfo,
) => {
  const config = {
    dataSourceId: BigQueryDataTransfer.DATA_TRANSFER_SOURCE.SCHEDULED_QUERY,
    displayName,
    params: {query},
  };
  const filterFn = transferConfig => {
    const {dataSourceId, displayName} = transferConfig;
    return (
      dataSourceId === config.dataSourceId && displayName === config.displayName
    );
  };
  return MojoFunctions.createOrUpdateDataTransfer_(
    config,
    datasetId,
    filterFn,
    versionInfo,
  );
};

/**
 * Creates or updates a Google Ads data transfer config.
 * This is a customized facade function based on
 * `MojoFunctions.createOrUpdateDataTransfer_` to handle Google Ads specific
 * configurations and custom reports.
 * @param {string} displayName
 * @param {Object} resource
 * @return {!CheckResult}
 */
MojoFunctions.createOrUpdateDataTransfer = (displayName, resource) => {
  const {attributes = [], attributesMap = {}} = resource;
  const properties = {};
  Object.keys(BigQueryDataTransfer.ATTRIBUTE_NAMES).forEach(key => {
    if (attributesMap[BigQueryDataTransfer.ATTRIBUTE_NAMES[key]]) {
      properties[key] =
        attributesMap[BigQueryDataTransfer.ATTRIBUTE_NAMES[key]].attributeValue;
    }
  });
  const config = {
    displayName,
    dataSourceId: properties.dataSourceId,
    destinationDatasetId: properties.destinationDatasetId,
    schedule: properties.schedule,
  };
  switch (properties.dataSourceId) {
    case BigQueryDataTransfer.DATA_TRANSFER_SOURCE.GOOGLE_ADS:
      if (!properties.customerId) {
        return {
          status: RESOURCE_STATUS.ERROR,
          message: 'Customer ID is required for Google Ads data transfer.',
        };
      }
      config.dataRefreshWindowDays = properties.dataRefreshWindowDays;
      const tableNames = []; // custom_report_table_names;
      const queries = []; // custom_report_queries
      const customReports = attributes.filter(({attributeValue}) => {
        if (typeof attributeValue !== 'string') return false;
        return attributeValue.toLowerCase().trim().startsWith('select ');
      });
      if (customReports.length > 0) {
        customReports.forEach(({attributeName, attributeValue}) => {
          tableNames.push(attributeName.trim());
          queries.push(attributeValue.trim());
        });
      }
      config.params = {customer_id: properties.customerId};
      if (tableNames.length > 0) {
        config.params.custom_report_table_names = tableNames;
        config.params.custom_report_queries = queries;
      }
      console.log('Google Ads Data Transfer config', JSON.stringify(config));
      break;
    // Other data source types can be added here with specific handling if
    // needed. By then, the function createOrUpdateScheduledQuery can be
    // removed.
    default:
      return {
        status: RESOURCE_STATUS.ERROR,
        message: `Unsupported data source: ${properties.dataSourceId}`,
      };
  }

  const filterFn = transferConfig => {
    const {dataSourceId, displayName} = transferConfig;
    return (
      dataSourceId === config.dataSourceId && displayName === config.displayName
    );
  };
  return MojoFunctions.createOrUpdateDataTransfer_(
    config,
    properties.destinationDatasetId,
    filterFn,
    properties.versionInfo,
  );
};

MojoTemplateRegistry.register('bigQueryDataTransfer', {
  category: 'Solution',
  resource: 'BigQuery Data Transfer',
  value: '${namespace}_data_transfer',
  attributes: [
    {
      attributeName: BigQueryDataTransfer.ATTRIBUTE_NAMES.dataSourceId,
      attributeValue_datarange: Object.values(
        BigQueryDataTransfer.DATA_TRANSFER_SOURCE,
      ),
    },
    {
      attributeName: BigQueryDataTransfer.ATTRIBUTE_NAMES.versionInfo,
    },
    {
      attributeName: BigQueryDataTransfer.ATTRIBUTE_NAMES.destinationDatasetId,
      attributeValue: '${dataset}',
    },
    {
      attributeName: BigQueryDataTransfer.ATTRIBUTE_NAMES.schedule,
      attributeValue: 'every day 8:00',
    },
  ],
  checkFn: MojoFunctions.createOrUpdateDataTransfer,
});
MojoTemplateRegistry.register('googleAdsDataTransfer', {
  template: 'bigQueryDataTransfer',
  value: '${namespace}_data_transfer_google_ads',
  attributes: [
    {
      attributeName: BigQueryDataTransfer.ATTRIBUTE_NAMES.dataSourceId,
      attributeValue: BigQueryDataTransfer.DATA_TRANSFER_SOURCE.GOOGLE_ADS,
    },
    {
      attributeName: BigQueryDataTransfer.ATTRIBUTE_NAMES.customerId,
    },
    {
      attributeName: BigQueryDataTransfer.ATTRIBUTE_NAMES.dataRefreshWindowDays,
      attributeValue: '1',
    },
  ],
  groupType: GROUP_TYPE.COLLAPSE,
});
