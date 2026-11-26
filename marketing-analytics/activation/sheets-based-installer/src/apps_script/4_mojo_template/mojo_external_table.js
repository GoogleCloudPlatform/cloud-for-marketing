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
 * Checks the target Google Sheets with given url and sheet name. Initializes it
 * if it doesn't exist. Then creates or updates the definition of external table
 * in BigQuery.
 *
 * @param {!DataTableSheet} externalTableSheet
 * @param {string} sheetsUrl
 * @param {string} role Possible values are 'Viewer', 'Commenter' or 'Editor'.
 * @return {!CheckResult}
 */
MojoFunctions.checkOrInitializeExternalTable = (
  externalTableSheet,
  sheetsUrl,
  resource,
) => {
  let result = MojoFunctions.checkAndGrantAccess(sheetsUrl, resource);
  if (result.status !== RESOURCE_STATUS.OK) return result;
  const {sheetName} = externalTableSheet;
  const checkedUrl = result.value;
  const targetSheet =
    SpreadsheetApp.openByUrl(checkedUrl).getSheetByName(sheetName);
  let message;
  if (!targetSheet) {
    externalTableSheet.initialize(checkedUrl);
    message = `Initialized target sheet: ${sheetName}`;
  } else {
    message = `Target sheet: ${sheetName} exists.`;
  }
  const externalTableResult = MojoFunctions.updateExternalTable(
    checkedUrl,
    externalTableSheet,
  );
  return Object.assign({value: checkedUrl, message}, externalTableResult);
};

/**
 * Checks whether the service account has the expected role of the target Google
 * Sheets with given url.
 *
 * @param {string} sheetsUrl
 * @param {{attributeValue:string}} resource The property 'attributeValue' of
 *    the given `resource` has a value of 'Viewer', 'Commenter' or 'Editor'.
 * @return {!CheckResult}
 */
MojoFunctions.checkAndGrantAccess = (sheetsUrl, resource) => {
  const role = resource.attributeValue;
  const serviceAccount = getDocumentProperty('serviceAccount');
  if (!serviceAccount) {
    return {
      status: RESOURCE_STATUS.PENDING,
      message:
        'The service account will be ready after Cloud Functions is installed',
    };
  }
  let message = '';
  if (!sheetsUrl) {
    sheetsUrl = SpreadsheetApp.getActiveSpreadsheet().getUrl();
    message = 'Empty sheet url. Using current sheet.\n';
  } else {
    const result = MojoFunctions.checkSheetsUrl(sheetsUrl);
    if (result) return result;
  }
  const fileId = getSpreadsheetIdFromUrl(sheetsUrl);
  const drive = new Drive();
  const {error} = drive.addUser(fileId, serviceAccount, role);
  if (error) {
    return {
      value: sheetsUrl,
      status: RESOURCE_STATUS.ERROR,
      message: message + error.message,
    };
  }
  return {
    value: sheetsUrl,
    status: RESOURCE_STATUS.OK,
    message:
      message +
      `Service account ${serviceAccount} has the access to the target sheet.`,
  };
};

/**
 * Creates or updates the BigQuery external table based on given Google Sheets
 * information.
 * It will use the default property 'projectId' and 'dataset'.
 *
 * @param {string} sheetsUrl
 * @param {!DataTableSheet} externalTableSheet
 * @return {!CheckResult}
 */
MojoFunctions.updateExternalTable = (sheetsUrl, externalTableSheet) => {
  const {sheetName, datasetPropertyName, fields, tableName, mappedTypes} =
    externalTableSheet;
  const projectId = getDocumentProperty('projectId');
  const datasetId = getDocumentProperty(datasetPropertyName);
  const bigquery = new BigQuery(projectId, datasetId);
  const tableId = tableName;
  const tableConfig = {
    type: 'EXTERNAL',
    tableReference: {tableId},
    externalDataConfiguration: {
      sourceUris: [sheetsUrl],
      sourceFormat: 'GOOGLE_SHEETS',
      googleSheetsOptions: {
        skipLeadingRows: '1',
        range: sheetName,
      },
    },
  };
  if (fields) {
    const schema = getBigQuerySchema(fields, mappedTypes);
    tableConfig.externalDataConfiguration.schema = {fields: schema};
  } else {
    tableConfig.externalDataConfiguration.autodetect = true;
  }
  const {error: bigQueryError} = bigquery.createOrUpdateTable(tableConfig);
  if (bigQueryError) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: bigQueryError.message,
    };
  }
  return {
    status: RESOURCE_STATUS.OK,
    message: `External table '${datasetId}.${tableName}' has been created/updated.`,
    status_link: bigquery.getLinkToTable(tableName),
  };
};

/** Checks whether it is a valid Sheets URL. */
MojoFunctions.checkSheetsUrl = sheetUrl => {
  if (!sheetUrl) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: 'Enter the URL of the Google Sheets.',
    };
  }
  if (!getSpreadsheetIdFromUrl(sheetUrl)) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: 'Can not get the Google Sheets Id from the URL',
    };
  }
};

MojoTemplateRegistry.register('externalTable', {
  category: 'Configuration',
  resource: 'Sheet URL',
  editType: RESOURCE_EDIT_TYPE.USER_INPUT,
  attributeName: 'Access Level for Service Account',
  attributeValue: 'Viewer',
  attributeValue_datarange: ['Viewer', 'Commenter', 'Editor'],
  checkFn: MojoFunctions.checkAndGrantAccess,
  propertyName: 'dataSheetUrl',
});
