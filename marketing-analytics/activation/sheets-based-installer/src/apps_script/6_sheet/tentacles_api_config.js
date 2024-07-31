// Copyright 2021 Google Inc.
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

/** @fileoverview Tentacles config sheet. */

/**
 * Type of the object that each row of the sheet can be mapped to.
 *
 * @typedef {{
 *   api: string,
 *   config: string,
 *   configConfent: string,
 * }}
 */
let TentaclesConfigRowEntity;

/**
 * No access checks for following Apis.
 * @const {!Array<string>}
 */
const NO_NEED_FOR_CHECK_API = [
  'MP',
  'MP_GA4',
  'PB',
  'SFTP',
];

/**
 * Checks Google Ads conversions.
 * @param {!ExternalApiAuthOption} option
 * @param {!Object} config
 * @return {!VerifyResult}
 */
const GOOGLE_ADS_CONVERSIONS_FN = (option, config) => {
  const { customerId, loginCustomerId, developerToken,
    adsConfig: { conversion_action: conversionAction } } = config;
  const conversionId =
    conversionAction.substring(conversionAction.lastIndexOf('/') + 1);
  const googleAds = new GoogleAds(option, loginCustomerId, developerToken);
  return googleAds.verifyConversion(conversionId, customerId);
};

/**
 * Returns a function to check the specified configuration. The configuration
 * can be a 'customer match config' or a 'offline user data job config'.
 * @param {string} configName
 * @return {function(ExternalApiAuthOption, string):!VerifyResult}
 */
const GOOGLE_ADS_USERLIST_FN = (configName) => {
  return (option, config) => {
    const {
      developerToken,
      [configName]: { customerId, loginCustomerId, listId }
    } = config;
    const googleAds = new GoogleAds(option, loginCustomerId, developerToken);
    if (listId) return googleAds.verifyUserList(listId, customerId);
    const result = googleAds.verifyReportAccess();
    if (result.valid === true) {
      result.reason = 'Did not check User List due to lack list_id';
    }
    return result;
  }
};

/**
 * Tentacles connectors and their access checking functions.
 * @const {!Object<string, function(ExternalApiAuthOption, Object):!VerifyResult>}
 */
const TENTACLES_CONFIG_CHECK_FUNCTIONS = {
  GS: (option, config) => {
    return new Sheets(option).verifySheet(config.spreadsheetId, config.sheetName);
  },
  GA: (option, config) => {
    const { accountId, webPropertyId, customDataSourceId } = config.gaConfig;
    return new Analytics(option).verifyDataSource(
      accountId, webPropertyId, customDataSourceId);
  },
  CM: (option, config) => {
    const { cmAccountId, cmConfig: { conversion } } = config;
    const campaignManager = new CampaignManager(option);
    return campaignManager.verifyFloodlightActivity(
      cmAccountId, conversion.floodlightActivityId);
  },
  SA: (option, config) => {
    const { availabilities } = config;
    const searchAds = new SearchAds(option);
    return searchAds.verifyFloodlightActivities(availabilities);
  },
  ACLC: GOOGLE_ADS_CONVERSIONS_FN,
  CALL: GOOGLE_ADS_CONVERSIONS_FN,
  ACA: GOOGLE_ADS_CONVERSIONS_FN,
  ACM: GOOGLE_ADS_USERLIST_FN('customerMatchConfig'),
  AOUD: GOOGLE_ADS_USERLIST_FN('offlineUserDataJobConfig'),
}

/**
 * The sheet stores Tentacles Api configurations which will be uploaded to
 * Firestore.
 */
class TentaclesConfig extends PlainSheet {

  /**
   * In the parameter `options`, the property named 'configs' is for intial data.
   */
  get initialData() {
    const { configs: initConfigs = {} } = this.options || {};
    return Object.keys(initConfigs).map((api) => {
      const configs = initConfigs[api];
      return Object.keys(configs).map((config) => {
        return [api, config, JSON.stringify(configs[config], null, 2)];
      });
    }).flat();
  }

  get defaultSheetName() {
    return 'Tentacles Config';
  }

  get columnConfiguration() {
    return [
      { name: 'API', width: 50, format: COLUMN_STYLES.ALIGN_MIDDLE },
      { name: 'Config', format: COLUMN_STYLES.ALIGN_MIDDLE },
      { name: 'Config Content', jsonColumn: true, defaultNote: true, width: 500 },
      { name: 'Test Data', width: 500 },
      { name: 'API Access Check', width: 400, format: COLUMN_STYLES.ALIGN_MIDDLE },
      {
        name: COLUMN_NAME_FOR_DEFAULT_CONFIG, width: 100,
        format: COLUMN_STYLES.MONO_FONT,
      },
    ];
  }

  get downloadContent() {
    const entities = this.getArrayOfRowEntity();
    const result = {};
    entities.forEach(({ api, config, configContent }) => {
      if (!result[api]) result[api] = {};
      result[api][config] = JSON.parse(configContent);
    });
    return JSON.stringify(result);
  }

  get downloadFilename() {
    const tag = this.options.sheetName
      ? `_${snakeize(this.options.sheetName)}` : '';
    return `config_api${tag}.json`;
  }

  get inherentMenuItems() {
    return [
      {
        name: 'Check selected config for accessibility',
        method: 'showApiCheckResult',
      },
      {
        name: 'Update selected config to Firestore',
        method: 'operateSingleRow',
      },
      {
        name: 'Upload selected data to test Tentacles',
        method: 'uploadTestData',
      },
      { separator: true },
      { name: 'Update all configs to Firestore', method: 'operateAllRows' },
      {
        name: 'Append all configs from Firestore to Sheets',
        method: 'loadFromFirestore',
      },
      { name: 'Download current sheet as a JSON file', method: 'export' },
      { separator: true },
      { name: 'Reset sheet (will lose monification)', method: 'initialize' },
    ];
  }

  /**
   * Returns a map of Tentacles Api configurations to be saved to Firestore. The
   * keys are Firestore entity Ids and the values are Firestore entities.
   *
   * @param {!Array<!TentaclesConfigRowEntity>} tasks
   * @return {!Object<string, !Object>}
   * @private
   */
  getEntities_(configs) {
    const upldatedConfigs = JSON.parse(
      replaceVariables(JSON.stringify(configs), getDocumentProperties()));
    const entities = {};
    upldatedConfigs.forEach(({ api, config, configContent }) => {
      entities[`${api}.${config}`] = JSON.parse(configContent);
    });
    return entities;
  }

  /**
   * Update Tentacles Api configurations to Firestore.
   * @override
   * @param {!Array<!TentaclesConfigRowEntity>} tasks
   * @return {!Array<string>}
   */
  processResources(configs) {
    const entities = this.getEntities_(configs);
    const results = gcloud.saveEntitiesToFirestore('ApiConfig', entities);
    return results.map(
      ({ updateTime }) => `Has been updated to Database at ${updateTime}`
    );
  }

  /**
   * Uploads test data as a file to Cloud Storage with proper file name to
   * trigger Tentacles.
   */
  uploadTestData() {
    const rowIndex = this.getSelectedRow();
    const { api, config, testData: content } =
      this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    const projectId = getDocumentProperty('projectId');
    const bucket = getDocumentProperty('tentaclesBucket');
    const outbound = getDocumentProperty('tentaclesOutbound');
    const timestamp = new Date().toISOString();
    const fileName =
      `${outbound}/API[${api}]_config[${config}]_test_by_cyborg_${timestamp}`;
    const storage = new Storage(projectId);
    const response = storage.uploadFile(fileName, bucket, content);
    const note = `Uploaded as ${response.name}.`;
    this.showOperationResult(rowIndex, note, 'black', 'Test Data');
  }

  /** Loads Api Configs from Firestore database to this Google Sheets. */
  loadFromFirestore() {
    const results = gcloud.loadEntitiesToFirestore('ApiConfig');
    const rows = results.map(
      ({ id, json }) => id.split('.').concat(JSON.stringify(json, null, 2)));
    this.getEnhancedSheet().append(rows);
  }

  /**
   * Shows the result of checking.
   */
  showApiCheckResult() {
    const columnIndex = this.columnName.indexOf('API Access Check');
    const rowIndex = this.getSelectedRow();
    const timeTag = new Date().toISOString();
    const result = this.checkApiAccess(rowIndex);
    const message = result.valid ? 'passed.' : `failed: ${result.reason}`;
    this.getEnhancedSheet().sheet.getRange(rowIndex, columnIndex + 1, 1, 1)
      .setFontColor(result.valid ? 'green' : 'red')
      .setValue(`[${timeTag}] ${result.label || ''} ${message}`);
  }

  /**
   * Returns the check result of a specified row.
   * @param {number} rowIndex
   * @return {!VerifyResult}
   */
  checkApiAccess(rowIndex) {
    const { api, configContent } =
      this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    if (NO_NEED_FOR_CHECK_API.indexOf(api) > -1) {
      return {
        valid: true,
        label: 'This API does not need authorization:',
      };
    }
    const checkFn = TENTACLES_CONFIG_CHECK_FUNCTIONS[api];
    if (!checkFn) {
      return {
        valid: false,
        reason: `Unsupported API: ${api}`,
      };
    }
    const config = JSON.parse(configContent);
    const secretName = config.secretName
      || getDocumentProperty('defaultSecretName', false);
    const getCheckResult = (option) => {
      return checkFn(option, config);
    };
    return gcloud.getAccessCheckResult(secretName, getCheckResult);
  }

}
