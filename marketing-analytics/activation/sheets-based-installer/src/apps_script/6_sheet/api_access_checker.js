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

/** @fileoverview Api access checker sheet. */

/**
 * Apis and their access checking functions.
 * @const {!Object<string, function(ExternalApiAuthOption,
 * string):!VerifyResult>}
 */
const API_CHECK_FUNCTIONS = {
  'Google Ads Report': (option, resourceId, extraInfo) => {
    return new GoogleAds(option, resourceId, extraInfo).verifyReportAccess();
  },
  'Search Ads Reporting': (option, resourceId) => {
    return new SearchAdsReporting(option, resourceId).verifyReportAccess();
  },
  'CM360 Account & Report': (option, resourceId, extraInfo) => {
    return new CampaignManager(option).verifyReport(resourceId, extraInfo);
  },
  'DV360 Query Id': (option, resourceId) => {
    return new DoubleClickBidManager(option).verifyQuery(resourceId);
  },
  Sheets: (option, resourceId) => {
    return new Sheets(option).verifySpreadsheet(resourceId);
  },
};

/**
 * Type of the object that each row of the sheet can be mapped to.
 *
 * @typedef {{
 *   secretName: string,
 *   api: string,
 *   resourceId: string,
 *   extraInfo: string|undefined,
 * }}
 */
let ApiAccessCheckerRowEntity;

/**
 * The sheet stores names of secrets (in Secret Manage) and configurations of
 * Apis. The functions can check whether the secret has access to that Api.
 */
class ApiAccessChecker extends PlainSheet {
  get defaultSheetName() {
    return 'Api Access Checker';
  }

  get defaultHeadlineStyle() {
    return {
      backgroundColor: '#FBBC04',
      fontColor: 'white',
    };
  }

  get columnConfiguration() {
    return [
      {
        name: 'Secret Name',
        dataRange: PlainSheet.instances[SECRET_MANAGER_MENU_OBJECT]
          ? PlainSheet.instances[
              SECRET_MANAGER_MENU_OBJECT
            ].getArrayOfRowEntity().map(({name}) => name)
          : [],
        width: 200,
      },
      {name: 'API', width: 200, dataRange: Object.keys(API_CHECK_FUNCTIONS)},
      {name: 'Resource Id', width: 300},
      {name: 'Extra Info'},
      {name: 'API Access Check', width: 400, format: COLUMN_STYLES.WRAP_TEXT},
      {
        name: COLUMN_NAME_FOR_DEFAULT_CONFIG,
        width: 200,
        format: [COLUMN_STYLES.MONO_FONT, COLUMN_STYLES.ALIGN_MIDDLE],
      },
    ];
  }

  get inherentMenuItems() {
    return [
      {name: 'Check selected API', method: 'operateSingleRow'},
      {separator: true},
      {name: 'Check all APIs', method: 'operateAllRows'},
      {name: 'Refresh secret name list', method: 'refreshDataRange'},
      {name: 'Reset sheet (will lose modification)', method: 'initialize'},
    ];
  }

  /**
   * Gets an ExternalApiAuthOption based on the given secret name.
   * If secretName was given, OAuth token will be loaded from Secret Manager.
   * Otherwise, it will try to get the default service account.
   * @param {string|undefined} secretName
   * @return {!ExternalApiAuthOption}
   * @private
   */
  getAuthOption_(secretName) {
    const option = {};
    if (!secretName) {
      const serviceAccount = getDocumentProperty('serviceAccount');
      if (!serviceAccount) {
        throw new Error('Can not find service account in document properties.');
      }
      option.serviceAccount = serviceAccount;
    } else {
      const projectId = getDocumentProperty('projectId');
      const secretManager = new SecretManager(projectId);
      const {error, data} = secretManager.accessSecret(secretName);
      if (error) {
        throw new Error(
          `Failed to access secret '${secretName}': ${
            error.message || JSON.stringify(error)
          }`,
        );
      }
      option.oauth = JSON.parse(data);
    }
    return option;
  }

  /**
   * Evaluates API verification with auth options and formats the outcome label.
   * @param {string|undefined} secretName
   * @param {function(!ExternalApiAuthOption):!VerifyResult} getCheckResult
   * @return {!VerifyResult}
   * @private
   */
  getAccessCheckResult_(secretName, getCheckResult) {
    const label = secretName
      ? `Auth with Secret Manager [name: ${secretName}]`
      : 'Auth with default service account';
    try {
      const option = this.getAuthOption_(secretName);
      const result = getCheckResult(option);
      result.label = label;
      return result;
    } catch (error) {
      return {
        valid: false,
        label,
        reason: error.message,
      };
    }
  }

  /**
   * Checks the access of given configuration.
   * @override
   * @param {!Array<!ApiAccessCheckerRowEntity>} configs
   * @return {!Array<!VerifyResult>}
   */
  processResources(configs) {
    return configs.map(({secretName, api, resourceId, extraInfo}) => {
      const checkFn = API_CHECK_FUNCTIONS[api];
      if (!checkFn) {
        return {
          valid: false,
          reason: `Unsupported API: ${api}`,
        };
      }
      const getCheckResult = option => {
        return checkFn(option, resourceId, extraInfo);
      };
      return this.getAccessCheckResult_(secretName, getCheckResult);
    });
  }

  /**
   * Shows the result of checking.
   * @param {number} rowIndex
   * @param {!VerifyResult} result
   */
  showOperationResult(rowIndex, result) {
    const timeTag = new Date().toISOString();
    const message = result.valid ? 'passed.' : `failed: ${result.reason}`;
    const columnIndex = this.columnName.indexOf('API Access Check');
    this.getEnhancedSheet()
      .sheet.getRange(rowIndex, columnIndex + 1, 1, 1)
      .setFontColor(result.valid ? 'green' : 'red')
      .setValue(`[${timeTag}] ${result.label || ''} ${message}`);
  }
}
