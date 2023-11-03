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
 * @const {!Object<string, function(ExternalApiAuthOption, string):!VerifyResult>}
 */
const API_CHECK_FUNCTIONS = {
  'Google Ads Report': (option, resourceId, extraInfo) => {
    return new GoogleAds(option, resourceId, extraInfo).verifyReportAccess();
  },
  'Search Ads Account': (option, resourceId) => {
    return new SearchAds(option).verifyAgency(resourceId);
  },
  'CM360 Account & Report': (option, resourceId, extraInfo) => {
    return new CampaignManager(option).verifyReport(resourceId, extraInfo);
  },
  'DV360 Query Id': (option, resourceId) => {
    return new DoubleclickBidManager(option).verifyQuery(resourceId);
  },
  'Sheets': (option, resourceId) => {
    return new Sheets(option).verifySpreadsheet(resourceId);
  },
}

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
 * The sheet stores names of secrets (in Secret Manage) and configruations of
 * Apis. The functions can check whether the secret has access to that Api.
 */
class ApiAccessChecker extends PlainSheet {

  constructor() {
    super();
    this.sheetName = 'Api Access Checker';
    this.columnName = [
      'Secret Name',
      'API',
      'Resource Id',
      'Extra Info',
      'API Access Check',
    ];
    this.fields = this.columnName.map(camelize);
    this.columnWidth = {
      'Secret Name': 200,
      'API': 200,
      'Resource Id': 300,
      'API Access Check': 400,
      default_: 200,
    };
    this.columnFormat = {
      default_: { fn: 'setFontFamily', format: 'Consolas' },
    };
    this.columnDataRange = {
      'API': Object.keys(API_CHECK_FUNCTIONS),
    }
    this.headlineStyle = {
      backgroundColor: '#FBBC04',
      fontColor: 'white',
    };
    // Menu items
    this.menuItem = [
      {
        name: 'Check selected API',
        method: `${this.menuFunctionHolder}.operateSingleRow`,
      },
      { seperateLine: true },
      {
        name: 'Check all APIs',
        method: `${this.menuFunctionHolder}.operateAllRows`,
      },
      {
        name: 'Reset sheet (will lose monification)',
        method: `${this.menuFunctionHolder}.initialize`,
      },
    ];
  }

  /**
   * Checks the access of given configuration.
   * @override
   * @param {!Array<!ApiAccessCheckerRowEntity>} configs
   * @return {!Array<!VerifyResult>}
   */
  processResources(configs) {
    return configs.map(({ secretName, api, resourceId, extraInfo }) => {
      const checkFn = API_CHECK_FUNCTIONS[api];
      if (!checkFn) {
        return {
          valid: false,
          reason: `Unsupported API: ${api}`,
        };
      }
      const getCheckResult = (option) => {
        return checkFn(option, resourceId, extraInfo);
      };
      return gcloud.getAccessCheckResult(secretName, getCheckResult);
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
    this.getEnhancedSheet().sheet.getRange(rowIndex, columnIndex + 1, 1, 1)
      .setFontColor(result.valid ? 'green' : 'red')
      .setValue(`[${timeTag}] ${result.label || ''} ${message}`);
  }

}
