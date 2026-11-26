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

/** @fileoverview Secret Manager Sheet.*/

/**
 * `oauth.js` and `api_access_checker.js` need to invoke some menu functions of
 * this sheet. If the menu object has a dynamic name (like other sheets, which
 * are based on the 'Sheet Name'), it would be hard to get this sheet. So, we
 * use a constant name as the menu object.
 * Note, there should not be multiple instances of this sheet.
 * @const {string} SECRET_MANAGER_MENU_OBJECT
 */
const SECRET_MANAGER_MENU_OBJECT = 'secretManager';

/**
 * The sheet list all secret names in Secret Manager.
 */
class SecretManagerSheet extends PlainSheet {
  /** @return {string} */
  get defaultSheetName() {
    return 'Secret Manager';
  }

  /** @return {string} */
  get menuObjectName() {
    return SECRET_MANAGER_MENU_OBJECT;
  }

  /** @return {!Object} */
  get defaultHeadlineStyle() {
    return {
      backgroundColor: '#FBBC04',
      fontColor: 'white',
    };
  }

  /** @return {!Array<!Object>} */
  get columnConfiguration() {
    return [
      {name: 'Name', width: 200},
      {
        name: 'Latest Version',
        width: 100,
        format: {fn: 'setHorizontalAlignment', format: 'center'},
      },
      {
        name: 'State',
        width: 100,
        format: {fn: 'setHorizontalAlignment', format: 'center'},
      },
      {name: 'Created', width: 300},
      {name: COLUMN_NAME_FOR_DEFAULT_CONFIG, format: COLUMN_STYLES.MONO_FONT},
    ];
  }

  /** @return {!Array<!Object>} */
  get inherentMenuItems() {
    return [
      {name: 'Refresh secrets', method: 'listSecrets'},
      {
        name: 'Add a new version to the selected secret',
        method: 'createOrUpdateSecret',
      },
      {name: 'Reset sheet', method: 'initialize'},
    ];
  }

  /**
   * Adds a new version to the selected secret.
   * The target secret can be a new one.
   */
  createOrUpdateSecret() {
    const rowIndex = this.getSelectedRow();
    const {name: secretName} =
      this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    const ui = SpreadsheetApp.getUi();
    const result = ui.prompt(
      `Please enter new version for the selected secret '${secretName}': \n\n`,
    );
    if (result.getSelectedButton() === ui.Button.CLOSE) {
      return;
    }
    const secret = result.getResponseText();
    const projectId = getDocumentProperty('projectId');
    const secretManager = new SecretManager(projectId);
    const response = secretManager.addSecretVersion(secretName, secret);
    if (!response || response.error) {
      const errMsg =
        response && response.error
          ? response.error.message || JSON.stringify(response.error)
          : 'Unknown error';
      ui.alert(`Failed to add secret version: ${errMsg}`);
      return;
    }
    const {name: fullName, createTime, state} = response;
    const version = fullName ? fullName.split('/versions/')[1] : 'N/A';
    const updated = {
      latestVersion: version,
      state: state || 'ENABLED',
      created: createTime || new Date().toISOString(),
    };
    this.updateCells(updated, rowIndex);
    const link = secretManager.getLinkToCloudConsole(secretName);
    this.updateLink(rowIndex, this.columnName.indexOf('Name') + 1, link);
  }

  /** @override */
  initialize() {
    super.initialize();
    this.listSecrets();
  }

  /** Lists all secrets in Secret Manager. */
  listSecrets() {
    const enhancedSheet = this.getEnhancedSheet();
    enhancedSheet.clearData();
    const projectId = getDocumentProperty('projectId');
    const secretManager = new SecretManager(projectId);
    const secrets = secretManager.listSecrets();
    if (secrets) {
      const links = [];
      const rows = secrets.map(({name: fullName}) => {
        const secret = fullName.split('/secrets/')[1];
        const versionResponse = secretManager.getSecretVersion(secret);
        const {createTime, state, name} = versionResponse || {};
        const version = name ? name.split('/versions/')[1] : 'N/A';
        const row = [];
        row[0] = secret;
        row[1] = version;
        row[2] = state || 'UNKNOWN';
        row[3] = createTime || 'N/A';
        const linkToSecret = secretManager.getLinkToCloudConsole(secret);
        links.push(linkToSecret);
        return row;
      });
      enhancedSheet.save(rows, ROW_INDEX_SHIFT);
      const linkColumnIndex = this.columnName.indexOf('Name') + 1;
      links.forEach((link, index) => {
        this.updateLink(index + ROW_INDEX_SHIFT, linkColumnIndex, link);
      });
    }
  }
}
