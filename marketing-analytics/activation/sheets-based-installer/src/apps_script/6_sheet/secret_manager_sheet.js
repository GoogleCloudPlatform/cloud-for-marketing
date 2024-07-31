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
 * The sheet list all secret names in Secret Manager.
 */
class SecretManagerSheet extends PlainSheet {

  get defaultSheetName() {
    return 'Secret Manager';
  }

  get defaultHeadlineStyle() {
    return {
      backgroundColor: '#FBBC04',
      fontColor: 'white',
    };
  }

  get columnConfiguration() {
    return [
      { name: 'Name', width: 200 },
      {
        name: 'Latest Version', width: 100,
        format: { fn: 'setHorizontalAlignment', format: 'center' },
      },
      {
        name: 'State', width: 100,
        format: { fn: 'setHorizontalAlignment', format: 'center' },
      },
      { name: 'Created', width: 300 },
      { name: COLUMN_NAME_FOR_DEFAULT_CONFIG, format: COLUMN_STYLES.MONO_FONT },
    ];
  }

  get inherentMenuItems() {
    return [
      { name: 'Refresh secrets', method: 'listSecrets' },
      {
        name: 'Save OAuth token in deployed Cloud Functions to Secret Manager',
        method: 'saveOauthTokenFromCloudFunctionsToSecretManager',
      },
      { name: 'Reset sheet', method: 'initialize' },
    ];
  }


  /**
   * Gets the OAuth token from deployed Cloud Functions and saves it as a secret
   * in Secret Manage.
   */
  saveOauthTokenFromCloudFunctionsToSecretManager() {
    const projectId = getDocumentProperty('projectId');
    const locationId = getDocumentProperty('locationId');
    const namespace = getDocumentProperty('namespace');
    const cloudFunctions = new CloudFunctions(projectId, locationId);
    const results = ['api', 'main'].map((functionSuffix) => {
      const functionName = `${namespace}_${functionSuffix}`;
      const blobs = cloudFunctions.getSourceCode(functionName);
      if (blobs.error) {
        console.log(blobs.error.message);
        return false;
      }
      const blob = blobs.filter(
        (blob) => blob.getName() === 'keys/oauth2.token.json')[0];
      if (!blob) {
        console.log('There is no OAuth token in Cloud Functions', functionName);
        return false;
      } else {
        const secretName = `${functionName}_legacy_token`;
        const token = blob.getDataAsString();
        console.log(
          `Found OAuth token in ${functionName}, will save it as ${secretName}.`);
        const secretManager = new SecretManager(projectId);
        secretManager.addSecretVersion(secretName, token);
        return true;
      }
    });
    if (results.indexOf(true) === -1) {
      const ui = SpreadsheetApp.getUi();
      ui.alert(`No existing OAuth token could be found.`);
    } else {
      this.listSecrets();
    }
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
    const { secrets } = secretManager.listSecrets();
    if (secrets) {
      const rows = secrets.map(({ name: fullName }) => {
        const secret = fullName.split('/secrets/')[1];
        const { createTime, state, name } = secretManager.getSecretVersion(secret);
        const version = name.split('/versions/')[1];
        const row = [];
        row[0] = secret;
        row[1] = version;
        row[2] = state;
        row[3] = createTime;
        return row;
      });
      enhancedSheet.save(rows, 2);
    }
  }

}
