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

  constructor() {
    super();
    this.sheetName = 'Secret Manager';
    this.columnName = [
      'Name',
      'Latest Version',
      'State',
      'Created',
    ];
    this.fields = this.columnName.map(camelize);
    this.columnWidth = {
      'Name': 200,
      'Latest Version': 100,
      'State': 100,
      'Created': 300,
    };
    this.columnFormat = {
      'Latest Version': { fn: 'setHorizontalAlignment', format: 'center' },
      'State': { fn: 'setHorizontalAlignment', format: 'center' },
      default_: { fn: 'setFontFamily', format: 'Consolas' },
    };
    this.headlineStyle = {
      backgroundColor: '#FBBC04',
      fontColor: 'white',
    }
    // Menu items
    this.menuItem = [
      {
        name: 'Refresh secrets',
        method: `${this.menuFunctionHolder}.listSecrets`,
      },
      {
        name: 'Reset sheet',
        method: `${this.menuFunctionHolder}.initialize`,
      },
    ];
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
