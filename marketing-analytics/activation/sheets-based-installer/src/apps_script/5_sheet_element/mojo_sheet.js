// Copyright 2022 Google Inc.
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

/** @fileoverview The Mojo sheet class. */

/**
 * Configuration of a Mojo sheet. It is based on `MojoConfig` with an extra
 * fields `oauthScope` which is an array of APIs to be used by the OAuth
 * sidebar page.
 * @typedef {{
 *   sheetName: string,
 *   config: !Array<!MojoResource>,
 *   headlineStyle: object|undefined,
 *   oauthScope: !Array<string>|undefined,
 * }}
 */
let MojoSheetConfig;

/**
 * The class of MojoSheet, which is a sheet based on a MojoConfig.
 * The solution defined by the MojoConfig will be installed in this sheet.
 *
 * @see DEFAULT_OAUTH_SCOPE in ./oauth.js
 * @see MENU_OBJECTS in ./trigger_functions.js
 */
class MojoSheet {

  /**
   * During initialization, the constructor will (1) add related APIs to
   * `DEFAULT_OAUTH_SCOPE` which is used by the OAuth sidebar that can guide the
   * users to generate OAuth2 tokens; (2) prepare itself (the instance) as the
   * object that holds functions for menu items.
   * @constructor
   * @param {!MojoSheetConfig} options
   */
  constructor(config) {
    this.mojo = new Mojo(config);
    // Set up the OAuth scopes
    if (config.oauthScope) {
      config.oauthScope.forEach((scope) => {
        if (DEFAULT_OAUTH_SCOPE.indexOf(scope) === -1) {
          DEFAULT_OAUTH_SCOPE.push(scope);
        }
      });
    }
    // Set up the holder for menu functions
    const instanceName = camelize(config.sheetName);
    MENU_OBJECTS[instanceName] = this;
    this.menuFunctionHolder = `MENU_OBJECTS.${instanceName}`;
    // `sheetName` and `menuItem` are used to create Cyborg menu.
    this.sheetName = config.sheetName;
    this.menuItem = [
      {
        name: 'Check resources',
        method: `${this.menuFunctionHolder}.checkResources`,
      },
      {
        name: 'Apply changes',
        method: `${this.menuFunctionHolder}.applyChanges`,
      },
      { separator: true },
      {
        name: 'Recheck resources (even it is OK)',
        method: `${this.menuFunctionHolder}.recheckResources`,
      },
      {
        name: 'Reset sheet',
        method: `${this.menuFunctionHolder}.initialize`,
      },
    ];
  }

  /**
   * Initializes the Mojo Sheet.
   */
  initialize() {
    this.mojo.initializeSheet();
    clearDocumentProperties();
  }

  /**
   * Checks the status of MojoResources.
   */
  checkResources() {
    this.mojo.checkResources();
  }

  /**
   * Checks the status of MojoResources regardless the resource status.
   */
  recheckResources() {
    this.mojo.checkResources(true);
  }

  /**
   * Applys changes.
   */
  applyChanges() {
    this.mojo.applyChanges();
  }
}
