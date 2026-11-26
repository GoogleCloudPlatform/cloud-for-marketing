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
 * Configuration of a Mojo sheet. It is based on `MojoConfig`.
 * @typedef {{
 *   sheetName: string,
 *   config: !Array<!MojoResource>,
 *   headlineStyle: (!Object|undefined),
 * }}
 */
let MojoSheetConfig;

/**
 * The class of MojoSheet, which is a sheet based on a MojoConfig.
 * The solution defined by the MojoConfig will be installed in this sheet.
 */
class MojoSheet {
  static get instances() {
    if (!this._instances) {
      this._instances = {};
    }
    return this._instances;
  }

  /**
   * During initialization, the constructor will prepare itself (the instance)
   * as the object that holds functions for menu items.
   * @param {!MojoSheetConfig} config
   */
  constructor(config) {
    this.config = config;
    // Set up the holder for menu functions
    const instanceName = getSafePropertyName(config.sheetName);
    MojoSheet.instances[instanceName] = this;
    this.menuFunctionHolder = `MojoSheet.instances.${instanceName}`;
    // `sheetName` and `menuItem` are used to create Cyborg submenu.
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
      {separator: true},
      {
        name: 'Check and apply all',
        method: `${this.menuFunctionHolder}.checkAndApplyAll`,
      },
      {separator: true},
      {
        name: 'Load all properties',
        method: `${this.menuFunctionHolder}.loadProperties`,
      },
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
   * Initializes Mojo instance only when it is used.
   * @return {!Mojo}
   */
  getMojo() {
    if (!this.mojo) {
      console.log('Initialize Mojo object for:', this.sheetName);
      this.mojo = new Mojo(this.config);
    }
    return this.mojo;
  }

  /**
   * Initializes the Mojo Sheet.
   */
  initialize() {
    this.getMojo().initializeSheet();
    clearDocumentProperties();
  }

  /**
   * Checks the status of MojoResources.
   */
  checkResources() {
    this.getMojo().checkResources();
  }

  /**
   * Checks the status of MojoResources regardless the resource status.
   */
  recheckResources() {
    this.getMojo().checkResources(true);
  }

  /**
   * Applies changes.
   */
  applyChanges() {
    this.getMojo().applyChanges();
  }

  /**
   * Checks and applies all enabled MojoResources automatically.
   */
  checkAndApplyAll() {
    this.getMojo().checkAndApplyAll();
  }
  /**
   * Load all properties.
   */
  loadProperties() {
    this.getMojo().loadSheet();
  }
}
