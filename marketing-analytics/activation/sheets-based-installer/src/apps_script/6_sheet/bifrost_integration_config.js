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

/** @fileoverview Integration config sheet. */

/**
 * Type of the object that each row of the sheet can be mapped to.
 *
 * @typedef {{
 *   api: string,
 *   config: string,
 *   configContent: string,
 * }}
 */
let IntegrationConfigRowEntity;

/**
 * The sheet stores Tentacles Api configurations which will be uploaded to
 * Firestore.
 */
class IntegrationConfig extends PlainSheet {
  /**
   * The name of the collection in Firestore.
   * @return {string}
   */
  get firestoreCollectionName() {
    return 'IntegrationConfig';
  }

  /**
   * In the parameter `options`, the property named 'configs' is for initial
   * data.
   * @return {!Array<!Array<string>>}
   */
  get initialData() {
    const {configs: initConfigs = {}} = this.options || {};
    return Object.keys(initConfigs)
      .map(api => {
        const configs = initConfigs[api];
        return Object.keys(configs).map(config => {
          return [api, config, JSON.stringify(configs[config], null, 2)];
        });
      })
      .flat();
  }

  /**
   * Returns the default sheet name.
   * @return {string}
   */
  get defaultSheetName() {
    return 'Integration Config';
  }

  /**
   * Returns the column configuration for the sheet.
   * @return {!Array<!Object>}
   */
  get columnConfiguration() {
    return [
      {name: 'API', width: 50, format: COLUMN_STYLES.ALIGN_MIDDLE},
      {name: 'Config', format: COLUMN_STYLES.ALIGN_MIDDLE},
      {name: 'Config Content', jsonColumn: true, defaultNote: true, width: 500},
      {name: 'Test Data', width: 500},
      {
        name: COLUMN_NAME_FOR_DEFAULT_CONFIG,
        width: 100,
        format: COLUMN_STYLES.MONO_FONT,
      },
    ];
  }

  /**
   * Returns the content to be downloaded.
   * @return {string}
   */
  get downloadContent() {
    const entities = this.getArrayOfRowEntity();
    const result = {};
    entities.forEach(({api, config, configContent}) => {
      if (!result[api]) result[api] = {};
      result[api][config] = JSON.parse(configContent);
    });
    return JSON.stringify(result);
  }

  /**
   * Returns the default filename for downloading.
   * @return {string}
   */
  get downloadFilename() {
    const tag = this.options.sheetName
      ? `_${serpentine(this.options.sheetName)}`
      : '';
    return `config_api${tag}.json`;
  }

  /**
   * Returns the inherent menu items for the sheet.
   * @return {!Array<!Object>}
   */
  get inherentMenuItems() {
    return [
      {
        name: 'Update selected config to Firestore',
        method: 'operateSingleRow',
      },
      {
        name: 'Upload selected data as a test file to Cloud Storage',
        method: 'uploadTestData',
      },
      {separator: true},
      {name: 'Update all configs to Firestore', method: 'operateAllRows'},
      {
        name: 'Append all configs from Firestore to Sheets',
        method: 'loadFromFirestore',
      },
      {name: 'Download current sheet as a JSON file', method: 'export'},
      {separator: true},
      {name: 'Reset sheet (will lose modification)', method: 'initialize'},
    ];
  }

  /**
   * Returns a map of Tentacles Api configurations to be saved to Firestore. The
   * keys are Firestore entity Ids and the values are Firestore entities.
   *
   * @param {!Array<!IntegrationConfigRowEntity>} configs
   * @return {!Object<string, !Object>}
   * @private
   */
  getEntities_(configs) {
    const updatedConfigs = JSON.parse(
      replaceVariables(JSON.stringify(configs), getDocumentProperties()),
    );
    const entities = {};
    updatedConfigs.forEach(({api, config, configContent}) => {
      entities[`${api}.${config}`] = JSON.parse(configContent);
    });
    return entities;
  }

  /**
   * Update Tentacles Api configurations to Firestore.
   * @override
   * @param {!Array<!IntegrationConfigRowEntity>} configs
   * @return {!Array<string>}
   */
  processResources(configs) {
    const entities = this.getEntities_(configs);
    const results = MojoFunctions.saveEntitiesToFirestore(
      this.firestoreCollectionName,
      entities,
    );
    return results.map(
      ({updateTime}) => `Has been updated to Database at ${updateTime}`,
    );
  }

  /**
   * Uploads test data as a file to Cloud Storage with proper file name to
   * trigger processing.
   */
  uploadTestData() {
    const rowIndex = this.getSelectedRow();
    const {
      api,
      config,
      testData: content,
    } = this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    const projectId = getDocumentProperty('projectId');
    const bucket = getDocumentProperty('bucket');
    const outbound = getDocumentProperty('outbound');
    const timestamp = new Date().toISOString();
    const fileName = `${outbound}/API[${api}]_config[${config}]_test_by_cyborg_${timestamp}`;
    const storage = new Storage(projectId);
    const response = storage.uploadFile(fileName, bucket, content);
    const note = `Uploaded as ${response.name}.`;
    this.showOperationResult(rowIndex, note, 'black', 'Test Data');
  }

  /** Loads Api Configs from Firestore database to this Google Sheets. */
  loadFromFirestore() {
    const results = MojoFunctions.loadEntitiesFromFirestore(
      this.firestoreCollectionName,
    );
    const rows = results.map(({id, json}) =>
      id.split('.').concat(JSON.stringify(json, null, 2)),
    );
    this.getEnhancedSheet().append(rows);
  }
}
