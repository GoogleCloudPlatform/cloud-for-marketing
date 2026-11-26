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

/** @fileoverview The sheet for files will be copied to Cloud Storage. */

/**
 * Type of the object that each row of the sheet can be mapped to.
 *
 * @typedef {{
 *   cloudStoragePath: string,
 *   file: string,
 * }}
 */
let FileToStorageRowEntity;

/**
 * The sheet stores files will be uploaded to Cloud Storage. Usually these files
 * are SQL files.
 */
class FileToStorage extends PlainSheet {
  /**
   * In the parameter `options`, there are two properties for initial data:
   * `files`: A map of files, the 'key' is the file name, the 'value' is the
   *   file content or a Url link.
   * `filePath`: Prefix of the file. It starts with the Cloud Storage bucket
   *   name and an optional folder name, e.g. '#bucket#/sql/'.
   * @return {!Array<!Array<string>>}
   */
  get initialData() {
    const {files = {}, filePath} = this.options || {};
    return Object.keys(files).map(key => [`${filePath}${key}`, files[key]]);
  }

  /**
   * Returns the default sheet name.
   * @return {string}
   */
  get defaultSheetName() {
    return 'File to Storage';
  }

  /**
   * Returns the column configuration for the sheet.
   * @return {!Array<!Object>}
   */
  get columnConfiguration() {
    return [
      {
        name: 'Cloud Storage Path',
        width: 400,
        format: COLUMN_STYLES.ALIGN_MIDDLE,
      },
      {
        name: 'File',
        width: 700,
        defaultNote: true,
        format: {
          fn: 'setWrapStrategy',
          format: SpreadsheetApp.WrapStrategy.CLIP,
        },
      },
      {name: COLUMN_NAME_FOR_DEFAULT_CONFIG, format: COLUMN_STYLES.MONO_FONT},
    ];
  }

  /**
   * Returns the inherent menu items for the sheet.
   * @return {!Array<!Object>}
   */
  get inherentMenuItems() {
    return [
      {name: 'Upload selected file', method: 'operateSingleRow'},
      {separator: true},
      {name: 'Upload all files', method: 'operateAllRows'},
      {name: 'Reset sheet (will lose modification)', method: 'initialize'},
    ];
  }

  /**
   * Uploads files to target Cloud Storage with specified paths.
   * @override
   * @param {!Array<!FileToStorageRowEntity>} files
   * @return {!Array<string>}
   */
  processResources(files) {
    const projectId = getDocumentProperty('projectId');
    const storage = new Storage(projectId);
    const properties = getDocumentProperties();
    return files
      .map(({cloudStoragePath, file}) => {
        const target = replaceVariables(cloudStoragePath, properties);
        const slashIndex = target.indexOf('/');
        const bucket =
          slashIndex > -1 ? target.substring(0, slashIndex) : target;
        const fileName =
          slashIndex > -1 ? target.substring(slashIndex + 1) : '';
        if (!fileName) {
          return {
            error: {
              message: `Invalid Cloud Storage path '${
                target
              }': missing object filename.`,
            },
          };
        }
        let fileTemplate;
        if (file.substring(0, 5).toLowerCase().startsWith('http')) {
          fileTemplate = UrlFetchApp.fetch(file).getContentText();
        } else if (file.startsWith('notebook:')) {
          fileTemplate = this.getNotebook(file);
        } else {
          fileTemplate = file;
        }
        const content = replaceVariables(fileTemplate, properties);
        if (fileName.toLowerCase().endsWith('.zip')) {
          return storage.uploadZip(fileName, bucket, content);
        } else {
          return storage.uploadFile(fileName, bucket, content);
        }
      })
      .map(response => {
        if (!response || response.error) {
          const errMsg =
            response && response.error
              ? response.error.message || JSON.stringify(response.error)
              : 'Unknown error';
          return `Failed to upload: ${errMsg}`;
        }
        return `Has been uploaded to Cloud Storage at ${
          response.updated || new Date().toISOString()
        }`;
      });
  }

  /**
   * Get the Notebook content based on configuration. The expected format is:
   * `notebook:COLAB_SHEET_NAME:B2,B4,B7`, etc.
   * @param {string} file
   * @return {string|undefined}
   */
  getNotebook(file) {
    const regex = /notebook:(['"]?)(.*?)\1:(.+)/;
    const match = file.match(regex);
    if (!match) {
      throw new Error(`Unknown Notebook definition: ${file}`);
    }
    const sheetName = match[2].trim();
    const colabCells = match[3];
    const colabSheet = PlainSheet.instances[getSafePropertyName(sheetName)];
    if (!colabSheet) {
      throw new Error(`Can't find sheet named ${sheetName}`);
    }
    const rows = colabSheet.getArrayOfRowEntity() || [];
    const inputUrl = rows[0] ? rows[0].code : '';
    const colab = colabSheet.getColabContent(inputUrl);
    const ui = SpreadsheetApp.getUi();
    if (!colab || !colab.cells) {
      console.log('Colab', colab);
      ui.alert(`Unknown Colab content from: ${inputUrl}`);
      return;
    }
    colab.cells = colabCells.split(',').map(cell => {
      const index = cell.trim().substring(1) - ROW_INDEX_SHIFT; // 'B3' -> 1
      const id = rows[index].metadataId;
      return {
        cell_type: 'code',
        source: rows[index].code.split('\n').map(line => line + '\n'),
        metadata: {
          id,
          cellView: 'form',
        },
        id,
        execution_count: null,
        outputs: [],
      };
    });
    return JSON.stringify(colab);
  }
}
