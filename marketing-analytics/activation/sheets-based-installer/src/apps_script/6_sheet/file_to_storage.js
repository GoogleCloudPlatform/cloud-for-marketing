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
   * In the parameter `options`, there are two properties for intial data.
   * @param {Object<string,string>} options.files A map of files, the 'key' is
   *   the file name, the 'value' is the file content or a Url link.
   * @param {string=} options.filePath Prefix of the file. It starts with the
   *   Cloud Storage bucket name and an optional folder name, e.g.
   *   '#bucket#/sql/'.
   */
  get initialData() {
    const { files = {}, filePath } = this.options || {};
    return Object.keys(files).map((key) => [`${filePath}${key}`, files[key]]);
  }

  get defaultSheetName() {
    return 'File to Storage';
  }

  get columnConfiguration() {
    return [
      {
        name: 'Cloud Storage Path', width: 400,
        format: COLUMN_STYLES.ALIGN_MIDDLE,
      },
      {
        name: 'File', width: 700, defaultNote: true,
        format: { fn: 'setWrapStrategy', format: SpreadsheetApp.WrapStrategy.CLIP },
      },
      { name: COLUMN_NAME_FOR_DEFAULT_CONFIG, format: COLUMN_STYLES.MONO_FONT },
    ];
  }

  get inherentMenuItems() {
    return [
      { name: 'Upload selected file', method: 'operateSingleRow' },
      { separator: true },
      { name: 'Upload all files', method: 'operateAllRows' },
      { name: 'Reset sheet (will lose modification)', method: 'initialize' },
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
    return files.map(({ cloudStoragePath, file }) => {
      const target = replaceVariables(cloudStoragePath, properties);
      const bucket = target.substring(0, target.indexOf('/'));
      const fileName = target.substring(target.indexOf('/') + 1);
      let fileTemplate;
      if (file.substring(0, 5).toLowerCase().startsWith('http')) {
        fileTemplate = UrlFetchApp.fetch(file).getContentText();
      } else {
        fileTemplate = file;
      }
      const content = replaceVariables(fileTemplate, properties);
      const response = storage.uploadFile(fileName, bucket, content);
      return response;
    }).map(({ updated }) => `Has been uploaded to Cloud Storage at ${updated}`);
  }

}
