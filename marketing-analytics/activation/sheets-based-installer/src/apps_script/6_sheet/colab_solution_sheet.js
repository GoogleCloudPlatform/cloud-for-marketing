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

/** @fileoverview Colab solution sheet. */

/**
 * Type of the object that each row of the sheet can be mapped to.
 *
 * @typedef {{
*   text: string|undefined,
*   code: string|undefined,
*   deployedName: string|undefined,
*   status: string|undefined,
*   testData: string|undefined,
*   metadataId: string|undefined,
* }}
*/
let ColabCellRowEntity;

/**
 * The sheet stores Colab code blocks that can be deployed as Cloud Functions.
 */
class ColabSolutionSheet extends PlainSheet {

  get initialData() {
    return this.data;
  }

  get defaultSheetName() {
    return 'Colab Solution';
  }

  get defaultHeadlineStyle() {
    return {
      backgroundColor: '#34A853',
      fontColor: 'white',
    };
  }

  get columnConfiguration() {
    return [
      {
        name: 'Text', width: 300, format: [
          COLUMN_STYLES.ALIGN_TOP,
          { fn: 'setFontStyle', format: 'italic' },
          { fn: 'setFontColor', format: 'gray' },
        ]
      },
      {
        name: 'Code', width: 600, format: [
          COLUMN_STYLES.ALIGN_TOP,
          { fn: 'setWrapStrategy', format: SpreadsheetApp.WrapStrategy.CLIP },
        ]
      },
      { name: 'Deployed Name', width: 150 },
      { name: 'Status', width: 80, defaultNote: true },
      { name: 'Test Data', width: 400, jsonColumn: true },
      { name: 'Metadata Id', width: 1 },
      {
        name: COLUMN_NAME_FOR_DEFAULT_CONFIG, format: [
          COLUMN_STYLES.MONO_FONT,
          { fn: 'setWrapStrategy', format: SpreadsheetApp.WrapStrategy.WRAP },
        ]
      },
    ];
  }

  get inherentMenuItems() {
    return [
      { name: 'Deploy selected code as a Cloud Function', method: 'deployFunction' },
      { name: 'Test selected function', method: 'callFunction' },
      { name: 'Update selected function status', method: 'checkFunction' },
      { separator: true },
      { name: 'Reload content from the Colab', method: 'reloadColab' },
      { name: 'Update all deployable functions status', method: 'checkFunctions' },
      { separator: true },
      { name: 'Reset sheet (will clear this sheet)', method: 'initialize' },
    ];
  }

  /**
   * Deploys the code as a Cloud Function.
   */
  deployFunction() {
    const rowIndex = this.getSelectedRow();
    const { code, deployedName } =
      this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    if (!deployedName) {
      throw new Error(`Input the deployed target name and retry.`);
    }
    const properties = getDocumentProperties();
    const { projectId, locationId } = properties;
    const functionName = replaceParameters(deployedName, properties);
    const colab = new ColabSolution({ projectId, locationId });
    const operationName = colab.deployCloudFunctions(functionName, code);
    let i = 0;
    let deploymentResult;
    let status;
    let message;
    while (status !== RESOURCE_STATUS.OK && status !== RESOURCE_STATUS.ERROR) {
      if (i % 20 === 0) {
        deploymentResult = gcloud.getCloudFunctionDeployStatus(
          operationName, locationId, projectId
        );
        status = deploymentResult.status;
        message = deploymentResult.message;
      } else {
        message += '.';
      }
      this.updateCells({ status: message }, rowIndex);
      SpreadsheetApp.flush();
      Utilities.sleep(1000);
      i++;
    }
    this.updateCells({ status }, rowIndex);
    const fontColor = status === RESOURCE_STATUS.ERROR ? 'red' : 'green';
    this.showOperationResult(rowIndex, deploymentResult.message, fontColor);
  }

  /**
   * Synchronously invokes a deployed Cloud Function. To be used for testing
   * purposes as very limited traffic is allowed (16 / 100 seconds).
   * @see CloudFunctionsInvoker.invoke
   */
  callFunction() {
    const rowIndex = this.getSelectedRow();
    const { deployedName, testData = '{}' } =
      this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    if (!deployedName) {
      throw new Error(`Select a line with deployed target to test.`);
    }
    const properties = getDocumentProperties();
    const { projectId, locationId } = properties;
    const functionName = replaceParameters(deployedName, properties);
    const realData = replaceVariables(testData, properties);
    const cloudFunctions = new CloudFunctionsInvoker(projectId, locationId);
    const { error, result } =
      cloudFunctions.invoke(functionName, JSON.parse(realData));
    const ui = SpreadsheetApp.getUi();
    if (error) {
      ui.alert(`Error: ${JSON.stringify(error)}`);
    } else {
      const output = JSON.stringify(result, null, 4).replaceAll(' ', '\xa0');
      ui.alert(`Get response:\n${output}`);
    }
  }

  /** Checks and updates the status of the selected function. */
  checkFunction() {
    const rowIndex = this.getSelectedRow();
    const row = this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    this.updateFunctionStatus_(row, rowIndex);
  }

  /** Reloads the Colab to the sheet. */
  reloadColab() {
    const rows = this.getArrayOfRowEntity() || [];
    const inputUrl = rows[0] ? rows[0].code : '';
    this.legacyConfig = {};
    if (inputUrl) {
      rows.forEach(({ deployedName, testData, metadataId }) => {
        if (deployedName && metadataId) {
          this.legacyConfig[metadataId] = { deployedName, testData, };
        }
      });
      this.loadColabToSheet_(inputUrl);
    } else {
      this.initialize();
    }
  }

  /** Checks and updates the status of all functions. */
  checkFunctions() {
    this.getArrayOfRowEntity().forEach((row, index) => {
      const rowIndex = index + ROW_INDEX_SHIFT;
      this.updateFunctionStatus_(row, rowIndex);
    });
  }

  /** @override */
  initialize() {
    const ui = SpreadsheetApp.getUi();
    const result = ui.prompt(
      'Please enter Colab link or Drive file Id of the Colab: \n\n');
    if (result.getSelectedButton() === ui.Button.CLOSE) {
      return;
    }
    const inputUrl = result.getResponseText();
    this.legacyConfig = {};
    this.loadColabToSheet_(inputUrl);
  }

  /**
   * Checks and updates the status of the selected function.
   * @private
   */
  updateFunctionStatus_(row, rowIndex) {
    const { code, deployedName } = row;
    if (!deployedName) {
      this.updateCells({ status: 'NOT A FUNCTION' }, rowIndex, 'gray');
      return;
    }
    const properties = getDocumentProperties();
    const { projectId, locationId } = properties;
    const realName = replaceParameters(deployedName, properties);
    const colab = new ColabSolution({ projectId, locationId });
    const sourceCode = colab.downloadSourceCode(realName);
    const { error } = sourceCode;
    if (error) {
      if (error.status === 'NOT_FOUND') {
        this.updateCells({ status: 'NOT INSTALLED' }, rowIndex, 'orange');
      } else {
        this.updateCells({ status: `ERROR: ${error.message}` }, rowIndex, 'red');
      }
    } else {
      console.log('old', sourceCode.length)
      const newDeployableCode = colab.getDeployableCode(code);
      console.log('new', newDeployableCode.length)
      const newLines = newDeployableCode.split('\n');
      const sourceLines = sourceCode.split('\n');
      const equal = newLines.every((line, index) => {
        if (sourceLines[index] === line) return true;
        console.log('different', line, sourceLines[index]);
        return false;
      });
      console.log(equal)
      if (equal && sourceLines.length === newLines.length) {
        this.updateCells({ status: 'OK' }, rowIndex, 'green');
      } else {
        this.updateCells({ status: 'UPDATE available' }, rowIndex, 'orange');
      }
    }
  }

  /**
   * Loads Colab content from the given resources and fill them in the sheet.
   * @param {string} inputUrl
   * @private
   */
  loadColabToSheet_(inputUrl) {
    const ui = SpreadsheetApp.getUi();
    let colabUrl;
    let fileId;
    const regex = /^(https:\/\/)?colab.\w*.google.com\/([^/]*)\/([^#?]*)/g;
    const matches = regex.exec(inputUrl);
    if (matches) {
      const resourceId = matches[3];
      switch (matches[2]) {
        case 'github':
          const githubLink = resourceId.replace('/blob/', '/');
          colabUrl = `https://raw.githubusercontent.com/${githubLink}`;
          break;
        case 'drive':
          fileId = resourceId;
          break;
        default:
          ui.alert(`Unknown Colab file source: ${inputUrl}`);
          return;
      }
    } else {
      fileId = inputUrl;
    }

    let colab;
    if (fileId) {
      colab = new Drive().getFileContent(fileId);
    } else if (colabUrl) {
      colab = JSON.parse(UrlFetchApp.fetch(colabUrl).getContentText());
    }
    if (!colab || !colab.cells) {
      console.log('Colab', colab);
      ui.alert(`Unknown Colab content from: ${inputUrl}`);
      return;
    }

    const data = [];
    if (fileId) {
      const { name } = new Drive().getFileInfo(fileId);
      data.push([`Colab: ${name}`, inputUrl]);
    } else {
      data.push([`Colab: ${colab.metadata.colab.name}`, inputUrl]);
    }
    const textIndex = this.fields.indexOf('text');
    const codeIndex = this.fields.indexOf('code');
    const metadataIdIndex = this.fields.indexOf('metadataId');
    let currentRow;
    colab.cells.forEach(({ cell_type, source, metadata: { id } }) => {
      if (cell_type === 'markdown') {
        currentRow = [];
        currentRow[textIndex] = source.join('');
        data.push(currentRow);
      } else if (cell_type === 'code') {
        const codeRow = currentRow ? currentRow : [];
        codeRow[codeIndex] = source.join('');
        codeRow[metadataIdIndex] = id;
        if (this.legacyConfig[id]) {
          const legacy = this.legacyConfig[id];
          Object.keys(legacy).forEach((key) => {
            codeRow[this.fields.indexOf(key)] = legacy[key];
          });
          console.log('got legacy for', id, codeRow);
        }
        if (currentRow) {
          currentRow = undefined;
        } else {
          data.push(codeRow);
        }
      }
    });
    data.forEach((row) => {
      row[this.fields.length - 1] = row[this.fields.length - 1] || '';
    });

    this.data = data;
    this.getEnhancedSheet().initialize(this);
    SpreadsheetApp.flush();
    SpreadsheetApp.getActiveSpreadsheet()
      .toast(`Colab content has been loaded from: ${inputUrl}`);
  }
}
