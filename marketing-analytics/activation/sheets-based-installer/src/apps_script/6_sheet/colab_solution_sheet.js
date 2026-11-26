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
 * Packaging utility to prepare Python code blocks from Colab for Cloud Run
 * Function deployment.
 */
const ColabPackager = {
  DEFAULT_RUNTIME: 'python312',
  PROXY_FUNCTION_NAME: 'proxy',
  COLAB_MODULE_NAME: 'colab_function',

  /**
   * Generates the source files array required for
   * CloudRunFunction.uploadSource.
   * @param {string} sourceCode Python source code from the Colab cell.
   * @return {!Array<!Object<string, string>>} Array of {file, content} objects.
   */
  createSourceFiles(sourceCode) {
    return [
      {file: 'main.py', content: this.getMainFile_()},
      {
        file: 'requirements.txt',
        content: this.getRequirementsFile_(sourceCode),
      },
      {
        file: `${this.COLAB_MODULE_NAME}.py`,
        content: this.cleanSourceCode(sourceCode),
      },
    ];
  },

  /**
   * Cleans source code by commenting out shell commands (e.g. !pip install).
   * @param {string} sourceCode
   * @return {string}
   */
  cleanSourceCode(sourceCode) {
    const regex = /^! *pip *install /;
    return sourceCode
      .split('\n')
      .map(line => (regex.test(line) ? `#${line}` : line))
      .join('\n');
  },

  /**
   * Extracts dependencies from pip install commands in the Python code.
   * @param {string} sourceCode
   * @return {string}
   * @private
   */
  getRequirementsFile_(sourceCode) {
    const libraries = ['functions-framework'];
    const regex = /^#? *! *pip *install (?:-[^ ]+ +)* *(.+)$/gm;
    let match;
    while ((match = regex.exec(sourceCode)) !== null) {
      libraries.push(match[match.length - 1]);
    }
    return libraries.join('\n');
  },

  /**
   * Returns the content of the entrypoint main.py delegating requests to the
   * colab function.
   * @return {string}
   * @private
   */
  getMainFile_() {
    return `import flask
import inspect
import functions_framework
import ${this.COLAB_MODULE_NAME}

@functions_framework.http
def ${this.PROXY_FUNCTION_NAME}(request: flask.Request) -> flask.typing.ResponseReturnValue:
    request_json = request.get_json(silent=True)
    if request_json and "args" in request_json:
        if "vars" in request_json:
            for key, val in request_json["vars"].items():
                setattr(${this.COLAB_MODULE_NAME}, key, val)
        if "functionName" in request_json:
            fn = getattr(${
              this.COLAB_MODULE_NAME
            }, request_json["functionName"])
        else:
            functions = inspect.getmembers(${
              this.COLAB_MODULE_NAME
            }, predicate=inspect.isfunction)
            if len(functions) > 1:
                return "Unspecified function name", 400
            fn = functions[0][1]
        return {"result": fn(*request_json["args"])}
    else:
        return "Bad request", 400
`;
  },
};

/**
 * The sheet stores Colab code blocks that can be deployed as Cloud Run
 * Functions.
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
        name: 'Text',
        width: 300,
        format: [
          COLUMN_STYLES.ALIGN_TOP,
          {fn: 'setFontStyle', format: 'italic'},
          {fn: 'setFontColor', format: 'gray'},
        ],
      },
      {
        name: 'Code',
        width: 600,
        format: [
          COLUMN_STYLES.ALIGN_TOP,
          {fn: 'setWrapStrategy', format: SpreadsheetApp.WrapStrategy.CLIP},
        ],
      },
      {name: 'Deployed Name', width: 150},
      {name: 'Status', width: 80, defaultNote: true},
      {name: 'Test Data', width: 400, jsonColumn: true},
      {name: 'AI Generated Test Data', width: 400},
      {name: 'Metadata Id', width: 1},
      {
        name: COLUMN_NAME_FOR_DEFAULT_CONFIG,
        format: [
          COLUMN_STYLES.MONO_FONT,
          {fn: 'setWrapStrategy', format: SpreadsheetApp.WrapStrategy.WRAP},
        ],
      },
    ];
  }

  get inherentMenuItems() {
    return [
      {
        name: 'Deploy selected code as a Cloud Function',
        method: 'deployFunction',
      },
      {name: 'Update selected function status', method: 'checkFunction'},
      {separator: true},
      {name: 'Test selected function', method: 'callFunction'},
      {
        name: 'Append AI function to generate test data',
        method: 'addFormulaToGenerateTestData',
      },
      {name: 'Update test data with AI generated', method: 'copyTestData'},
      {separator: true},
      {name: 'Reload content from the Colab', method: 'reloadColab'},
      {
        name: 'Update all deployable functions status',
        method: 'checkFunctions',
      },
      {separator: true},
      {name: 'Reset sheet (will clear this sheet)', method: 'initialize'},
    ];
  }

  /**
   * Deploys the selected code block as a Cloud Run Function.
   */
  deployFunction() {
    const rowIndex = this.getSelectedRow();
    const {code, deployedName} =
      this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    if (!deployedName) {
      throw new Error('Input the deployed target name and retry.');
    }
    const properties = getDocumentProperties();
    const {projectId, locationId} = properties;
    const functionName = replaceParameters(deployedName, properties);
    const cloudRunFunction = new CloudRunFunction(projectId, locationId);

    const sourceFiles = ColabPackager.createSourceFiles(code);
    const {storageSource} = cloudRunFunction.uploadSource(sourceFiles);

    const payload = {
      buildConfig: {
        runtime: ColabPackager.DEFAULT_RUNTIME,
        entryPoint: ColabPackager.PROXY_FUNCTION_NAME,
        source: {storageSource},
      },
      serviceConfig: {
        timeoutSeconds: 3600,
        availableMemory: '4096M',
        environmentVariables: {
          DEPLOYED_BY: 'cyborg',
          VERSION: new Date().toISOString(),
        },
      },
      environment: DEFAULT_GENERATION,
    };

    const operationName = cloudRunFunction.createOrUpdate(
      functionName,
      payload,
    );
    let i = 0;
    let deploymentResult;
    let status;
    let message = 'Deploying ';
    while (status !== RESOURCE_STATUS.OK && status !== RESOURCE_STATUS.ERROR) {
      if (i % 5 === 0) {
        deploymentResult = MojoFunctions.getCloudFunctionDeployStatus(
          operationName,
          locationId,
          projectId,
        );
        status = deploymentResult.status;
        message = deploymentResult.message;
      } else {
        message += '.';
      }
      this.updateCells({status: message}, rowIndex);
      SpreadsheetApp.flush();
      Utilities.sleep(1000);
      i++;
    }
    this.updateCells({status}, rowIndex);
    const fontColor = status === RESOURCE_STATUS.ERROR ? 'red' : 'green';
    this.showOperationResult(rowIndex, deploymentResult.message, fontColor);
    if (status === RESOURCE_STATUS.OK) {
      const linkColumnIndex = this.columnName.indexOf('Deployed Name') + 1;
      this.updateLink(rowIndex, linkColumnIndex, deploymentResult.status_link);
      this.addFormulaToGenerateTestData();
    }
  }

  /**
   * Adds the AI function to generate a piece of test data for the deployed
   * Cloud Function of the selected row.
   * @param {number=} rowIndex
   */
  addFormulaToGenerateTestData(rowIndex = this.getSelectedRow()) {
    const columnIndex = this.columnName.indexOf('AI Generated Test Data') + 1;
    const codeColumn = getColumnLetter(this.columnName.indexOf('Code'));
    const cell = this.getEnhancedSheet().sheet.getRange(
      rowIndex,
      columnIndex,
      1,
      1,
    );
    cell.setFormula(`=AI("Based on the given content, generate one valid JSON
      string that will be used to invoke the function defined in this row with
      following rules:
      1, put all arguments in to the property named 'args' as an array;
      2. put the invoked function name under the property named 'functionName';
      3. put all global variables (capitalized parameters) in to the property
       named 'vars' which is a map with the names of global variables as keys;
       (if the values of global variables are missing, use the following steps
       to create the placeholder: i. lowercased variable names; ii. camelized
       the name; iii. appending a '#' sign at the beginning and end of it).",
       ${codeColumn}${rowIndex}:${codeColumn}${rowIndex + 1})`);
  }

  /**
   * Copies the AI generated test data to the 'Test Data' column.
   */
  copyTestData() {
    const rowIndex = this.getSelectedRow();
    const templateIndex = this.columnName.indexOf('AI Generated Test Data') + 1;
    const templateCell = this.getEnhancedSheet().sheet.getRange(
      rowIndex,
      templateIndex,
      1,
      1,
    );
    const value = templateCell.getDisplayValue().replaceAll(/```json|```/g, '');
    const columnIndex = this.columnName.indexOf('Test Data') + 1;
    const cell = this.getEnhancedSheet().sheet.getRange(
      rowIndex,
      columnIndex,
      1,
      1,
    );
    cell.setValue(value);
  }

  /**
   * Synchronously invokes a deployed Cloud Run Function for testing.
   */
  callFunction() {
    const rowIndex = this.getSelectedRow();
    const {deployedName, testData = '{}'} =
      this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    if (!deployedName) {
      throw new Error('Select a line with deployed target to test.');
    }
    const properties = getDocumentProperties();
    const {projectId, locationId} = properties;
    const functionName = replaceParameters(deployedName, properties);
    const realData = replaceVariables(testData, properties);

    const cloudRunFunction = new CloudRunFunction(projectId, locationId);
    const invoker = new CloudRunInvoker(cloudRunFunction, functionName);
    const ui = SpreadsheetApp.getUi();
    try {
      const result = invoker.invoke(JSON.parse(realData));
      const output = JSON.stringify(result, null, 4).replaceAll(' ', '\xa0');
      ui.alert(`Response:\n${output}`);
    } catch (error) {
      ui.alert(`Error: ${error.message}`);
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
      rows.forEach(({deployedName, testData, metadataId}) => {
        if (deployedName && metadataId) {
          this.legacyConfig[metadataId] = {deployedName, testData};
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
    try {
      const ui = SpreadsheetApp.getUi();
      const result = ui.prompt(
        'Please enter Colab link or Drive file Id of the Colab: \n\n',
      );
      if (result.getSelectedButton() === ui.Button.CLOSE) {
        return;
      }
      const inputUrl = result.getResponseText();
      this.legacyConfig = {};
      this.loadColabToSheet_(inputUrl);
    } catch (e) {
      console.log('Quit initializing', e);
      return;
    }
  }

  /**
   * Checks and updates the status of the selected function.
   * @param {!ColabCellRowEntity} row
   * @param {number} rowIndex
   * @private
   */
  updateFunctionStatus_(row, rowIndex) {
    const {code, deployedName} = row;
    if (!deployedName) {
      this.updateCells({status: 'NOT A FUNCTION'}, rowIndex, 'gray');
      return;
    }
    const properties = getDocumentProperties();
    const {projectId, locationId} = properties;
    const realName = replaceParameters(deployedName, properties);
    const cloudRunFunction = new CloudRunFunction(projectId, locationId);

    const response = cloudRunFunction.getFunction(realName);
    if (response.error) {
      if (
        response.error.status === 'NOT_FOUND' ||
        response.error.code === 404
      ) {
        this.updateCells({status: 'NOT INSTALLED'}, rowIndex, 'orange');
      } else {
        this.updateCells(
          {status: `ERROR: ${response.error.message}`},
          rowIndex,
          'red',
        );
      }
      return;
    }

    const blobs = cloudRunFunction.getSourceCode(realName);
    if (blobs.error) {
      this.updateCells({status: 'OK'}, rowIndex, 'green');
    } else {
      const blob = blobs.find(
        b => b.getName() === `${ColabPackager.COLAB_MODULE_NAME}.py`,
      );
      if (!blob) {
        this.updateCells({status: 'UPDATE AVAILABLE'}, rowIndex, 'orange');
      } else {
        const sourceCode = blob.getDataAsString();
        const newDeployableCode = ColabPackager.cleanSourceCode(code);
        if (sourceCode === newDeployableCode) {
          this.updateCells({status: 'OK'}, rowIndex, 'green');
        } else {
          this.updateCells({status: 'UPDATE AVAILABLE'}, rowIndex, 'orange');
        }
      }
    }

    const link = getLinkToCloudFunction(response);
    const linkColumnIndex = this.columnName.indexOf('Deployed Name') + 1;
    this.updateLink(rowIndex, linkColumnIndex, link);
    this.addFormulaToGenerateTestData(rowIndex);
  }

  /**
   * Gets Colab content from the given resources.
   * @param {string} inputUrl
   * @return {string}
   */
  getColabContent(inputUrl) {
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
          console.error(`Unknown Colab file source: ${inputUrl}`);
          return;
      }
    } else {
      fileId = inputUrl;
    }
    if (fileId) {
      const {name} = new Drive().getFileInfo(fileId);
      const result = new Drive().getFileContent(fileId);
      result.metadata.colab.name = result.metadata.colab.name || name;
      return result;
    } else if (colabUrl) {
      return JSON.parse(UrlFetchApp.fetch(colabUrl).getContentText());
    }
  }

  /**
   * Loads Colab content from the given resources and fill them in the sheet.
   * @param {string} inputUrl
   * @private
   */
  loadColabToSheet_(inputUrl) {
    const ui = SpreadsheetApp.getUi();
    const colab = this.getColabContent(inputUrl);
    if (!colab || !colab.cells) {
      console.log('Colab', colab);
      ui.alert(`Unknown Colab content from: ${inputUrl}`);
      return;
    }

    const data = [];
    data.push([`Colab: ${colab.metadata.colab.name}`, inputUrl]);
    const textIndex = this.fields.indexOf('text');
    const codeIndex = this.fields.indexOf('code');
    const metadataIdIndex = this.fields.indexOf('metadataId');
    let currentRow;
    colab.cells.forEach(({cell_type, source, metadata: {id}}) => {
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
          Object.keys(legacy).forEach(key => {
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
    data.forEach(row => {
      row[this.fields.length - 1] = row[this.fields.length - 1] || '';
    });

    this.data = data;
    this.getEnhancedSheet().initialize(this);
    SpreadsheetApp.flush();
    SpreadsheetApp.getActiveSpreadsheet().toast(
      `Colab content has been loaded from: ${inputUrl}`,
    );
  }
}
