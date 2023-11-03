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

/** @fileoverview The class stands for a solution (mojo). */

/**
 * Mojo sheet rows start at index 2 as it is 1-based index and has 1 head line.
 * @const {number}
 */
const ROW_INDEX_SHIFT = 2;

/**
 * Edit types of a GCP resource.
 * @enum {string}
 */
const RESOURCE_EDIT_TYPE = Object.freeze({
  // This is expecting users to input.
  USER_INPUT: 'USER_INPUT',
  // This has a default value, but users can modify it.
  DEFAULT_VALUE: 'DEFAULT_VALUE',
  // This is readonly.
  READONLY: 'READONLY',
});

/**
 * Types of whether a GCP resource will be checked or enabled in the solution.
 * @enum {string}
 */
const OPTIONAL_TYPE = Object.freeze({
  //This resource is mandatory. This is the default value.
  MANDATORY: 'MANDATORY',
  //This resource is optional, default checked.
  DEFAULT_CHECKED: 'DEFAULT_CHECKED',
  //This resource is optional, default unchecked.
  DEFAULT_UNCHECKED: 'DEFAULT_UNCHECKED',
});

/**
 * Status of a GCP resource.
 * @enum {string}
 */
const RESOURCE_STATUS = Object.freeze({
  OK: 'OK',
  READY_TO_INSTALL: 'TO_APPLY',
  ERROR: 'ERROR',
});

/**
 * Check result of a resource.
 * @typedef{{
 *   status: RESOURCE_STATUS,
 *   message: string|undefined,
 *   value: string|undefined,
 *   value_datarange: Array<string>|function|undefined,
 *   attributeValue: string|undefined,
 *   attributeValue_datarange: Array<string>|undefined,
 *   refreshFn: function|undefined,
 * }}
 */
let CheckResult;

/**
 * @typedef {{
 *   template: string|undefined,
 *   categoery: string|undefined,
 *   resource: string|undefined,
 *   value: string|function|Array<string>|undefined,
 *   editType: !RESOURCE_EDIT_TYPE,
 *   optionalType: !OPTIONAL_TYPE|undefined,
 *   group: string|undefined,
 *   value_datarange: Array<string>|function|undefined,
 *   attributeName: string|undefined,
 *   attributeValue: string|undefined,
 *   attributeValue_datarange: Array<string>|function|undefined,
 *   propertyName: string|undefined,
 *   alwaysCheck: boolean|undefined,
 *   checkFn: function|undefined,
 *   enableFn: function|undefined,
 * }}
 */
let MojoResource;

/**
 * Definition of a solution that will be deployed by Cyborg. It contains:
 *  1. `sheetName`, name of the sheet
 *  2. `config`, an array of GCP resources(@see MojoResource)
 *  3. `headlineStyle`, optional settings for the style of the sheet, e.g. color
 *
 * @typedef {{
 *   sheetName: string,
 *   config: !Array<!MojoResource>,
 *   headlineStyle: object|undefined,
 * }}
 */
let MojoConfig;

/**
 * Configurations for a Mojo Sheet.
 * @const {!SheetConfig}
 */
const MOJO_SHEET_CONFIG = {
  columnName: [
    'Category',
    'Resource',
    'Enable',
    'Value',
    'Attribute Name',
    'Attribute Value',
    'Status',
    'Note',
    'Property Name'
  ],
  columnWidth: {
    'Resource': 120,
    'Enable': 50,
    'Value': 400,
    'Attribute Value': 250,
    'Note': 400,
    'Property Name': 150,
    default_: 100,
  },
  columnFormat: {
    'Category': COLUMN_STYLES.ALIGN_MIDDLE_AND_CENTER,
    'Resource': COLUMN_STYLES.ALIGN_MIDDLE_AND_CENTER,
    'Enable': COLUMN_STYLES.ALIGN_MIDDLE_AND_CENTER,
    'Attribute Name': { fn: 'setFontColor', format: '#5F6368' },
    'Status': COLUMN_STYLES.ALIGN_MIDDLE_AND_CENTER,
    default_: { fn: 'setFontFamily', format: 'Google Sans' },
  }
};

/**
 * Type of objects restored from rows of Mojo sheet. This type objects are used
 * for checking or enabling.
 *
 * @typedef {{
 *   categoery: string,
 *   resource: string,
 *   enable: string,
 *   value: string,
 *   attributeName: string,
 *   attributeValue: string,
 *   status: string,
 *   note: string,
 *   propertyName: string,
 * }}
 */
let MojoSheetRow;

/**
 * Mojo sheet field names.
 * @const {!Array<string>}
 */
const MOJO_FIELDS = MOJO_SHEET_CONFIG.columnName.map(camelize);

/**
 * The columns will be merged vertically.
 * @const {!Array<string>}
 */
const MOJO_COLUMN_TO_BE_MERGED = Object.freeze([
  'Category',
  'Resource',
]);

/**
 * Conditioanl format for status cells.
 * @const {!Object<RESOURCE_STATUS, string>}
 */
const STATUS_BACKGROUND_COLOR = Object.freeze({
  [RESOURCE_STATUS.OK]: '#CEEAD6',
  [RESOURCE_STATUS.READY_TO_INSTALL]: '#FEEFC3',
  [RESOURCE_STATUS.ERROR]: '#FAD2CF',
});

/**
 * Class Mojo stands for a group of GCP resources that belong to one solution.
 * This class offers the functions to (1) create a sheet based on those GCP
 * resources; (2) run `check` functions for those resources update the sheet
 * with results; (3) run `enable` functions to create or update related
 * resources.
 */
class Mojo {

  /**
   * Creates the Mojo instance based on a MojoConfig.
   * @constructor
   * @param {!MojoConfig} solution
   */
  constructor(solution) {
    // Restore mojo resource settings from all templates.
    this.solutionConfig = solution.config.map((row) => {
      const configChain = [row];
      let latest = row;
      while (latest.template) {
        latest = MOJO_CONFIG_TEMPLATE[latest.template];
        configChain.push(latest);
      }
      return Object.assign({}, ...configChain.reverse());
    });
    this.enhancedSheet = new EnhancedSheet(solution.sheetName);
    this.headlineStyle = solution.headlineStyle;
    /**
     * Whether the `checkResource` should check the resource with status `OK`.
     * By default it is `false`. If there was a `property` updated, then this
     * will be set as `true` to check all the following resources regardless
     * their status.
     * This might be updated when the `updateStatus_` (@see updateStatus_) is
     * executed, depending on whether there are changes of properties.
     * @const {boolean=}
     */
    this.forceCheck = false;
    /**
     * For those long time operations, e.g. deploying a Cloud Functions, the
     * `note` cell is used to show the process and final result. This kind of
     * update needs extra functions. This is used to store the `rowIndex` and
     * the related `function`.
     * This will be updated when the `updateStatus_` (@see updateStatus_) is
     * executed.
     *
     * @const {!Object<number, function>}
     */
    this.longTimeOperations = {};
  }

  /**
   * Initializes the sheet.
   */
  initializeSheet() {
    // Clear sheet and create headline
    this.enhancedSheet.initialize(MOJO_SHEET_CONFIG, this.headlineStyle);

    const { sheet } = this.enhancedSheet;
    // Mojo sheet last column index
    const numColumns = MOJO_FIELDS.length;
    const valueCellIndex = MOJO_FIELDS.indexOf('value') + 1;
    const enableCellIndex = MOJO_FIELDS.indexOf('enable') + 1;
    /**
     * A map for properties and their A1 notations. This is used to replace
     * values that have placeholders like `${propertyName}`.
     */
    const propertyA1s = {};
    // The whole category will be enabed or disabled as a group.
    this.groupedCategory = {};
    /**
     * Returns an array that stands for a row data in sheet from a MojoResource.
     * @param {!MojoResource} row
     * @param {number} index
     * @return {!Array<string>}
     */
    const generateSheetRow = (row, index) => {
      const result = [];
      const rowIndex = index + ROW_INDEX_SHIFT;
      // 'value' cell format & value
      const valueCell = sheet.getRange(rowIndex, valueCellIndex, 1, 1);
      this.setValueCell_(valueCell, row.editType);
      Object.keys(row).forEach((key) => {
        if (typeof row[key] === 'string' && row[key].indexOf('${') > -1) {
          // Update place holders with sheet functions
          row[key] = this.replacePlaceHolder_(row[key], propertyA1s);
        }
      });
      // 'enable' cell format & value
      const enableCell = sheet.getRange(rowIndex, enableCellIndex, 1, 1);
      const { group, optionalType = OPTIONAL_TYPE.MANDATORY } = row;
      row.enable = this.initializeEnableCell_(enableCell, group, optionalType);
      if (row.propertyName) {
        propertyA1s[row.propertyName] = valueCell.getA1Notation();
      }
      // Prepare data array from the row
      Object.keys(row).forEach((key) => {
        const value = row[key];
        const index = MOJO_FIELDS.indexOf(key);
        if (index > -1) {
          result[index] = typeof value === 'function' ? value(row) : value;
        }
        if (key.endsWith('datarange')) { //Set a droplist for the cell.
          const columnName = key.split('_')[0];
          this.setDropList_(columnName, rowIndex, value);
        }
      });
      // Prepare links for the row
      Object.keys(row)
        .filter((key) => key.endsWith('link'))
        .forEach((key) => {
          const columnName = key.split('_')[0];
          const index = MOJO_FIELDS.indexOf(columnName);
          const url = row[key].startsWith('=')
            ? row[key].substring(1)
            : `"${row[key]}"`;
          result[index] = `=HYPERLINK(${url}, "${result[index]}")`;
        });
      // Fill the last column of this row
      if (!result[numColumns - 1]) result[numColumns - 1] = '';
      return result;
    };

    const flatConfig = this.flattenValue_(); // Flatten MojoResources
    const sheetRows = flatConfig.map(generateSheetRow); // Map to sheet data

    // Set content and style.
    sheet.getRange(ROW_INDEX_SHIFT, 1, flatConfig.length, numColumns)
      .setValues(sheetRows)
      .setWrapStrategy(SpreadsheetApp.WrapStrategy.WRAP)
      .setBorder(true, true, true, true, true, true, '#9AA0A6'
        , SpreadsheetApp.BorderStyle.SOLID);

    // Merge 'Category' and 'Resource' vertically.
    this.enhancedSheet.mergeVertically(
      MOJO_COLUMN_TO_BE_MERGED.map(
        (column) => MOJO_SHEET_CONFIG.columnName.indexOf(column)
      )
    );

    // Status column conditional format
    const statusIndex = MOJO_FIELDS.indexOf('status') + 1;
    const statusColumn = sheet.getRange(
      ROW_INDEX_SHIFT, statusIndex, sheet.getMaxRows(), 1);
    const statusRules = Object.keys(STATUS_BACKGROUND_COLOR).map(
      (key) => {
        return SpreadsheetApp.newConditionalFormatRule()
          .whenTextEqualTo(key).setBackground(STATUS_BACKGROUND_COLOR[key])
          .setRanges([statusColumn]).build();
      }
    );
    sheet.setConditionalFormatRules(statusRules);
  }

  /**
   * Checks the resources in Mojo sheet.
   * @param {boolen} forceCheck
   */
  checkResources(forceCheck) {
    const allMojoSheetRows = this.loadSheet_();
    const forcedCheck = forceCheck || this.forceCheck;
    if (forcedCheck) this.clearStatus_(ROW_INDEX_SHIFT);
    allMojoSheetRows.forEach((mojoSheetRows) => {
      const { rowIndex: resourceStartRow } = mojoSheetRows[0];
      const succeeded = this.checkResources_(mojoSheetRows,
        forceCheck || this.forceCheck);
      console.log('Result of checking:', succeeded);
      SpreadsheetApp.flush();
      if (!succeeded) {
        this.clearStatus_(resourceStartRow + mojoSheetRows.length);
        throw new Error(`Check the row with 'ERROR' for details.`);
      }
      this.waitLongTimeOperations_();
    });
  }

  /**
   * Applys changes in the Mojo sheet.
   */
  applyChanges() {
    const allMojoSheetRows = this.loadSheet_();
    allMojoSheetRows.forEach((mojoSheetRows) => {
      const mojoResource = this.solutionConfig.filter(({ category, resource }) => {
        return category === mojoSheetRows[0].category
          && resource === mojoSheetRows[0].resource;
      })[0];
      const { enableFn: createOrUpdate, resource } = mojoResource;
      if (typeof createOrUpdate === 'undefined') {
        console.log(`Skip resource [${resource}] as no enable function.`);
        return;
      }
      console.log('Start applying resource:', resource);
      mojoSheetRows
        .filter(({ status }) => status === RESOURCE_STATUS.READY_TO_INSTALL)
        .forEach((resource) => {
          const { value, rowIndex } = resource;
          /** @type {CheckResult} */ let updatedStatus;
          try {
            updatedStatus = createOrUpdate(value, resource);
          } catch (error) {
            updatedStatus = {
              status: RESOURCE_STATUS.ERROR,
              message: error.message,
            };
          }
          this.updateStatus_(updatedStatus, rowIndex);
        });
    });
    this.waitLongTimeOperations_();
  }

  /**
   * Returns an array of `MojoResource` with flat `value`.
   * `value` in MojoResource can be a function or an array. This function will
   * get the final value(s) and split them into multiple resource which has only
   * one string `value`. The flat MojoResources are ready for status check.
   * @return {!Array<!MojoResource>}
   * @private
   */
  flattenValue_() {
    const flatConfig = [];
    this.solutionConfig.forEach((row) => {
      if (typeof row.value === 'function') row.value = row.value();
      if (!Array.isArray(row.value)) {
        flatConfig.push(row);
      } else {
        flatConfig.push(
          ...row.value.map(
            (singleValue) => Object.assign({}, row, { value: singleValue })
          ));
      }
    });
    return flatConfig;
  }

  /**
   * Sets the 'value' cells format based on the edit type.
   * @param {!Range} valueCell
   * @param {!RESOURCE_EDIT_TYPE} editType
   * @private
   */
  setValueCell_(valueCell, editType) {
    valueCell.setVerticalAlignment('middle');
    if (editType === RESOURCE_EDIT_TYPE.USER_INPUT) {
      valueCell.setBackground('#FDD663');
    } else if (editType === RESOURCE_EDIT_TYPE.READONLY) {
      valueCell.setFontColor('#5F6368')
        .setBackground('#F1F3F4')
        .setFontStyle('italic');
    }
  }

  /**
   * Replaces property placeholders in a string with functions in Sheet.
   * Sometimes the property value can not fit in the target directly. For
   * example, when the GCP project Id has `:` or `google` in it, it can not be
   * used as part of a Cloud Storage bucket name, which by default is
   * '${namespace}-${projectId}'. In this case, use a `normalized` suffix to
   * get the normalized value like this: '${namespace}-${projectId_normalized}'.
   *
   * @param {string} str
   * @param {Object<string,string>} propertyA1s
   * @return {string} A string of Sheets functions.
   * @private
   */
  replacePlaceHolder_(str, propertyA1s) {
    const result = [];
    const regex = /\$\{(\w+)\}/;
    let left = str;
    while (left.indexOf('${') > -1) {
      const indexOfFirstPlaceholder = left.indexOf('${');
      result.push(`"${left.substring(0, indexOfFirstPlaceholder)}"`);
      const matchResult = left.match(regex);
      const [property, normalized] = matchResult[1].split('_');
      if (propertyA1s[property]) {
        if (normalized === 'normalized') {
          result.push(`REGEXREPLACE(${propertyA1s[property]},":|google","-")`);
        } else {
          result.push(propertyA1s[property]);
        }
      } else {
        result.push(`"${matchResult[0]}"`);
      }
      left = left.substring(indexOfFirstPlaceholder + matchResult[0].length);
    }
    result.push(`"${left}"`);
    const segments = result.filter((e) => e !== '""').join(',');
    return `=CONCATENATE(${segments})`;
  }

  /**
   * Sets the 'enable' cells value and format based on the edit type.
   * If it is mandatory, then the cell shows a dot, otherwise it shows a
   * checkbox.
   * If it belongs to a 'grouped' resources, which should be enabled or disabled
   * altogether, then all those checkboxes will equal to the first one through
   * Google Sheets equals sign.
   * @param {!Range} enableCell
   * @param {string} group
   * @param {!OPTIONAL_TYPE} optionalType
   * @return {string|boolean} The value of 'Enable' cell of this MojoResource.
   * @private
   */
  initializeEnableCell_(enableCell, group, optionalType) {
    enableCell.setVerticalAlignment('middle');
    if (this.groupedCategory[group]) {
      enableCell.insertCheckboxes();
      enableCell.setBackground('#E8EAED');
      return this.groupedCategory[group];
    } else if (optionalType === OPTIONAL_TYPE.MANDATORY) {
      return '●';
    } else {
      enableCell.insertCheckboxes();
      if (group) {
        this.groupedCategory[group] = `=${enableCell.getA1Notation()}`;
      }
      return optionalType === OPTIONAL_TYPE.DEFAULT_CHECKED;
    }
  }

  /**
   * Sets DataValidation of a given cell.
   * @see https://developers.google.com/apps-script/reference/spreadsheet/data-validation
   * @param {string} columnName
   * @param {number} rowIndex
   * @param {(Array<string>|Function)} datarange
   * @private
   */
  setDropList_(columnName, rowIndex, datarange) {
    const range = this.enhancedSheet.sheet.getRange(
      rowIndex, MOJO_FIELDS.indexOf(columnName) + 1, 1, 1);
    setSelectDroplist(range,
      typeof datarange === 'function' ? datarange() : datarange);
  }

  /**
   * Loads Mojo sheet into an array. The elements of the array are the array of
   * `MojoSheetRow` that has the same `category` and `resource`.
   * @return {!Array<!Array<!MojoSheetRow>>>}
   * @private
   */
  loadSheet_() {
    const mojoSheetRows = this.enhancedSheet.loadArrayOfEntity(
      { fields: MOJO_FIELDS }
    );
    const result = [];
    let category;
    let index = 0;
    do {
      const line = mojoSheetRows[index];
      if (line.category && line.category !== category) {
        category = line.category;
      } else {
        line.category = category;
      }
      const values = [];
      result.push(values);
      do {
        values.push(
          Object.assign({ rowIndex: index + ROW_INDEX_SHIFT }, mojoSheetRows[index])
        );
        this.loadProperty_(mojoSheetRows[index]);
        index++;
      } while (index < mojoSheetRows.length && mojoSheetRows[index].resource === '')
    } while (index < mojoSheetRows.length)
    console.log('Properties in Sheet',
      PropertiesService.getDocumentProperties().getProperties());
    return result;
  }

  /**
   * Loads a property to document property if the line has `propertyName` and
   * the status is not `ERROR`. The `forceCheck` will be set as true if any
   * property was changed.
   * @param {!Object<string, string>} line
   * @private
   */
  loadProperty_(line) {
    const { enable, value, propertyName, status } = line;
    const updatedValue = enable === 'FALSE' ? '' : value;
    if (propertyName && status !== RESOURCE_STATUS.ERROR) {
      this.updateProperty_(propertyName, updatedValue);
    }
  }

  /**
   * Updates the document property. It the updated value is differnt from the
   * existing value, it will set the `forceCheck` to true.
   * @param {string} name
   * @param {string} value
   * @private
   */
  updateProperty_(name, value) {
    const updated = updateDcoumentPropertyValue(name, value);
    if (updated) console.log(`Updated property '${name}' to`, value);
    this.forceCheck = this.forceCheck || updated;
    if (name === 'location') {
      const locationId = getLocationId(value);
      this.updateProperty_('locationId', locationId);
    }
  }

  /**
   * Checks and updates status of an array of `MojoSheetRow` with the same
   * `category` and `resource`. It will get `checkFn` and `alwaysCheck` from the
   * original `MojoConfig` to do the checking.
   *
   * @param {!Array<!MojoSheetRow>} mojoSheetRows
   * @param {boolean} forceCheck Whether to check when the status is `OK`.
   * @return {boolean}
   * @private
   */
  checkResources_(mojoSheetRows, forceCheck) {
    const mojoResource = this.solutionConfig.filter(({ category, resource }) => {
      return category === mojoSheetRows[0].category
        && resource === mojoSheetRows[0].resource;
    })[0];
    const { checkFn: checkStatus, alwaysCheck, resource } = mojoResource;
    console.log(`Start ${forceCheck ? 'FORCED ' : ''}checking:`, resource);
    return mojoSheetRows.map((mojoSheetRow) => {
      const { value, status, enable, rowIndex, propertyName } = mojoSheetRow;
      if (!forceCheck && !alwaysCheck && status === RESOURCE_STATUS.OK) {
        console.log(`Skip row [${rowIndex}] with status`, RESOURCE_STATUS.OK);
        return;
      }
      if (enable === 'FALSE') {
        console.log(`Skip row [${rowIndex}] as unchecked enable resource`);
        if (propertyName) {
          console.log(`  unset property [${propertyName}]`);
          this.updateProperty_(propertyName, '');
        }
        return;
      }
      if (typeof checkStatus === 'undefined') {
        console.log(`Skip row [${rowIndex}] as no check function`);
        return;
      }
      /** @type {CheckResult} */ let updatedStatus;
      try {
        updatedStatus = checkStatus(value, mojoSheetRow);
        if (propertyName) {
          this.updateProperty_(propertyName, updatedStatus.value || value);
        }
      } catch (error) {
        updatedStatus = {
          status: RESOURCE_STATUS.ERROR,
          message: error.message,
        };
      }
      console.log(`Row [${rowIndex}] checking result`, updatedStatus);
      this.updateStatus_(updatedStatus, rowIndex);
      return updatedStatus;
    }).filter((checkResult) => checkResult !== undefined)
      .every(({ status }) => status !== RESOURCE_STATUS.ERROR);
  }

  /**
   * Updates `note` cell for long time operations. It will execute the
   * registed `refereshFn` to get the updated status every 20 seconds, between
   * two checks, the `note` cell will be updated with a increasing dot string
   * to show as it is processing.
   * @private
   */
  waitLongTimeOperations_() {
    let rowIndexes = Object.keys(this.longTimeOperations);
    const notes = {};
    let i = 0;
    while (rowIndexes.length > 0) {
      rowIndexes.forEach((rowIndex) => {
        if (i % 20 === 0) {
          const status = this.longTimeOperations[rowIndex]();
          this.updateStatus_(status, rowIndex);
          notes[rowIndex] = status.message;
        } else {
          notes[rowIndex] += '.';
          this.updateCells_({ note: notes[rowIndex] }, rowIndex);
        }
      });
      SpreadsheetApp.flush();
      Utilities.sleep(1000);
      i++;
      rowIndexes = Object.keys(this.longTimeOperations);
    }
  }

  /**
   * Updates checking result to the specified row. If there is a `refreshFn` in
   * the result, it will register it to instance property `registedResources`
   * for continued updating.
   * @see waitLongTimeOperations_
   * @param {!CheckResult} rowStatus
   * @param {number} rowIndex
   * @private
   */
  updateStatus_(rowStatus, rowIndex) {
    const { status, message = '', value, attributeValue, refreshFn } = rowStatus;
    const cellValues = { status, note: message, value, attributeValue };
    this.updateCells_(cellValues, rowIndex);
    Object.keys(rowStatus)
      .filter((key) => key.endsWith('datarange'))
      .forEach((key) => {
        const columnName = key.split('_')[0];
        this.setDropList_(columnName, rowIndex, rowStatus[key]);
      });
    Object.keys(rowStatus)
      .filter((key) => key.endsWith('link'))
      .forEach((key) => {
        const columnName = key.split('_')[0];
        this.setLink_(columnName, rowIndex, rowStatus[key]);
      });
    if (refreshFn) {
      console.log(`Set row ${rowIndex} refresh Fn`, refreshFn);
      this.longTimeOperations[rowIndex] = refreshFn;
    } else {
      if (this.longTimeOperations[rowIndex]) {
        console.log(`Remove row ${rowIndex} refresh Fn`);
        delete this.longTimeOperations[rowIndex];
      }
    }
  }

  /**
   * Updates a row specified by `rowIndex` with values from `cells`.
   * @param {!Object<string, string>} cells
   * @param {number} rowIndex
   * @private
   */
  updateCells_(cells, rowIndex) {//columnName, rowIndex, value
    Object.keys(cells)
      .filter((columnName) => typeof cells[columnName] !== 'undefined')
      .forEach((columnName) => {
        this.enhancedSheet.save(
          [[cells[columnName]]], rowIndex, MOJO_FIELDS.indexOf(columnName) + 1);
      });
  }

  /**
   * Sets a link for a cell.
   * @param {string} columnName
   * @param {number} rowIndex
   * @param {string} link
   * @private
   */
  setLink_(columnName, rowIndex, link) {
    const range = this.enhancedSheet.sheet.getRange(
      rowIndex, MOJO_FIELDS.indexOf(columnName) + 1, 1, 1);
    const richTextValue = SpreadsheetApp.newRichTextValue()
      .setText(range.getDisplayValue())
      .setLinkUrl(link)
      .build();
    range.setRichTextValue(richTextValue);
  }

  /**
   * Clears `status` cell after row number `startRow` (included). This is used
   * to clear `status` and `note` after an error happened.
   * @param {number} startRow
   * @private
   */
  clearStatus_(startRow) {
    const { sheet } = this.enhancedSheet;
    const rowNumber = sheet.getLastRow() - startRow + 1;
    if (rowNumber === 0) {
      console.log('Clearing resource status after the last row, quit.');
      return;
    }
    console.log('Clearing resource status after row:', startRow);
    ['status', 'note']
      .map((column) => MOJO_FIELDS.indexOf(column) + 1)
      .forEach((startColumn) => {
        sheet.getRange(startRow, startColumn, rowNumber, 1).clearContent();
      });
  }
}
