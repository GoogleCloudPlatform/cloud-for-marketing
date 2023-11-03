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

/** @fileoverview A type of data to be shown and operated in a sheet. */

/**
 * This is the base class of 'Data' sheet (not the Mojo Sheet). A kind of data,
 * e.g. Sentinel task config, can be loaded to this sheet, be uploaded to
 * Firestore or tested.
 *
 * @abstract
 */
class PlainSheet {

  /**
   * During initialization, the constructor will prepare itself (the instance)
   * as the object that holds functions for menu items.
   * @constructor
   */
  constructor() {
    // Default headline style
    this.headlineStyle = {
      backgroundColor: '#669DF6',
      fontColor: 'white',
    };
    // Set up the holder for menu functions
    const instanceName = camelize(this.constructor.name);
    MENU_OBJECTS[instanceName] = this;
    this.menuFunctionHolder = `MENU_OBJECTS.${instanceName}`;
    // The column to host operation result (note)
    this.defaultNoteColumn = '';
  }

  /**
   * Operates on an array of rows. The detailed operation will be defined in the
   * sub classes.
   * @abstract
   * @param {!Array<object>} rowEntities
   * @return {!Array<object>}
   */
  processResources(rowEntities) {
    throw new Error('Should be overwrittern in sub class.');
  }

  /**
   * Initializes the data Sheet. This is a menu function.
   */
  initialize() {
    this.getEnhancedSheet().initialize(this);
  }

  /**
   * Operates on the selected row. This is a menu funciton.
   * The operation to a row is defined in the function `processResources` which
   * should be overrided by the sub classes.
   */
  operateSingleRow() {
    const rowIndex = this.getSelectedRow();
    const rowEntity = this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    this.processResources([rowEntity]).forEach(
      (result) => void this.showOperationResult(rowIndex, result)
    );
  }

  /**
   * Operates on all rows in this sheet. This is a menu funciton.
   * The operation to a row is defined in the function `processResources` which
   * should be overrided by the sub classes.
   */
  operateAllRows() {
    this.processResources(this.getArrayOfRowEntity()).forEach(
      (result, index) => {
        this.showOperationResult(index + ROW_INDEX_SHIFT, result);
      });
  }

  /**
   * Shows the result of operations. By default it will change the
   * `defaultNoteColumn` font color to green and add the result as a note for
   * the cell.
   * @param {number} rowIndex
   * @param {!string} note
   */
  showOperationResult(rowIndex, note,
    fontColor = 'green', column = this.defaultNoteColumn) {
    if (!column) {
      throw new Error('There is no column defined to host the result.');
    }
    const columnIndex = this.columnName.indexOf(column) + 1;
    this.getEnhancedSheet().sheet.getRange(rowIndex, columnIndex, 1, 1)
      .setFontColor(fontColor).setNote(note);
  }

  /**
   * Returns the EnhancedSheet instance for current sheet.
   * @return {!EnhancedSheet}
   */
  getEnhancedSheet() {
    return new EnhancedSheet(this.sheetName);
  }

  /**
   * Returns an array of elements are built from sheet rows. Every row will be
   * loaded as an object with properties are the `fields`.
   * @return {!Array<!Object>}
   */
  getArrayOfRowEntity() {
    return this.getEnhancedSheet().loadArrayOfEntity({ fields: this.fields });
  }

  /**
   * Checks if a valid cell (not the headline nor empty row) of the current
   * sheet is selected. If it is valid, return the number of row that the cell
   * belongs to.
   * @return {number}
   */
  getSelectedRow() {
    const cell = SpreadsheetApp.getSelection().getCurrentCell();
    const sheetName = cell.getSheet().getSheetName();
    const row = cell.getRow();
    if (sheetName !== this.sheetName
      || row < ROW_INDEX_SHIFT || row > cell.getSheet().getLastRow()) {
      throw new Error(
        `Select a valid row in [${this.sheetName}] and retry.`
      );
    }
    return row;
  }

  /**
   * Sets a note for the specified cell.
   * @param {number} row Row number of the cell.
   * @param {number} column Column number of the cell.
   * @param {string} note
   */
  updateNote(row, column, note) {
    const enhancedSheet = this.getEnhancedSheet();
    enhancedSheet.sheet.getRange(row, column, 1, 1).setNote(note);
  }

}
