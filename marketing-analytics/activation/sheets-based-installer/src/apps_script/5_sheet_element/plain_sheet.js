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
 * This is enhanced 'DataTableSheet' with a menu. A kind of data,
 * e.g. Sentinel task config, can be loaded to this sheet, be uploaded to
 * Firestore or tested.
 *
 * @abstract
 */
class PlainSheet extends DataTableSheet {
  /** @return {!Object<string, !PlainSheet>} */
  static get instances() {
    if (!this._instances) {
      this._instances = {};
    }
    return this._instances;
  }

  /**
   * During initialization, the constructor will prepare itself (the instance)
   * as the object that holds functions for menu items.
   * @param {?Object=} options Configuration to initialize the sheet, can have
   *   optional properties: sheetName,
   */
  constructor(options = {}) {
    super();
    this.options = options;
    const {menuItems = []} = this.options;
    /**
     * Prepares customized menu items. Some Plane Sheets have customized menu
     * items whose functions are not in this class or subclass. For example,
     * some updates to (Sentinel) task configurations for a specific solution.
     * To support this use case, the passed in menu functions can be bound to
     * 'this' object and so it can gain the access of the target plane sheet
     * object. After the function is bound, the menuItems can be handled in the
     * same of `inherentMenuItems`.
     * @see menuItem
     *
     */
    this.customizedMenuItems = menuItems.map(({separator, name, method}) => {
      if (separator) return {separator: true};
      this[method.name] = method.bind(this);
      return {name, method: method.name};
    });

    /**
     * `onOpen` will render the customized menu `Cyborg` using the instances
     * registered here. `onEdit` will route to this sheet's `processEditEvent`
     * instance method to format a cell if it is configured as a `JSON` cell
     * (via `this.jsonColumn`). Any creation of a new instance here will
     * OVERWRITE the menu object. So we should try to avoid creating different
     * instances for the same sheet, use the objects in `PlainSheet.instances`
     * instead.
     */
    // Set up the holder for menu functions
    PlainSheet.instances[this.menuObjectName] = this;
  }

  /** Default name of the sheet. */
  get defaultSheetName() {
    throw new Error('Undefined defaultSheetName');
  }
  /**
   * Name of the sheet. Will be the default value if not specified by options.
   * @override
   * @return {string}
   */
  get sheetName() {
    return this.options.sheetName || this.defaultSheetName;
  }

  /**
   * Default headline style.
   * @return {!Object<string, string>}
   */
  get defaultHeadlineStyle() {
    return {
      backgroundColor: '#669DF6',
      fontColor: 'white',
    };
  }
  /**
   * Headline style.
   * @override
   * @return {!Object<string, string>}
   */
  get headlineStyle() {
    return this.options.headlineStyle || this.defaultHeadlineStyle;
  }

  /**
   * Menu items of the sheet. All functions should be in the instance.
   * @return {!Array<!Object>}
   */
  get inherentMenuItems() {
    throw new Error('Undefined inherent MenuItems');
  }
  /**
   * @return {!Array<!Object>}
   */
  get menuItem() {
    return this.inherentMenuItems
      .concat(this.customizedMenuItems)
      .map(({separator, name, method}) => {
        if (separator) return {separator: true};
        return {name, method: `${this.menuFunctionHolder}.${method}`};
      });
  }
  /**
   * @return {string}
   */
  get menuObjectName() {
    return getSafePropertyName(this.sheetName);
  }
  /**
   * @return {string}
   */
  get menuFunctionHolder() {
    return `PlainSheet.instances.${this.menuObjectName}`;
  }

  /**
   * The column to host operation result (note)
   * @return {string|undefined}
   */
  get defaultNoteColumn() {
    return this.columnConfiguration
      .filter(({defaultNote}) => defaultNote)
      .map(({name}) => name)[0];
  }
  /**
   * Columns contain a JSON string to `JSON_COLUMNS` for auto-checking.
   * @return {!Array<string>}
   */
  get jsonColumn() {
    return this.columnConfiguration
      .filter(({jsonColumn}) => jsonColumn)
      .map(({name}) => name);
  }

  /**
   * Filter function to filter out results.
   * @return {function(!Object): boolean}
   */
  get entityFilter() {
    return this.options.entityFilter || (() => true);
  }

  /**
   * Operates on an array of rows. The detailed operation will be defined in the
   * sub classes.
   * @param {!Array<!Object>} rowEntities
   * @param {number} startRowIndex Index number of the starting selected row.
   * @return {!Array<!Object>}
   */
  processResources(rowEntities, startRowIndex) {
    throw new Error('Should be overwritten in sub class.');
  }

  /**
   * Initializes the data Sheet. This is a menu function.
   */
  initialize() {
    this.getEnhancedSheet().initialize(this);
  }

  /**
   * Refreshes the data range of columns. This is a menu function.
   */
  refreshDataRange() {
    this.getEnhancedSheet().setColumnDataRange_(
      this.columnName,
      this.columnDataRange,
    );
  }

  /**
   * Operates on the selected row. This is a menu function.
   * The operation to a row is defined in the function `processResources` which
   * should be overridden by the sub classes.
   */
  operateSingleRow() {
    const rowIndex = this.getSelectedRow();
    const rowEntity = this.getArrayOfRowEntity()[rowIndex - ROW_INDEX_SHIFT];
    this.processResources([rowEntity], rowIndex).forEach(
      result => void this.showOperationResult(rowIndex, result),
    );
  }

  /**
   * Operates on all rows in this sheet. This is a menu function.
   * The operation to a row is defined in the function `processResources` which
   * should be overridden by the sub classes.
   */
  operateAllRows() {
    this.processResources(this.getArrayOfRowEntity(), ROW_INDEX_SHIFT).forEach(
      (result, index) => {
        this.showOperationResult(index + ROW_INDEX_SHIFT, result);
      },
    );
  }

  /**
   * Shows the result of operations. By default it will change the
   * `defaultNoteColumn` font color to green and add the result as a note for
   * the cell.
   * @param {number} rowIndex
   * @param {string} note
   * @param {string=} fontColor
   * @param {string=} column
   */
  showOperationResult(
    rowIndex,
    note,
    fontColor = 'green',
    column = this.defaultNoteColumn,
  ) {
    if (!column) {
      throw new Error('There is no column defined to host the result.');
    }
    const columnIndex = this.columnName.indexOf(column) + 1;
    this.getEnhancedSheet()
      .sheet.getRange(rowIndex, columnIndex, 1, 1)
      .setFontColor(fontColor)
      .setNote(note);
  }

  /**
   * Returns the EnhancedSheet instance for current sheet.
   * @return {!EnhancedSheet}
   */
  getEnhancedSheet() {
    const enhancedSheet = new EnhancedSheet(this.sheetName);
    if (enhancedSheet.sheet.getLastRow() === 0) {
      enhancedSheet.initialize(this);
    }
    return enhancedSheet;
  }

  /**
   * Returns an array of elements are built from sheet rows. Every row will be
   * loaded as an object with properties are the `fields`.
   * @return {!Array<!Object>}
   */
  getArrayOfRowEntity() {
    return this.getEnhancedSheet().loadArrayOfEntity({fields: this.fields});
  }

  /**
   * Returns a map of objects that are built from sheet rows. Every row will be
   * loaded as an object with properties are the `fields`, the specified column
   * (value of `idProperty`) will be used as the key of the object in the map.
   * @param {string=} idProperty
   * @return {!Object<string, !Object>}
   */
  getMapOfRowEntity(idProperty = this.idColumn) {
    return this.getEnhancedSheet().loadMapOfEntity({
      idProperty,
      fields: this.fields,
    });
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
    if (
      sheetName !== this.sheetName ||
      row < ROW_INDEX_SHIFT ||
      row > cell.getSheet().getLastRow()
    ) {
      throw new Error(`Select a valid row in [${this.sheetName}] and retry.`);
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

  /**
   * Sets a link for the specified cell.
   * @param {number} row Row number of the cell.
   * @param {number} column Column number of the cell.
   * @param {string} link
   */
  updateLink(row, column, link) {
    const enhancedSheet = this.getEnhancedSheet();
    const range = enhancedSheet.sheet.getRange(row, column, 1, 1);
    const richTextValue = SpreadsheetApp.newRichTextValue()
      .setText(range.getDisplayValue())
      .setLinkUrl(link)
      .build();
    range.setRichTextValue(richTextValue);
  }

  /**
   * Updates a row specified by `rowIndex` with an object of key value pairs.
   * @param {!Object<string, string>} cells
   * @param {number} rowIndex
   * @param {string=} fontColor
   */
  updateCells(cells, rowIndex, fontColor = 'black') {
    // columnName, rowIndex, value
    Object.keys(cells)
      .filter(columnName => typeof cells[columnName] !== 'undefined')
      .forEach(columnName => {
        const columnIndex = this.fields.indexOf(columnName) + 1;
        this.getEnhancedSheet()
          .sheet.getRange(rowIndex, columnIndex, 1, 1)
          .setValue(cells[columnName])
          .setFontColor(fontColor);
      });
  }

  /** Content for the downloaded file. */
  get downloadContent() {
    throw new Error('Should be overwritten in sub class.');
  }

  /** Default file name for the downloaded file. */
  get downloadFilename() {
    throw new Error('Should be overwritten in sub class.');
  }

  /**
   * MIME media type for the downloaded file.
   * @return {string}
   */
  get downloadType() {
    return 'application/json';
  }

  /**
   * Handle the edit event for the sheet.
   * Format JSON columns if they are edited. Supports editing multiple cells at
   * once (e.g. by paste).
   * @param {!Object} event Event object from Google Apps Script.
   */
  processEditEvent(event) {
    const range = event.range;
    if (range.getRow() <= 1) return; // Ignore edits to the title row

    const numCols = range.getNumColumns();
    const numRows = range.getNumRows();
    const startRow = range.getRow();
    const startCol = range.getColumn();
    const activeSheet = range.getSheet();

    // Loop over the columns that are being edited
    for (let c = 0; c < numCols; c++) {
      const currentEditingColIndex = startCol + c;
      const colName = this.columnName[currentEditingColIndex - 1];

      // If this column is configured as a JSON column, format it
      if (colName && this.jsonColumn.includes(colName)) {
        for (let r = 0; r < numRows; r++) {
          const cell = activeSheet.getRange(
            startRow + r,
            currentEditingColIndex,
            1,
            1,
          );
          if (typeof formatSheetRowForJsonCell === 'function') {
            formatSheetRowForJsonCell(cell);
          }
        }
      }
    }
  }

  /**
   * Exports the content sheet as a downloaded file. The sub class need to
   * implement `downloadContent` and `downloadFilename` for the content and
   * default file name.
   */
  export() {
    const content = this.downloadContent;
    const filename = this.downloadFilename;
    const type = this.downloadType;
    const htmlTemplate = `<html>
  <head>
    <script>
      window.close = function(){
        window.setTimeout(google.script.host.close,100);
      }
      function download() {
        const element = document.createElement('a');
        // 'encodeURIComponent' does not escape the ' character
        element.setAttribute('href',
          "data:${type};charset=utf-8,${encodeURIComponent(content)}");
        element.setAttribute('download', '${filename}');
        element.style.display = 'none';
        document.body.appendChild(element);
        element.click();
        document.body.removeChild(element);
        close();
      }
    </script>
  </head>
  <body onload="download()">
  </body>
</html>
`;
    const html = HtmlService.createHtmlOutput(htmlTemplate)
      .setWidth(250)
      .setHeight(100);
    SpreadsheetApp.getUi().showModalDialog(html, 'Start downloading ...');
  }
}
