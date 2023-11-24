// Copyright 2023 Google Inc.
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

/**
 * @fileoverview An enhanced sheet class contain utilities functions for a
 * GoogleSpreadsheet (@link {SpreadsheetApp}).
 */

/**
 * The options when load data into entity from the sheet.
 * @typedef {{
 *   fields: !Array<string>,
 *   idProperty: string|undefined,
 *   skipHeadLines: number|undefined,
 * }}
 */
let SheetLoadOptions;

/**
 * The options to initialize the sheet for data and/or styles.
 *   `columnsToBeMerged` is the columns will be merged vertically.
 *   `columnesToBeMergedMap`, the columns will be merged vertically by the other
 *     given column. By default, a column will be merged vertically if the
 *     values are the same. This object defines other columns will be merged
 *     veritically when the given column has the same value.
 * @typedef {{
 *   columnName: !Array<string>,
 *   columnFormat: !Object<string, (!RangeStyle|!Array<!RangeStyle>)>|undefined,
 *   columnWidth: !Object<string, number>|undefined,
 *   columnDataRange: !Object<string, !Array<string>>|undefined,
 *   columnConditionalFormat: !Object<string, string>|undefined,
 *   headlineStyle: !Object<string, string>|undefined,
 *   initialData: Array<Array<string|number>>,
 *   columnsToBeMerged: !Array<string>|undefined,
 *   columnsToBeMergedMap: !Object<string, !Array<string>>|undefined,
 * }}
 */
let SheetConfig;

/**
 * Style definition for spreadsheet ranges.
 * For example, the 'fn' can be 'setNumberFormat', while the 'format' can be
 * '#0.00%'.
 * @see https://developers.google.com/apps-script/reference/spreadsheet/range
 * @typedef {{
*   fn: string,
*   format: string,
* }}
*/
let RangeStyle;

/** Style for column. */
const COLUMN_STYLES = {
  ALIGN_MIDDLE_AND_CENTER: [
    { fn: 'setHorizontalAlignment', format: 'center' },
    { fn: 'setVerticalAlignment', format: 'middle' },
  ],
  ALIGN_MIDDLE: { fn: 'setVerticalAlignment', format: 'middle' },
}

/** Default headline style. */
const DEFAULT_HEADLINE_STYLE = {
  rowHeight: 28,
  fontSize: 11,
  fontFamily: 'Google Sans',
  horizontalAlignment: 'center',
  verticalAlignment: 'middle',
};

/**
 * Default column format.
 * @type {{string: !RangeStyle}}
 */
const COLUMN_FORMAT = {
  DATE: { fn: 'setNumberFormat', format: 'YYYY-MM-DD' },
  TIMESTAMP: { fn: 'setNumberFormat', format: 'YYYY-MM-DD HH:mm:SS' },
  CURRENCY: { fn: 'setNumberFormat', format: '$#,###.00' },
  PERCENTAGE: { fn: 'setNumberFormat', format: '#0.00%' },
};

/**
 * A sheet class with enhanced functions, including:
 * 1. Clear the sheet data or data and charts;
 * 2. Create and/or initialize the sheet with given contents and styles.
 * 3. Save or append data/entities into the sheet;
 * 4. Load content from this sheet as entities;
 * 5. Merge contents from other sheets to this one;
 * 6. Merge empty cells vertically.
 */
class EnhancedSheet {

  /**
   * Create a
   * @constructor
   * @param {string} sheetName
   * @param {Spreadsheet} spreadsheet
   * @see https://developers.google.com/apps-script/reference/spreadsheet/spreadsheet
   */
  constructor(sheetName, spreadsheet = SpreadsheetApp.getActiveSpreadsheet()) {
    this.sheetName = sheetName;
    this.spreadsheet = spreadsheet;
    this.sheet = this.spreadsheet.getSheetByName(this.sheetName);
    if (!this.sheet) {
      this.sheet = this.spreadsheet.insertSheet(this.sheetName);
    }
  }

  /** Clears the whole sheet, including charts. */
  clear() {
    this.sheet.clear();
    this.sheet.getCharts().forEach(
      (chart) => void this.sheet.removeChart(chart));
  }

  /**
   * Clears the content with given starting position.
   * @param {number=} startRow, starting row 1-based index. Default 2.
   * @param {number=} startColumn, starting column 1-based index. Default 1.
   */
  clearData(startRow = 2, startColumn = 1) {
    if (this.sheet.getLastRow() < startRow) return;
    this.sheet.getRange(
      startRow,
      startColumn,
      this.sheet.getLastRow() - startRow + 1,
      this.sheet.getLastColumn() - startColumn + 1,
    ).clearContent();
  }

  /**
   * Saves the data to the given position.
   * @param {Array<Array<string|number>>} data
   * @param {number=} startRow, starting row 1-based index. Default 1.
   * @param {number=} startColumn, starting column 1-based index. Default 1.
   */
  save(data, startRow = 1, startColumn = 1) {
    if (data.length > 0)
      this.sheet.getRange(startRow, startColumn, data.length, data[0].length)
        .setValues(data);
  }

  /**
   * Appends the data to the current sheet.
   * @param {Array<Array<string|number>>} data
   */
  append(data) {
    const startRow = this.sheet.getLastRow() + 1;
    return this.save(data, startRow, 1);
  }

  /**
   * Cleans the data and loads an array of entities to this sheet.
   * @param {Array<object>} entities
   * @param {Array<string>} fields Names of fields which are mapped to the
   *   headline.
   */
  saveEntities(entities, fields) {
    this.clearData();
    const data = entities.map((entity) => {
      return fields.map((field) => {
        const value = entity[field];
        if (typeof value !== 'object')
          return value;
        return JSON.stringify(value);//stringify(value, null, 2);
      });
    });
    this.save(data, 2);
  }

  /**
   * Initialize the sheet:
   *  1. clear the sheet;
   *  2. fill the headline based on `columnName`;
   *  3. set the format for the sheet;
   *  4. append the data if there are.
   * For a Mojo sheet, the headline has the fixed columns but different styles.
   * That's the reason need two parameters.
   * @param {!SheetConfig} sheetConfig
   * @param {!Object<string, string>} headlineStyle
   */
  initialize(sheetConfig, headlineStyle) {
    this.clear();
    const { columnName: columns, initialData = [] } = sheetConfig;
    if (columns) {
      this.sheet.deleteColumns(1, columns.length);
      this.sheet.insertColumns(1, columns.length);
      this.save([columns]);
    }
    this.setSheetStyle(sheetConfig, headlineStyle);
    this.initMergeSettings(sheetConfig);
    const appendedData =
      typeof initialData === 'function' ? initialData() : initialData;
    if (appendedData.length > 0) {
      this.append(appendedData);
    }
  }

  /**
   * Sets the sheet style if related configurations were present, including:
   *  1. set columns' format, alignment, font, color, etc.;
   *  2. set columns' data range (drop list);
   *  3. set columns' width;
   *  4. set columns' conditional format to indicate the validation of a row;
   *  5. set the syle of headline.
   * @param {!SheetConfig} sheetConfig
   * @param {!Object<string, string>} headlineStyle
   */
  setSheetStyle(sheetConfig, headlineStyle) {
    const {
      columnName: columns,
      columnFormat,
      columnWidth,
      columnDataRange,
      columnConditionalFormat,
    } = sheetConfig;
    if (columnFormat)
      this.setColumnFormat_(columns, columnFormat);
    if (columnDataRange)
      this.setColumnDataRange_(columns, columnDataRange);
    if (columnWidth)
      this.setColumnWidth_(columns, columnWidth);
    if (columnConditionalFormat)
      this.setConditionalFormat_(columns, columnConditionalFormat);
    const style = Object.assign({}, DEFAULT_HEADLINE_STYLE,
      headlineStyle || sheetConfig.headlineStyle);
    this.setHeadlineStyle_(columns, style);
  }

  /**
   * Maps column-name-based definition of how to merge cells vertically into
   * number-based (index of the columns) array or object to simplify the
   * merge operation later.
   * @param {!SheetConfig} sheetConfig
   */
  initMergeSettings(sheetConfig) {
    const {
      columnName,
      columnsToBeMerged = [],
      columnsToBeMergedMap = {}
    } = sheetConfig;
    this.mergedColumnIndexes = columnsToBeMerged.map(
      (column) => columnName.indexOf(column)
    );
    this.mergedColumnIndexMap = {};
    Object.keys(columnsToBeMergedMap).forEach((key) => {
      this.mergedColumnIndexMap[columnName.indexOf(key)] =
        columnsToBeMergedMap[key].map((value) => columnName.indexOf(value));
    })
  }

  /**
   * Sets the format of columns. Because there is a headline as column names,
   * all the formats set here start from the second row.
   * @param {!Array<string>} columns Column names.
   * @param {!Object<string, (!RangeStyle|!Array<!RangeStyle>)>} columnFormat
   *   A map contains formats for columns. The `key` is the column names and the
   *  `value` is `RangeStyle` or an array of `RangeStyle`.
   *   A specific key `default_` stands for the format of the whole content.
   * @private
   */
  setColumnFormat_(columns, columnFormat = {}) {
    if (columnFormat.default_) {
      const exceptHeadline = this.sheet.getRange('2:' + this.sheet.getMaxRows());
      this.setRangeFormat_(exceptHeadline, columnFormat.default_);
    }
    columns.forEach((column, index) => {
      const format = columnFormat[column];
      if (!format) return;
      const columnLetter = getColumnLetter(index);
      const range = this.sheet.getRange(columnLetter + '2:' + columnLetter);
      this.setRangeFormat_(range, format);
    });
  }

  /**
   * Sets the DataValidation of columns. DataValidation is built from an array
   * of string. Because there is a headline as column names, the DataValidation
   * starts from the second row.
   * @see https://developers.google.com/apps-script/reference/spreadsheet/data-validation
   * @param {!Array<string>} columns Column names.
   * @param {!Object<string, !Array<string>>} columnDataRang
   * @private
   */
  setColumnDataRange_(columns, columnDataRange = {}) {
    columns.forEach((field, index) => {
      const dataRange = columnDataRange[field];
      if (!dataRange) return;
      const columnLetter = getColumnLetter(index);
      const range = this.sheet.getRange(columnLetter + '2:' + columnLetter);
      setSelectDroplist(range, dataRange);
    });
  }

  /**
   * Set width for columns.
   * @param {!Array<string>} columns Column names.
   * @param {!Object<string, number>} columnWidth The default value is 100.
   * @private
   */
  setColumnWidth_(columns, columnWidth = {}) {
    const defaultWidth = columnWidth.default_ || 100;
    columns.forEach((field, index) => {
      const width = columnWidth[field] || defaultWidth;
      this.sheet.setColumnWidth(index + 1, width);
    });
  }

  /**
   * Set format for the given range.
   * @param {Range} range
   *   @see https://developers.google.com/apps-script/reference/spreadsheet/range
   * @param {Array<!RangeStyle>|!RangeStyle} format
   * @private
   */
  setRangeFormat_(range, format) {
    const formats = Array.isArray(format) ? format : [format];
    formats.forEach(({ fn, format }) => range[fn](format));
  }

  /**
   * Sets the conditional formats for the sheet. This is only used for some
   * templated Sheets and might need further optimization.
   * @param {!Array<string>} columns Column names.
   * @param {!Object<string, string>} conditionalFormats A property named
   *   `requiredColumn_` acts at the indicator of a row exits. If this cell is
   *   empty, then 'required' cells will not be checked for this row.
   */
  setConditionalFormat_(columns, conditionalFormats) {
    let requiredColumnLetter;
    const requiredIndex = columns.indexOf(conditionalFormats.requiredColumn_);
    if (requiredIndex > -1) {
      requiredColumnLetter = getColumnLetter(requiredIndex);
    } else {
      console.warn('No required column, so REQUIRED is not supported.');
    }
    const rules = [];
    columns.forEach((field, index) => {
      if (!conditionalFormats[field]) return;
      const [requried, type] = conditionalFormats[field].split('_');
      const columnLetter = getColumnLetter(index);
      const range = this.sheet.getRange(`${columnLetter}2:${columnLetter}`);
      const conditions = [];
      if (requried === 'REQUIRED' && requiredColumnLetter) {
        conditions.push(
          `AND(NOT(ISBLANK($${requiredColumnLetter}2)),ISBLANK($${columnLetter}2))`
        )
      }
      if (type) {
        conditions.push(
          `AND(NOT(ISBLANK($${columnLetter}2)),NOT(IS${type}($${columnLetter}2)))`
        )
      }
      const fomular = conditions.length > 1
        ? `=OR(${conditions.join(',')})`
        : `=${conditions[0]}`;
      const rule = SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied(fomular)
        .setBackground('#F5AEA9')
        .setRanges([range])
        .build();
      rules.push(rule);
    });
    this.sheet.setConditionalFormatRules(rules);
  }

  /**
   * Sets the style of the headline.
   * Note: there is no `setBackgroundColor` methods in `Range`, however there is
   * the function in the `Range` object. Will use the property as
   * `backgroundColor` not `background` for clarity.
   * @see https://developers.google.com/apps-script/reference/spreadsheet/range
   * @param {!Array<string>} columns Sheet column names.
   * @param {!Object<string:string>} style
   */
  setHeadlineStyle_(columns, styles = DEFAULT_HEADLINE_STYLE) {
    if (columns) {
      const headline = this.sheet.getRange(1, 1, 1, columns.length);
      Object.keys(styles).forEach((style) => {
        const value = styles[style];
        const fn = `set${style.replace(/^([a-z])/, (m, c) => c.toUpperCase())}`;
        if (style.startsWith('row')) {
          this.sheet[fn](1, value);
        } else {
          headline[fn](value);
        }
      });
    }
    if (styles.backgroundColor)
      this.sheet.setTabColor(styles.backgroundColor);
  }

  /**
   * Loads data from the sheet as an array of entity (JSON object) which have
   * the given fields as properties.
   * @param {!SheetLoadOptions} options
   * @return {!Array<Object>}
   */
  loadArrayOfEntity(options) {
    const { fields, skipHeadLines = 1 } = options;
    if (this.sheet.getLastRow() <= skipHeadLines) {
      return [];
    }
    const rows = this.sheet.getRange(
      skipHeadLines + 1, 1, this.sheet.getLastRow() - skipHeadLines, fields.length
    ).getDisplayValues();
    return rows.map((row) => {
      const entity = {};
      fields.forEach((key, index) => void (entity[key] = row[index]));
      return entity;
    });
  }

  /**
   * Loads data fromt the sheet as a map with the value as the entities.
   * For the key in map, there must be a field (idProperty) as "id".
   * @param {!SheetLoadOptions} options
   * @return {Object<string, Object>} the map contains all objects in the sheet.
   */
  loadMapOfEntity(options) {
    const { idProperty = 'id' } = options;
    const entities = {};
    this.loadArrayOfEntity(options).forEach((entity) => {
      const id = entity[idProperty];
      if (!id) {
        console.error(`There is no 'id' detected in the fields. Quit`);
        return;
      } else {
        if (entities[id]) {
          console.log(
            `Warning: Duplicated id(${id}) detected in the fields.` +
            ' The previous one will be overwritten.');
        }
        entities[id] = entity;
      }
    });
    return entities;
  }

  /**
   * Merge the empty cells up to the non-empty one vertically.

   * @param {number=} index Index of columns to be merged in the array
   *   `this.mergedColumnIndexes`.
   * @param {number=} startRowIndex Index of the begin row number. Because there
   *   is a headline headline to be skipped, the default value is 1.
   * @param {number=} endRowIndex Index of end row of the merge range. The rows
   *   are grouped by order. The rows belong to a later column will not be
   *   merged even if they belong to a different previous group. This parameter
   *   limits how far a merge operation will go.
   */
  mergeVertically(index = 0, startRowIndex = 1,
    endRowIndex = this.sheet.getLastRow() - 1) {
    if (index >= this.mergedColumnIndexes.length) return;
    const cellValues = this.sheet
      .getRange(1, this.mergedColumnIndexes[index] + 1, endRowIndex + 1, 1)
      .getDisplayValues();
    let startRow = startRowIndex;
    let previousValue = cellValues[startRow][0];
    for (let i = startRow + 1; i < cellValues.length; i++) {
      const value = cellValues[i][0];
      if (value !== previousValue) {
        if (startRow + 1 < i) { // Merge when there are multiple lines.
          this.mergeVertical_(index, startRow, i - 1);
        }
        previousValue = value;
        startRow = i;
      }
    }
    if (startRow + 1 < cellValues.length) {
      this.mergeVertical_(index, startRow, cellValues.length - 1);
    }
  }

  /**
   * Merges cells at column based on the given `index` from row `startRowIndex`
   * to `endRowIndex`. All the indexes are 0-based.
   * @param {number} index Index of columns to be merged in the array
   *   `this.mergedColumnIndexes`.
   * @param {number} startRowIndex Index of start row of the merge range.
   * @param {number} endRowIndex Index of end row of the merge range.
   * @private
   */
  mergeVertical_(index, startRowIndex, endRowIndex) {
    const mainColumnIndex = this.mergedColumnIndexes[index];
    const columnsToBeMerged = [mainColumnIndex];
    if (this.mergedColumnIndexMap[mainColumnIndex]) {
      columnsToBeMerged.push(...this.mergedColumnIndexMap[mainColumnIndex]);
    }
    columnsToBeMerged.forEach((columnIndex) => {
      this.sheet.getRange(
        startRowIndex + 1, columnIndex + 1, endRowIndex - startRowIndex + 1, 1)
        .merge();
    });
    this.mergeVertically(index + 1, startRowIndex, endRowIndex);
  }

  /**
   * Merge other sheets to this one. It appends the contents to this one.
   * @param {!Array<!Sheet>} sheets Array of the sheets to be merged to here.
   * @param {number=} skipHeadLines Numbers of headline to be skipped. Default
   *   value is 1.
   */
  mergeSheets(sheets, skipHeadLines = 1) {
    const startRow = skipHeadLines + 1;
    sheets.forEach((sheet) => {
      const rows = sheet.getLastRow() - skipHeadLines;
      if (rows > 0) {
        const source = sheet.getRange(startRow, 1, rows, sheet.getLastColumn());
        const target = this.sheet.getRange(
          this.sheet.getLastRow() + 1, 1, rows, sheet.getLastColumn());
        this.sheet.insertRowsAfter(this.sheet.getLastRow(), rows);
        source.copyTo(target);
      }
      SpreadsheetApp.flush();
    });
  }
};
