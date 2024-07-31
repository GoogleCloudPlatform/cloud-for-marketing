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

/** @fileoverview A type of data to be shown and operated in a sheet. */
/**
 * This is the base class of a Google sheet which will be used as an external
 * table in BigQuery.
 * The sheet doesn't necessarily be the same Cyborg sheet. If the target
 * spreadsheet didn't contain the sheet, the enable function will initialize it.
 *
 * @abstract
 */
class DataTableSheet {

  /** Name of the sheet. */
  get sheetName() {
    throw new Error('Undefined sheetName');
  }
  /**
   * Configurations of columns, including name, width and format. This proprety
   * is used to generate 'columnName', 'columnWidth', 'columnFormat', etc. which
   * will be used by EnhancedSheet to create the sheet.
   */
  get columnConfiguration() {
    throw new Error('Undefined columnConfiguration');
  }

  /** Names of fields based on column names. */
  get fields() {
    return this.columnName.map(camelize);
  }
  get columnName() {
    return this.columnConfiguration
      .filter(({ name }) => name !== COLUMN_NAME_FOR_DEFAULT_CONFIG)
      .map(({ name }) => name);
  }
  get columnWidth() {
    const widths = {};
    this.columnConfiguration.filter(({ width }) => width)
      .forEach(({ name, width }) => void (widths[name] = width));
    return widths;
  }
  get columnFormat() {
    const formats = {};
    this.columnConfiguration.filter(({ format }) => format)
      .forEach(({ name, format }) => void (formats[name] = format));
    return formats;
  }
  get columnDataRange() {
    const dataRanges = {};
    this.columnConfiguration.filter(({ dataRange }) => dataRange)
      .forEach(({ name, dataRange }) => void (dataRanges[name] = dataRange));
    return dataRanges;
  }

  /** Headline style. */
  get headlineStyle() {
    return {
      backgroundColor: '#9AA0A6',
      fontColor: 'white',
    };
  }

  /** Property name of target dataset. */
  get datasetProprtyName() {
    return 'dataset';
  }
  /** Name of the external table. */
  get tableName() {
    return snakeize(this.sheetName);
  }

  /** Initial data for the sheet. */
  get initialData() { }

  /**
   * Returns the EnhancedSheet instance for current sheet.
   * @param {string|undefined} url
   * @return {!EnhancedSheet}
   */
  getEnhancedSheet(url) {
    const spreadsheet = url ? SpreadsheetApp.openByUrl(url) : undefined;
    return new EnhancedSheet(this.sheetName, spreadsheet);
  }

  /**
   * Initializes the sheet.
   * @param {string|undefined} url
   */
  initialize(url) {
    this.getEnhancedSheet(url).initialize(this);
  }
}
