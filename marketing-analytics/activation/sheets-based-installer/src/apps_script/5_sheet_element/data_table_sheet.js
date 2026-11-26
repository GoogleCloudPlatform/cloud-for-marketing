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
   * Configurations of columns, including name, width and format. This property
   * is used to generate 'columnName', 'columnWidth', 'columnFormat', etc. which
   * will be used by EnhancedSheet to create the sheet.
   */
  get columnConfiguration() {
    throw new Error('Undefined columnConfiguration');
  }

  // TODO: Optimize the way to generate fields. getSafePropertyName
  //  The EnhancedSheet uses field names here as the default property names for
  //  the data loaded, when load the sheet as external table in BigQuery, the
  //  field names do not align with the SQL naming convention. So should add a
  //  new function to distinguish these two cases.
  //  Now in PlainSheet, when it loads the data, it would use the fields defined
  //  here explicitly, so it would work fine.
  //  If directly load a sheet through EnhancedSheet, the auto guessed fields
  //  process is required to generated the proper fields.
  /** Names of fields based on column names. */
  get fields() {
    const defaultColumn = this.columnConfiguration.filter(
      ({name}) => name === COLUMN_NAME_FOR_DEFAULT_CONFIG,
    )[0];
    const fieldFormat = defaultColumn
      ? defaultColumn.fieldFormat || camelize
      : camelize;
    return this.columnConfiguration
      .filter(({name}) => name !== COLUMN_NAME_FOR_DEFAULT_CONFIG)
      .map(({name, field}) => field || fieldFormat(name));
  }
  get columnName() {
    return this.columnConfiguration
      .filter(({name}) => name !== COLUMN_NAME_FOR_DEFAULT_CONFIG)
      .map(({name}) => name);
  }
  get columnNote() {
    return this.columnConfiguration
      .filter(({name}) => name !== COLUMN_NAME_FOR_DEFAULT_CONFIG)
      .map(({note}) => note);
  }
  get columnWidth() {
    const widths = {};
    this.columnConfiguration
      .filter(({width}) => width)
      .forEach(({name, width}) => void (widths[name] = width));
    return widths;
  }
  get columnFormat() {
    const formats = {};
    this.columnConfiguration
      .filter(({format}) => format)
      .forEach(({name, format}) => void (formats[name] = format));
    return formats;
  }
  get columnDataRange() {
    const dataRanges = {};
    this.columnConfiguration
      .filter(({dataRange}) => dataRange)
      .forEach(({name, dataRange}) => void (dataRanges[name] = dataRange));
    return dataRanges;
  }
  get idColumn() {
    const idColumns = this.columnConfiguration
      .filter(({idColumn}) => idColumn)
      .map(({name}) => name);
    if (idColumns.length === 0) {
      throw Error('There is no Id column.');
    } else if (idColumns.length > 1) {
      throw Error(`There are multiple Id columns: ${idColumns.join()}`);
    }
    return camelize(idColumns[0]);
  }

  /** Headline style. */
  get headlineStyle() {
    return {
      backgroundColor: '#9AA0A6',
      fontColor: 'white',
    };
  }

  /** Property name of target dataset. */
  get datasetPropertyName() {
    return 'dataset';
  }
  /** Name of the external table. */
  get tableName() {
    return serpentine(this.sheetName);
  }

  /** Initial data for the sheet. */
  get initialData() {}

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
