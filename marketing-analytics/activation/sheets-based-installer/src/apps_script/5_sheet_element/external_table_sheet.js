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
class ExternalTableSheet {
  constructor() {
    this.columnFormat = {
      default_: { fn: 'setFontFamily', format: 'Consolas' },
    };
    this.headlineStyle = {
      backgroundColor: '#EA4335',
      fontColor: 'white',
    };
  }

  /**
   * Returns the expected BigQuery table name.
   * @abstract
   * @return {string}
   */
  getTableName() {
    return camelize(this.sheetName);
  }

  /**
   * Initializes the sheet.
   */
  initialize(url) {
    const spreadsheet = url ? SpreadsheetApp.openByUrl(url) : undefined;
    const enhancedSheet = new EnhancedSheet(this.sheetName, spreadsheet);
    enhancedSheet.initialize(this);
  }
}
