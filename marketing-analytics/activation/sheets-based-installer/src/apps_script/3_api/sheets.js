// Copyright 2022 Google Inc.
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

/** @fileoverview Google Sheets API handler class.*/

class Sheets extends ExternalApi {

  constructor(option) {
    super(option);
    this.apiUrl = 'https://sheets.googleapis.com';
    this.version = 'v4';
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /** @override */
  getScope() {
    return 'https://www.googleapis.com/auth/spreadsheets';
  }

  /**
   * Verifies the accessibililty of the spreadsheet.
   * @see https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get
   * @param {string} spreadsheetId
   * @return {VerifyResult}
   */
  verifySpreadsheet(spreadsheetId) {
    const { error } = this.get(`spreadsheets/${spreadsheetId}`);
    if (error) {
      return {
        valid: false,
        reason: error.message,
      };
    }
    return { valid: true };
  }

  /**
   * Verifies the sheet by the spreadsheet id and sheet name.
   * @param {string} spreadsheetId
   * @param {string} sheetName
   * @return {VerifyResult}
   */
  verifySheet(spreadsheetId, sheetName) {
    const { error } = this.get(
      `spreadsheets/${spreadsheetId}`, { ranges: sheetName });
    if (error) {
      return {
        valid: false,
        reason: error.message,
      };
    }
    return { valid: true };
  }

  /**
   * Gets the values of given range of a spreadsheet.
   * @param {string} spreadsheetId
   * @param {string} sheetName
   * @param {number=} row
   * @return {Array<Array<string>>}
   */
  getHeadline(spreadsheetId, sheetName, row = 1) {
    const range = `'${sheetName}'!A${row}:${row}`
    return this.get(
      `spreadsheets/${spreadsheetId}/values/${encodeURIComponent(range)}`);
  }
}
