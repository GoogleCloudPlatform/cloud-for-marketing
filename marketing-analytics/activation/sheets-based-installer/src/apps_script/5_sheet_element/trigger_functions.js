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

/** @fileoverview Google Sheets AppsScript trigger functions. */

/**
 * A menu item contains a label and a function name.
 * To assign a function dynamically, `MENU_OBJECTS` is used to host those
 * instance objects that have the detailed functions. In this way, different
 * sheets will have their own functions even they are the same ones (but for
 * different sheet).
 * @see https://developers.google.com/apps-script/reference/base/menu
 * @const {!Object<string, !Object<string, function>>}
 */
const MENU_OBJECTS = {};

/**
 * Creates the 'Cyborg' menu when a user who has permission to edit opens the
 * spreadsheet. This function will use a global variable `SOLUTION_MENUS` which
 * defines all the menu items.
 * @see https://developers.google.com/apps-script/guides/triggers#onopene
 */
function onOpen() {
  /**
   * Creates menu items and add them to the given menu.
   *
   * @param {!Menu} menu
   * @param {!Array<{
   *   name: string|undefined,
   *   method: string|undefined,
  *   seperator: boolean|undefined,
   * }>} menuItems An array of menu item.
   */
  const insertMenuItem = (menu, menuItems) => {
    menuItems.forEach(({ name, method, separator }) => {
      if (!separator) {
        menu.addItem(name, method);
      } else {
        menu.addSeparator();
      }
    });
  }
  const menu = SpreadsheetApp.getUi().createMenu('🤖 Cyborg');
  SOLUTION_MENUS.forEach(({ sheetName, menuItem = [], separator }) => {
    if (separator) {
      menu.addSeparator();
      return;
    }
    if (menuItem.length === 0) return;
    if (sheetName) {
      const subMenu = SpreadsheetApp.getUi().createMenu(sheetName);
      insertMenuItem(subMenu, menuItem);
      menu.addSubMenu(subMenu);
    } else {
      insertMenuItem(menu, menuItem);
    }
  });
  menu.addToUi();
}

/**
 * The columns listed in this array are expected to have a JSON string.
 * `onEdit` trigger  will run a function to validate and format the content.
 */
const JSON_COLUMNS = [];

/**
 * For those registed JSON cells (@see JSON_COLUMNS), `onEdit` trigger tries to
 * format the string with a better style for readablity and changes the font
 * color to red if it is an malformat JSON string.
 * @see https://developers.google.com/apps-script/guides/triggers#onedite
 */
function onEdit(event) {
  const range = event.range;
  const activeSheet = range.getSheet();
  const sheetName = activeSheet.getName();
  if (range.getRow() > 1) {
    const columnNames = activeSheet.getRange(
      1, range.getColumn(), 1, range.getNumColumns()).getDisplayValues()[0];
    columnNames.forEach((columnName, index) => {
      if (JSON_COLUMNS.indexOf(`${sheetName}.${columnName}`) > -1) {
        const columnIndex = index + range.getColumn();
        const rowStart = range.getRow();
        for (let i = 0; i < range.getNumRows(); i++) {
          const cell = activeSheet.getRange(rowStart + i, columnIndex, 1, 1);
          formatSheetRowForJsonCell(cell);
        }
      }
    });
  }
}
