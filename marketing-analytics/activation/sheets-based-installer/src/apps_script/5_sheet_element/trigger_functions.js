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
   * @param {!Array<!Object>} menuItems An array of menu item.
   */
  const insertMenuItem = (menu, menuItems) => {
    menuItems.forEach((item) => {
      const {separator, name, method, sheetName, menuItem = []} = item;
      if (separator) {
        menu.addSeparator();
      } else if (name && method) {
        menu.addItem(name, method);
      } else if (menuItem.length > 0) {
        const subMenuName = sheetName || name;
        // If there is a 'name' or 'sheetName' for an array of menu items, then
        // add all menu items as sub menu items of that name.
        if (subMenuName) {
          const subMenu = SpreadsheetApp.getUi().createMenu(subMenuName);
          insertMenuItem(subMenu, menuItem);
          menu.addSubMenu(subMenu);
        } else {  // Append the menu items to the current menu.
          insertMenuItem(menu, menuItem);
        }
      } else {
        console.log('Unknown menu item', item);
      }
    });
  };
  const menu = SpreadsheetApp.getUi().createMenu('🤖 Cyborg');
  insertMenuItem(menu, SOLUTION_MENUS);
  menu.addToUi();
}

/**
 * OnEdit trigger routes the edit event to the specific active sheet instance
 * (currently supported for PlainSheet and its subclasses) to handle tasks like
 * formatting JSON columns automatically.
 *
 * NOTE: If other sheet classes (e.g., MojoSheet) need to support automatic
 * format or validation on edit features in the future, you should:
 * 1. Add their class registry to the event routing logic below
 *    (e.g., `sheetInstance = sheetInstance ||
 * MojoSheet.instances[menuObjectName]`).
 * 2. Implement an `onEdit(event)` instance method within that sheet class.
 *    Look at `PlainSheet.prototype.onEdit` for reference on handling range
 * dimensions.
 *
 * @see https://developers.google.com/apps-script/guides/triggers#onedite
 * @param {!Object} event Event object from Google Apps Script.
 */
function onEdit(event) {
  const range = event.range;
  const activeSheet = range.getSheet();
  const sheetName = activeSheet.getName();

  if (typeof getSafePropertyName !== 'undefined' &&
      typeof PlainSheet !== 'undefined') {
    const menuObjectName = getSafePropertyName(sheetName);
    const sheetInstance = PlainSheet.instances[menuObjectName];
    if (sheetInstance && typeof sheetInstance.processEditEvent === 'function') {
      sheetInstance.processEditEvent(event);
    }
  }
}

/**
 * Initializes all sheets of the application. This function is used by Cyborg
 * command init.
 * @param {!Array<!Object>=} menuItems Array of menu items to process.
 */
function initializeApplication(menuItems = SOLUTION_MENUS) {
  menuItems.forEach((menuItem) => {
    const {sheetName, initialize, menuItem: subMenuItems} = menuItem;
    if (Array.isArray(subMenuItems)) {
      initializeApplication(subMenuItems);
    }
    if (!sheetName || !initialize) {
      if (sheetName) console.log('Skip non sheet menu item', sheetName);
      return;
    }
    if (SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName)) {
      console.log(`Sheet [${sheetName}] exists. Skip initializing.`);
    } else {
      menuItem.initialize();
      console.log(`Initialized Sheet [${sheetName}].`);
    }
  });
}
