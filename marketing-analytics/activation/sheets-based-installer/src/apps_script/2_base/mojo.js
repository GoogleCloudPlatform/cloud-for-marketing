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
 * Types of a sheet group
 * @enum {string}
 */
const GROUP_TYPE = Object.freeze({
  COLLAPSE: 'COLLAPSE',
  EXPAND: 'EXPAND',
});

/**
 * Types of whether a GCP resource will be checked or enabled in the solution.
 * @enum {string}
 */
const OPTIONAL_TYPE = Object.freeze({
  // This resource is mandatory. This is the default value.
  MANDATORY: 'MANDATORY',
  // This resource is optional, default checked.
  DEFAULT_CHECKED: 'DEFAULT_CHECKED',
  // This resource is optional, default unchecked.
  DEFAULT_UNCHECKED: 'DEFAULT_UNCHECKED',
});

/**
 * Status of a GCP resource.
 * @enum {string}
 */
const RESOURCE_STATUS = Object.freeze({
  OK: 'OK',
  TO_APPLY: 'TO_APPLY',
  PENDING: 'PENDING',
  ERROR: 'ERROR',
});

/**
 * Check result of a resource.
 * @typedef {{
 *   status: !RESOURCE_STATUS,
 *   message: (string|undefined),
 *   value: (string|undefined),
 *   value_datarange: (!Array<string>|!Function|undefined),
 *   attributeValue: (string|undefined),
 *   attributeValue_datarange: (!Array<string>|undefined),
 *   refreshFn: (!Function|undefined)
 * }}
 */
let CheckResult;

/**
 * @typedef {{
 *   template: (string|undefined),
 *   category: (string|undefined),
 *   resource: (string|undefined),
 *   value: (string|!Function|!Array<string>|undefined),
 *   editType: !RESOURCE_EDIT_TYPE,
 *   optionalType: (!OPTIONAL_TYPE|undefined),
 *   group: (string|undefined),
 *   value_datarange: (!Array<string>|!Function|undefined),
 *   attributeName: (string|undefined),
 *   attributeValue: (string|undefined),
 *   attributeValue_datarange: (!Array<string>|!Function|undefined),
 *   propertyName: (string|undefined),
 *   alwaysCheck: (boolean|undefined),
 *   checkFn: (!Function|undefined),
 *   enableFn: (!Function|undefined)
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
 *   headlineStyle: (!Object|undefined)
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
    'Property Name',
  ],
  columnWidth: {
    Resource: 120,
    Enable: 50,
    Value: 400,
    'Attribute Name': 150,
    'Attribute Value': 250,
    Note: 400,
    'Property Name': 150,
    [COLUMN_NAME_FOR_DEFAULT_CONFIG]: 100,
  },
  columnFormat: {
    Category: COLUMN_STYLES.ALIGN_MIDDLE_AND_CENTER,
    Resource: COLUMN_STYLES.ALIGN_MIDDLE_AND_CENTER,
    Enable: COLUMN_STYLES.ALIGN_MIDDLE_AND_CENTER,
    'Attribute Name': {fn: 'setFontColor', format: '#5F6368'},
    Status: COLUMN_STYLES.ALIGN_MIDDLE_AND_CENTER,
    [COLUMN_NAME_FOR_DEFAULT_CONFIG]: [
      {fn: 'setFontFamily', format: 'Google Sans'},
      COLUMN_STYLES.ALIGN_MIDDLE,
    ],
  },
  columnsToBeMerged: ['Category', 'Resource', 'Value'],
  columnsToBeMergedMap: {
    Value: ['Enable', 'Status', 'Note', 'Property Name'],
  },
};

/**
 * Type of objects restored from rows of Mojo sheet. This type objects are used
 * for checking or enabling.
 *
 * @typedef {{
 *   category: string,
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
 * Conditional format for status cells.
 * @const {!Object<!RESOURCE_STATUS, string>}
 */
const STATUS_BACKGROUND_COLOR = Object.freeze({
  [RESOURCE_STATUS.OK]: '#CEEAD6',
  [RESOURCE_STATUS.PENDING]: '#D2E3FC',
  [RESOURCE_STATUS.TO_APPLY]: '#FEEFC3',
  [RESOURCE_STATUS.ERROR]: '#FAD2CF',
});

/**
 * Templates for Mojo resources.
 * @const {!Object<string,!MojoResource>}
 */
const MOJO_CONFIG_TEMPLATE = {};

const MojoTemplateRegistry = {
  /**
   * Registers a single Mojo template.
   * To ensure the automated packager (cyborg.js) successfully tracks
   * dependencies, you MUST register templates one by one using a string key as
   * the first argument.
   *
   * @param {string} name The unique key name for this template.
   * @param {!MojoResource} template The template definition object.
   */
  register(name, template) {
    if (typeof name !== 'string' || !template) {
      throw new Error(
        `[MojoTemplateRegistry] Invalid registration for '${
          name
        }'. You must explicitly provide a string key and a template object.`,
      );
    }
    // Prevent accidental overwrites of existing templates
    if (MOJO_CONFIG_TEMPLATE[name]) {
      console.warn(
        `[MojoTemplateRegistry] Overwriting existing template for '${name}'.`,
      );
    }
    MOJO_CONFIG_TEMPLATE[name] = template;
  },
};

const MojoFunctions = {};

/**
 * Dynamically builds a deeply nested payload object from dot-separated property
 * paths declared in an attribute mapping table. Reusable across all GCP
 * resource builders in the gcloud namespace.
 * @param {!Object} attributesMap Resource attributes map containing UI values.
 * @param {!Object<string, string>} attributeNamesMapping Mapping of
 *     dot-delimited property paths to UI attribute titles.
 * @param {(function(string, *): *)=} valueTransformFn Optional transformer
 *     callback for cell values.
 * @return {!Object} Nested payload structure.
 */
MojoFunctions.buildPayloadFromAttributes = (
  attributesMap,
  attributeNamesMapping,
  valueTransformFn,
) => {
  const payload = {};
  const properties = getDocumentProperties();

  Object.keys(attributeNamesMapping).forEach(dotPath => {
    const attrTitle = attributeNamesMapping[dotPath];
    const attr = attributesMap[attrTitle];
    if (
      !attr ||
      attr.attributeValue === undefined ||
      attr.attributeValue === ''
    ) {
      return;
    }

    let val = attr.attributeValue;
    if (typeof val === 'string') {
      val = replaceVariables(val, properties);
    }
    if (typeof valueTransformFn === 'function') {
      val = valueTransformFn(dotPath, val);
    } else if (typeof val === 'string' && /^(true|false)$/i.test(val.trim())) {
      val = val.trim().toLowerCase() === 'true';
    }

    const segments = dotPath.split('.');
    let current = payload;
    for (let i = 0; i < segments.length - 1; i++) {
      const segment = segments[i];
      const nextSegment = segments[i + 1];
      const isNextNumeric = /^[0-9]+$/.test(nextSegment);
      if (current[segment] === undefined) {
        current[segment] = isNextNumeric ? [] : {};
      }
      current = current[segment];
    }
    current[segments[segments.length - 1]] = val;
  });

  return payload;
};

/**
 * Combined one-step check and idempotent update/create for lightweight Cloud
Tasks queues.
 * Verifies current queue configuration and creates or updates immediately if
needed.
 * @param {string} queueId - The identifier for the Cloud Tasks Queue.
/**
 * Unifies 404 automatic provisioning and diff patching for GCP resources.
 * Caller explicitly evaluates GET call and passes a mutation closure receiving
payload.
 * @private
 * @param {string} resourceType Friendly resource label.
 * @param {!Object} response Response from current GET probe.
 * @param {function(!Object):!Object} mutateFn Closure receiving payload to
mutate resource.
 * @param {!Object} expectedPayload Desired resource definition.
 * @param {string} statusLink Console link.
 * @param {function(!Object):!Object=} postHook Optional post-reconciliation
hook yielding extra properties.
 * @return {!CheckResult} Standardized status and outcome message.
 */
MojoFunctions.reconcileDeclarativeResource = (
  resourceType,
  response,
  mutateFn,
  expectedPayload,
  statusLink,
  postHook,
) => {
  const executePostHook = resourceObj => {
    if (postHook) {
      try {
        postHook(resourceObj);
      } catch (error) {
        return {
          status: RESOURCE_STATUS.ERROR,
          message: `${resourceType} reconciled, but IAM grant failed: ${
            error.message
          }`,
          status_link: statusLink,
        };
      }
    }
    return null;
  };

  if (response.error) {
    if (response.error.status === 'NOT_FOUND' || response.error.code === 404) {
      console.log(`${resourceType} undiscovered, provisioning resource...`);
      const res = mutateFn(expectedPayload);
      if (res.error) {
        return {
          status: RESOURCE_STATUS.ERROR,
          message: res.error.message,
          status_link: statusLink,
        };
      }
      const hookErr = executePostHook(res);
      if (hookErr) return hookErr;
      return {
        status: RESOURCE_STATUS.OK,
        message: `${resourceType} created successfully.`,
        status_link: statusLink,
      };
    }
    return {
      status: RESOURCE_STATUS.ERROR,
      message: response.error.message,
      status_link: statusLink,
    };
  }

  const diff = diffApiResponseObjects(response, expectedPayload);
  if (!diff) {
    const hookErr = executePostHook(response);
    if (hookErr) return hookErr;
    return {
      status: RESOURCE_STATUS.OK,
      message: `${resourceType} configuration verified up-to-date.`,
      status_link: statusLink,
    };
  }

  console.log(`${resourceType} differs, applying patch:`, JSON.stringify(diff));
  const res = mutateFn(expectedPayload);
  if (res.error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: res.error.message,
      status_link: statusLink,
    };
  }
  const hookErr = executePostHook(res);
  if (hookErr) return hookErr;
  return {
    status: RESOURCE_STATUS.OK,
    message: `${resourceType} configuration updated to match desired specification.`,
    status_link: statusLink,
  };
};

/**
 * Returns a complete MojoResource object based on the given one. This function
 * will gather configurations from the template-chain. The further template in
 * the chain will be overwritten by nearer ones.
 * @param {!MojoResource} row
 * @return {!MojoResource}
 */
const completeMojoConfigFromTemplate = row => {
  const configChain = [row];
  const attributesChain = [row.attributes];
  let latest = row;
  while (latest.template) {
    const {template} = latest;
    latest = MOJO_CONFIG_TEMPLATE[template];
    if (!latest) {
      throw new Error(`Can not find template ${template}.`);
    }
    configChain.push(latest);
    attributesChain.push(latest.attributes);
  }
  // To merge attributes which are arrays
  const attributesMap = {};
  attributesChain.reverse().forEach((attributes = []) => {
    attributes.forEach(attribute => {
      if (!attributesMap[attribute.attributeName]) {
        attributesMap[attribute.attributeName] = Object.assign({}, attribute);
      } else {
        attributesMap[attribute.attributeName] = Object.assign(
          attributesMap[attribute.attributeName],
          attribute,
        );
      }
    });
  });
  const attributes = Object.values(attributesMap);
  const config = Object.assign({}, ...configChain.reverse());
  if (attributes.length > 0) {
    config.attributes = attributes;
  }
  return config;
};

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
   * @param {!MojoConfig} solution
   */
  constructor(solution) {
    // Restore mojo resource settings from all templates.
    this.solutionConfig =
      typeof solution.config === 'function'
        ? solution.config().map(completeMojoConfigFromTemplate)
        : solution.config.map(completeMojoConfigFromTemplate);
    this.enhancedSheet = new EnhancedSheet(solution.sheetName);
    this.headlineStyle = solution.headlineStyle;
    /**
     * Whether the `checkResource` should check the resource with status `OK`.
     * By default it is `false`. If there was a `property` updated, then this
     * will be set as `true` to check all the following resources regardless
     * their status.
     * This might be updated when the `updateStatus_` (@see updateStatus_) is
     * executed, depending on whether there are changes of properties.
     * @type {boolean}
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
     * @type {!Object<number, !Function>}
     */
    this.longTimeOperations = {};
  }

  /**
   * Initializes the sheet.
   */
  initializeSheet() {
    // Clear sheet and create headline
    this.enhancedSheet.initialize(MOJO_SHEET_CONFIG, this.headlineStyle);

    const {sheet} = this.enhancedSheet;
    // Mojo sheet last column index
    const numColumns = MOJO_FIELDS.length;
    const valueCellIndex = MOJO_FIELDS.indexOf('value') + 1;
    const enableCellIndex = MOJO_FIELDS.indexOf('enable') + 1;
    /**
     * A map for properties and their A1 notations. This is used to replace
     * values that have placeholders like `${propertyName}`.
     */
    const propertyA1s = {};
    // The whole category will be enabled or disabled as a group.
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
      Object.keys(row).forEach(key => {
        if (typeof row[key] === 'string' && row[key].indexOf('${') > -1) {
          // Update place holders with sheet functions
          row[key] = this.replacePlaceHolder_(row[key], propertyA1s);
        }
      });
      // 'enable' cell format & value
      const enableCell = sheet.getRange(rowIndex, enableCellIndex, 1, 1);
      const {group, optionalType = OPTIONAL_TYPE.MANDATORY} = row;
      row.enable = this.initializeEnableCell_(enableCell, group, optionalType);
      if (row.propertyName) {
        if (propertyA1s[row.propertyName]) {
          console.warn(
            `Property '${row.propertyName}' has A1 notation `,
            propertyA1s[row.propertyName],
            ', will ignore the new A1 notation',
            valueCell.getA1Notation(),
          );
        } else {
          propertyA1s[row.propertyName] = valueCell.getA1Notation();
        }
      }
      // Prepare data array from the row
      Object.keys(row).forEach(key => {
        const value = row[key];
        const index = MOJO_FIELDS.indexOf(key);
        if (index > -1) {
          result[index] = typeof value === 'function' ? value(row) : value;
        }
        if (key.endsWith('datarange')) {
          // Set a droplist for the cell.
          const columnName = key.split('_')[0];
          this.setDropList_(columnName, rowIndex, value);
        }
      });
      // Prepare links for the row
      Object.keys(row)
        .filter(key => key.endsWith('link'))
        .forEach(key => {
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
    sheet
      .getRange(ROW_INDEX_SHIFT, 1, flatConfig.length, numColumns)
      .setValues(sheetRows)
      .setWrapStrategy(SpreadsheetApp.WrapStrategy.WRAP)
      .setBorder(
        true,
        true,
        true,
        true,
        true,
        true,
        '#9AA0A6',
        SpreadsheetApp.BorderStyle.SOLID,
      );

    this.enhancedSheet.mergeVertically();

    // Set row groups.
    const rowGroups = this.getRowGroups_(flatConfig);
    rowGroups.forEach(({start, end, depth = 1, defaultStatus}) => {
      if (start >= end) return; // Skip for single line row groups.
      start += ROW_INDEX_SHIFT;
      end += ROW_INDEX_SHIFT;
      console.log(`Set row group [${start}, ${end}] ${depth}:`, defaultStatus);
      const range = sheet.getRange(`${start}:${end}`);
      range.shiftRowGroupDepth(depth);
      const group = sheet.getRowGroup(start, depth);
      if (defaultStatus === GROUP_TYPE.COLLAPSE) group.collapse();
    });

    // Status column conditional format
    const statusIndex = MOJO_FIELDS.indexOf('status') + 1;
    const statusColumn = sheet.getRange(
      ROW_INDEX_SHIFT,
      statusIndex,
      sheet.getMaxRows(),
      1,
    );
    const statusRules = Object.keys(STATUS_BACKGROUND_COLOR).map(key => {
      return SpreadsheetApp.newConditionalFormatRule()
        .whenTextEqualTo(key)
        .setBackground(STATUS_BACKGROUND_COLOR[key])
        .setRanges([statusColumn])
        .build();
    });
    sheet.setConditionalFormatRules(statusRules);

    // Set a note at 'Status' column head
    sheet
      .getRange(1, statusIndex, 1, 1)
      .setNote(`Generated at ${new Date().toLocaleString()}`);
  }

  /**
   * Checks the resources in Mojo sheet.
   * @param {boolean} forceCheck
   */
  checkResources(forceCheck) {
    const allMojoSheetRows = this.loadSheet();
    const forcedCheck = forceCheck || this.forceCheck;
    if (forcedCheck) this.clearStatus_(ROW_INDEX_SHIFT);
    allMojoSheetRows.forEach(mojoSheetRows => {
      const {rowIndex: resourceStartRow} = mojoSheetRows[0];
      const succeeded = this.checkResources_(mojoSheetRows, forcedCheck);
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
   * Applies changes in the Mojo sheet.
   */
  applyChanges() {
    const allMojoSheetRows = this.loadSheet();
    allMojoSheetRows.forEach(mojoSheetRows => {
      const mojoResource = this.solutionConfig.filter(
        ({category, resource}) => {
          return (
            category === mojoSheetRows[0].category &&
            resource === mojoSheetRows[0].resource
          );
        },
      )[0];
      const {enableFn: createOrUpdate, resource} = mojoResource;
      if (typeof createOrUpdate === 'undefined') {
        console.log(`Skip resource [${resource}] as no enable function.`);
        return;
      }
      console.log('Start applying resource:', resource);
      mojoSheetRows
        .filter(({status}) => status === RESOURCE_STATUS.TO_APPLY)
        .forEach(resource => {
          const {value, rowIndex} = resource;
          /** @type {!CheckResult} */ let updatedStatus;
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
   * Automatically checks and applies all enabled resources in iterations
   * until all checkable resources reach 'OK' or an error/timeout occurs.
   */
  checkAndApplyAll() {
    const startTime = Date.now();
    // Apps Script max execution time is 6 minutes; reserve 30 seconds buffer.
    const maxElapsedMs = 330 * 1000;
    let previousUnfinishedKey = '';

    /**
     * Loads all enabled and checkable rows from the sheet, and resets
     * `forceCheck` on subsequent rounds so previously 'OK' rows are not cleared.
     * @return {!Array<!MojoSheetRow>}
     */
    const getEnabledCheckableRows = () => {
      const rows = this.loadSheet()
        .reduce((acc, group) => acc.concat(group), [])
        .filter(row => {
          if (row.enable === 'FALSE') return false;
          const config = this.solutionConfig.find(
            ({category, resource}) =>
              category === row.category && resource === row.resource,
          );
          return config && typeof config.checkFn === 'function';
        });
      this.forceCheck = false;
      return rows;
    };

    while (true) {
      // 1. Run checkResources (throws automatically if any row returns ERROR).
      this.checkResources();

      // 2. Inspect current statuses of all enabled checkable rows.
      const unfinishedRows = getEnabledCheckableRows().filter(
        ({status}) => status !== RESOURCE_STATUS.OK,
      );

      // All enabled checkable resources have reached 'OK'.
      if (unfinishedRows.length === 0) {
        SpreadsheetApp.getActiveSpreadsheet().toast(
          'All resources have been checked and applied successfully.',
          'Completed',
        );
        return;
      }

      const toApplyRows = unfinishedRows.filter(
        ({status}) => status === RESOURCE_STATUS.TO_APPLY,
      );

      // 3. If there are TO_APPLY resources, apply them if time permits.
      if (toApplyRows.length > 0) {
        if (Date.now() - startTime >= maxElapsedMs) {
          SpreadsheetApp.getActiveSpreadsheet().toast(
            'Apps Script execution time is running out (< 1 min left). Please click "Check and apply all" again to continue.',
            'Time Limit Notice',
            -1,
          );
          return;
        }

        this.applyChanges();
        SpreadsheetApp.flush();

        const rowsAfterApply = getEnabledCheckableRows();
        if (rowsAfterApply.some(({status}) => status === RESOURCE_STATUS.ERROR)) {
          throw new Error(`Check the row with 'ERROR' for details.`);
        }

        const remainingToApplyKey = rowsAfterApply
          .filter(({status}) => status === RESOURCE_STATUS.TO_APPLY)
          .map(({rowIndex}) => rowIndex)
          .join(',');
        const beforeToApplyKey = toApplyRows
          .map(({rowIndex}) => rowIndex)
          .join(',');
        if (remainingToApplyKey === beforeToApplyKey) {
          SpreadsheetApp.getActiveSpreadsheet().toast(
            'Some resources still require manual input before applying. Please check the rows with "TO_APPLY".',
            'Action Required',
            -1,
          );
          return;
        }
        continue;
      }

      // 4. Unfinished rows remain (e.g. PENDING or blank) with no TO_APPLY.
      // Continue to next checkResources() only if progress was made.
      const currentUnfinishedKey = unfinishedRows
        .map(({rowIndex, status}) => `${rowIndex}:${status}`)
        .join(',');
      if (currentUnfinishedKey === previousUnfinishedKey) {
        SpreadsheetApp.getActiveSpreadsheet().toast(
          'Some resources are still waiting on dependencies and cannot proceed automatically. Please check the non-OK rows.',
          'Pending Dependencies',
          -1,
        );
        return;
      }
      previousUnfinishedKey = currentUnfinishedKey;
    }
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
    this.solutionConfig.forEach(row => {
      if (typeof row.value === 'function') row.value = row.value();
      if (!Array.isArray(row.value)) {
        flatConfig.push(row);
      } else {
        flatConfig.push(
          ...row.value.map((singleValue, index) => {
            const singleRow = Object.assign({}, row, {value: singleValue});
            if (Array.isArray(row.attributeValue)) {
              singleRow.attributeValue = row.attributeValue[index];
            }
            if (Array.isArray(row.attributeName)) {
              singleRow.attributeName = row.attributeName[index];
            }
            return singleRow;
          }),
        );
      }
    });
    return this.flattenAttributes_(flatConfig);
  }

  /**
   * Converts arrays of attributes into multiple rows of data to be fill in
   * the sheet.
   * @param {!Array<!MojoResource>} flatConfig
   * @return {!Array<!MojoResource>}
   * @private
   */
  flattenAttributes_(flatConfig) {
    const results = [];
    flatConfig.forEach(row => {
      if (!Array.isArray(row.attributes)) {
        results.push(row);
      } else {
        results.push(
          ...row.attributes.map(attribute => Object.assign({}, row, attribute)),
        );
      }
    });
    return results;
  }

  /**
   * Generates an array of row groups configuration.
   * @param {!Array<!MojoResource>} flatConfig
   * @return {!Array<{
   *   start: number,
   *   end: number,
   *   defaultStatus: !GROUP_TYPE
   * }>}
   * @private
   */
  getRowGroups_(flatConfig) {
    const rowGroups = [];
    let startedGroups = [];
    // Tag, currentGroupStart, currentGroupType, currentGroupName;
    flatConfig.forEach(
      ({category, resource, group, groupType, optionalType}, index) => {
        const tag = `${category}.${resource}`;
        if (startedGroups.length === 0 && !group && !groupType) return; // not in a group
        let needCheck = true;
        while (startedGroups.length > 0 && needCheck) {
          // already in a group
          needCheck = false;
          const currentGroup = startedGroups[startedGroups.length - 1];
          // console.log(index, tag, startedGroups, currentGroup, rowGroups);
          if (!currentGroup.name) {
            if (tag === currentGroup.tag) return; // continue the same row group
            rowGroups.push({
              start: currentGroup.start,
              end: index - 1,
              defaultStatus: currentGroup.type,
            });
            startedGroups.pop();
            needCheck = true;
          } else {
            if (group === currentGroup.name) {
              // continue the same group and no new row group
              if (!groupType) return;
            } else {
              rowGroups.push({
                start: currentGroup.start,
                end: index - 1,
                defaultStatus: currentGroup.type,
              });
              startedGroups.pop();
              needCheck = true;
            }
          }
        }
        if (group || groupType) {
          const newGroup = {
            start: index + 1,
          };
          if (groupType) {
            // Resource level row group
            newGroup.tag = tag;
            newGroup.type = groupType;
          } else {
            // Category level row group
            newGroup.tag = category;
            newGroup.type =
              optionalType === OPTIONAL_TYPE.DEFAULT_CHECKED
                ? GROUP_TYPE.EXPAND
                : GROUP_TYPE.COLLAPSE;
            newGroup.name = group;
          }
          startedGroups.push(newGroup);
        }
      },
    );
    if (startedGroups.length > 0) {
      startedGroups.reverse().forEach((group, index) => {
        rowGroups.push({
          start: group.start,
          end: flatConfig.length - 1,
          defaultStatus: group.type,
        });
      });
    }
    return rowGroups;
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
      valueCell
        .setFontColor('#5F6368')
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
   * get the normalized value like this: '${namespace}-${projectId|normalized}'.
   *
   * @param {string} str
   * @param {!Object<string, string>} propertyA1s
   * @return {string} A string of Sheets functions.
   * @private
   */
  replacePlaceHolder_(str, propertyA1s) {
    const result = [];
    const regex = /\$\{([^}]+)\}/g;
    let lastIndex = 0;
    let match;
    while ((match = regex.exec(str)) !== null) {
      const literalText = str.substring(lastIndex, match.index);
      result.push(`"${literalText}"`);

      const parts = match[1].split('|');
      const property = parts[0].trim();
      const formatStyle = parts.length > 1 ? parts[1].trim() : undefined;

      if (propertyA1s[property]) {
        if (formatStyle === 'normalized') {
          result.push(
            `REGEXREPLACE(${propertyA1s[property]},"google|\\W","-")`,
          );
        } else if (formatStyle === 'hyphened') {
          result.push(`REGEXREPLACE(${propertyA1s[property]},"_","-")`);
        } else if (formatStyle === 'underscored') {
          result.push(`REGEXREPLACE(${propertyA1s[property]},"-","_")`);
        } else {
          result.push(propertyA1s[property]);
        }
      } else {
        result.push(`"${match[0]}"`);
      }
      lastIndex = match.index + match[0].length;
    }
    result.push(`"${str.substring(lastIndex)}"`);
    const segments = result.filter(e => e !== '""').join(',');
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
   * @param {(!Array<string>|!Function)} datarange
   * @private
   */
  setDropList_(columnName, rowIndex, datarange) {
    const range = this.enhancedSheet.sheet.getRange(
      rowIndex,
      MOJO_FIELDS.indexOf(columnName) + 1,
      1,
      1,
    );
    setSelectDroplist(
      range,
      typeof datarange === 'function' ? datarange() : datarange,
    );
  }

  /**
   * Loads Mojo sheet into an array. The elements of the array are the array of
   * `MojoSheetRow` that has the same `category` and `resource`.
   * All properties will be loaded during this loading process. This can be used
   * to quickly load properties for operations on other sheets.
   * @return {!Array<!Array<!MojoSheetRow>>}
   */
  loadSheet() {
    const mojoSheetRows = this.enhancedSheet.loadArrayOfEntity({
      fields: MOJO_FIELDS,
    });
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
        const mojoSheetRow = Object.assign(
          {rowIndex: index + ROW_INDEX_SHIFT},
          mojoSheetRows[index],
        );
        index++;
        // Push multiple attributes of the same 'value' into an array
        while (
          index < mojoSheetRows.length &&
          mojoSheetRows[index].resource === '' &&
          mojoSheetRows[index].value === ''
        ) {
          if (!mojoSheetRow.attributes) {
            mojoSheetRow.attributes = [
              {
                attributeName: mojoSheetRow.attributeName,
                attributeValue: mojoSheetRow.attributeValue,
              },
            ];
          }
          mojoSheetRow.attributes.push({
            attributeName: mojoSheetRows[index].attributeName,
            attributeValue: mojoSheetRows[index].attributeValue,
          });
          index++;
        }
        if (mojoSheetRow.attributes) {
          mojoSheetRow.attributesMap = {};
          mojoSheetRow.attributes.forEach(attribute => {
            mojoSheetRow.attributesMap[attribute.attributeName] = attribute;
          });
        }
        values.push(mojoSheetRow);
        this.loadProperty_(mojoSheetRow);
      } while (
        index < mojoSheetRows.length &&
        mojoSheetRows[index].resource === ''
      );
    } while (index < mojoSheetRows.length);
    console.log(
      'Properties in Sheet',
      PropertiesService.getDocumentProperties().getProperties(),
    );
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
    const {enable, value, propertyName, status} = line;
    if (propertyName && propertyName.toLocaleLowerCase().endsWith('dataset')) {
      const locationId = getLocationId(
        line.attributesMap['Location'].attributeValue,
      );
      this.updateProperty_(`${propertyName}Location`, locationId);
    }
    const updatedValue = enable === 'FALSE' ? '' : value;
    if (propertyName && status !== RESOURCE_STATUS.ERROR) {
      this.updateProperty_(propertyName, updatedValue);
    }
  }

  /**
   * Updates the document property. It the updated value is different from the
   * existing value, it will set the `forceCheck` to true.
   * @param {string} name
   * @param {string} value
   * @private
   */
  updateProperty_(name, value) {
    const updated = updateDocumentPropertyValue(name, value);
    if (updated) console.log(`Updated property '${name}' to`, value);
    this.forceCheck = this.forceCheck || updated;
    // if (name === 'location') {
    //   const locationId = getLocationId(value);
    //   this.updateProperty_('locationId', locationId);
    // }
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
    const mojoResource = this.solutionConfig.filter(({category, resource}) => {
      return (
        category === mojoSheetRows[0].category &&
        resource === mojoSheetRows[0].resource
      );
    })[0];
    const {sheet} = this.enhancedSheet;
    const {checkFn: checkStatus, alwaysCheck, resource} = mojoResource;
    console.log(`Start ${forceCheck ? 'FORCED ' : ''}checking:`, resource);
    return mojoSheetRows
      .map(mojoSheetRow => {
        const {value, status, enable, rowIndex, propertyName} = mojoSheetRow;
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
        /** @type {!CheckResult} */ let updatedStatus;
        try {
          updatedStatus = checkStatus(value, mojoSheetRow);
        } catch (error) {
          updatedStatus = {
            status: RESOURCE_STATUS.ERROR,
            message: error.message,
          };
        }
        console.log(`Row [${rowIndex}] checking result`, updatedStatus);
        this.updateStatus_(updatedStatus, rowIndex);
        if (propertyName && updatedStatus.status === RESOURCE_STATUS.OK) {
          let propertyValue = updatedStatus.value || value;
          if (propertyValue === '') {
            propertyValue = this.getCellValue_('value', rowIndex);
          }
          this.updateProperty_(propertyName, propertyValue);
        }
        // Expand row groups if the result is not OK. Currently, only expand
        // here to reduce the impact to other process.
        if (updatedStatus && updatedStatus.status !== RESOURCE_STATUS.OK) {
          const depth = sheet.getRowGroupDepth(rowIndex);
          if (depth > 0) {
            sheet.getRowGroup(rowIndex, depth).expand();
          }
        }

        return updatedStatus;
      })
      .filter(checkResult => checkResult !== undefined)
      .every(({status}) => status !== RESOURCE_STATUS.ERROR);
  }

  /**
   * Updates `note` cell for long time operations. It will execute the
   * register `refreshFn` to get the updated status every 20 seconds, between
   * two checks, the `note` cell will be updated with a increasing dot string
   * to show as it is processing.
   * @private
   */
  waitLongTimeOperations_() {
    let rowIndexes = Object.keys(this.longTimeOperations);
    const notes = {};
    let i = 0;
    while (rowIndexes.length > 0) {
      rowIndexes.forEach(rowIndex => {
        if (i % 10 === 0) {
          const status = this.longTimeOperations[rowIndex]();
          this.updateStatus_(status, rowIndex);
          notes[rowIndex] = status.message;
        } else {
          notes[rowIndex] += '.';
          this.updateCells_({note: notes[rowIndex]}, rowIndex);
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
   * the result, it will register it to instance property `longTimeOperations`
   * for continued updating.
   * @see waitLongTimeOperations_
   * @param {!CheckResult} rowStatus
   * @param {number} rowIndex
   * @private
   */
  updateStatus_(rowStatus, rowIndex) {
    if (rowStatus.attributes) {
      rowStatus = Object.assign(rowStatus, rowStatus.attributes[0]);
      if (rowStatus.attributes.length > 1) {
        rowStatus.attributes.slice(1).forEach((attribute, index) => {
          if (attribute.attributeValue_datarange) {
            this.setDropList_(
              'attributeValue',
              rowIndex + index + 1,
              attribute.attributeValue_datarange,
            );
          }
          this.updateCells_(
            {attributeValue: attribute.attributeValue},
            rowIndex + index + 1,
          );
        });
      }
    }
    const {status, message = '', value, attributeValue, refreshFn} = rowStatus;
    const cellValues = {status, note: message, value, attributeValue};
    this.updateCells_(cellValues, rowIndex);
    Object.keys(rowStatus)
      .filter(key => key.endsWith('datarange'))
      .forEach(key => {
        const columnName = key.split('_')[0];
        this.setDropList_(columnName, rowIndex, rowStatus[key]);
      });
    Object.keys(rowStatus)
      .filter(key => key.endsWith('link'))
      .forEach(key => {
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
  updateCells_(cells, rowIndex) {
    // columnName, rowIndex, value
    Object.keys(cells)
      .filter(columnName => typeof cells[columnName] !== 'undefined')
      .forEach(columnName => {
        this.enhancedSheet.save(
          [[cells[columnName]]],
          rowIndex,
          MOJO_FIELDS.indexOf(columnName) + 1,
        );
      });
  }

  /**
   * Ges the value from a cell.
   * @param {string=} columnName
   * @param {number} rowIndex
   * @return {string}
   * @private
   */
  getCellValue_(columnName, rowIndex) {
    const range = this.enhancedSheet.sheet.getRange(
      rowIndex,
      MOJO_FIELDS.indexOf(columnName) + 1,
      1,
      1,
    );
    console.log('get range', range.getDisplayValue());
    return range.getDisplayValue();
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
      rowIndex,
      MOJO_FIELDS.indexOf(columnName) + 1,
      1,
      1,
    );
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
    const {sheet} = this.enhancedSheet;
    const rowNumber = sheet.getLastRow() - startRow + 1;
    if (rowNumber === 0) {
      console.log('Clearing resource status after the last row, quit.');
      return;
    }
    console.log('Clearing resource status after row:', startRow);
    ['status', 'note']
      .map(column => MOJO_FIELDS.indexOf(column) + 1)
      .forEach(startColumn => {
        sheet.getRange(startRow, startColumn, rowNumber, 1).clearContent();
      });
  }
}
