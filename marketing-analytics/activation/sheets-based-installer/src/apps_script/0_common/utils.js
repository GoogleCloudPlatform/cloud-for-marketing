// Copyright 2019 Google Inc.
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

/** @fileoverview Utility functions. */

/**
 * Converts a field description into a lowerCamelCase property name.
 * Example: `File Name` -> `fileName`
 * It can also turn snake_style property name to lowerCamelCase.
 * Example: `file_name` -> `fileName`
 * @param {string} name
 * @return {string}
 */
const camelize = name => {
  return name
    .toLowerCase()
    .replace(/[^a-zA-Z0-9]+(.)/g, (m, chr) => chr.toUpperCase())
    .replace(/[^a-zA-Z0-9]/g, '');
};

/**
 * Converts a field description into a snake style property name.
 * Example: `File Name` -> `file_name`
 * @param {string} name
 * @return {string}
 */
const serpentine = name => {
  return name.toLowerCase().replaceAll(' ', '_');
};

/**
 * Replaces a string with parameters in the pattern like `${key}`. Gets values
 * from the parameters object. Nested keys are supported.
 * @param {string} str Original string with parameters.
 * @param {!Object<string, string>} parameters
 * @param {boolean=} ignoreUnfounded Whether to ignore those properties that are
 *     not founded in the parameters. Default it throws an error if any
 *     property is not found. If set as true, it will keep parameters in
 *     original `${key}` way.
 * @return {string} Parameters replaced string.
 */
const replaceParameters = (str, parameters, ignoreUnfounded = false) => {
  const indexOfFirstPlaceholder = str.indexOf('${');
  if (indexOfFirstPlaceholder === -1) return str;
  const prefix = str.substring(0, indexOfFirstPlaceholder);
  const regex = /\${([^}]*)}/;
  const matchResult = str.match(regex);
  const splitNames = matchResult[1].split('.');
  const left = str.substring(indexOfFirstPlaceholder + matchResult[0].length);
  let value = parameters;
  for (let index in splitNames) {
    const namePiece = splitNames[index];
    if (!value || !value[namePiece]) {
      if (ignoreUnfounded) {
        value = matchResult[0];
        break;
      }
      console.error(
        `Fail to find property ${matchResult[1]} in parameters: `,
        parameters,
      );
      throw new Error(`Fail to find property ${matchResult[1]} in parameter.`);
    }
    value = value[namePiece];
  }
  return prefix + value + replaceParameters(left, parameters, ignoreUnfounded);
};

/**
 * Replaces a string with parameters in the pattern like `{snake_style_key}`.
 * This function will replace those parameters with `${snakeStyleKey}` then use
 * `replaceParameters` function. No nested keys are supported.
 * Note the names of properties in `parameters` are all camel style.
 * @param {string} str
 * @param {!Object<string, string>} parameters
 * @param {boolean=} ignoreUnfounded
 * @return {string}
 */
const replacePythonStyleParameters = (
  str,
  parameters,
  ignoreUnfounded = false,
) => {
  const regex = /\{([^}]*)}/g;
  const updatedStr = str.replaceAll(regex, (match, parameterName) => {
    return '${' + camelize(parameterName) + '}';
  });
  return replaceParameters(updatedStr, parameters, ignoreUnfounded);
};

/**
 * Replaces a string with variables in the pattern like `#key#`. Gets values
 * from a given map object. No nested keys are supported.
 * @param {string} str Original string with parameters.
 * @param {!Object<string, string>} variables
 * @return {string} Variables replaced string.
 */
const replaceVariables = (str, variables) => {
  return Object.keys(variables).reduce((previousValue, key) => {
    const regExp = new RegExp(`#${key}#`, 'g');
    return previousValue.replace(regExp, variables[key]);
  }, str);
};

// Functions help generate data for a mutation API request.
/**
 * Deeply compares a target object against a source object and returns an object
 * containing only the changed or added properties.
 *
 * Features a smart comparison for numbers and numeric strings, treating
 * variations like '08:00' and '8:00', or 1 and '1', as equivalent.
 *
 * @param {!Object=} source
 * @param {!Object} target
 * @return {!Object}
 */
const diffApiResponseObjects = (source = {}, target) => {
  if (Array.isArray(target)) {
    if (!Array.isArray(source) || source.length !== target.length) {
      return target;
    }
    const unmatchedSourceIndices = new Set(source.map((_, i) => i));
    for (const targetItem of target) {
      let matchFound = false;
      for (const sourceIndex of unmatchedSourceIndices) {
        const sourceItem = source[sourceIndex];
        if (typeof targetItem === 'object' && targetItem !== null) {
          if (!diffApiResponseObjects(sourceItem, targetItem)) {
            unmatchedSourceIndices.delete(sourceIndex);
            matchFound = true;
            break;
          }
        } else if (sourceItem === targetItem) {
          unmatchedSourceIndices.delete(sourceIndex);
          matchFound = true;
          break;
        } else if (
          ['number', 'string'].includes(typeof sourceItem) &&
          ['number', 'string'].includes(typeof targetItem) &&
          String(sourceItem).localeCompare(String(targetItem), undefined, {
            numeric: true,
          }) === 0
        ) {
          unmatchedSourceIndices.delete(sourceIndex);
          matchFound = true;
          break;
        }
      }
      if (!matchFound) {
        return target;
      }
    }
    return undefined;
  }

  const result = {};
  Object.keys(target).forEach(key => {
    const value = target[key];
    const sourceValue = source ? source[key] : undefined;
    if (typeof value === 'object' && value !== null) {
      const diffed = diffApiResponseObjects(sourceValue, value);
      if (diffed !== undefined) {
        result[key] = diffed;
      }
    } else if (sourceValue !== value) {
      if (
        [sourceValue, value].every(v => ['number', 'string'].includes(typeof v))
      ) {
        if (
          String(sourceValue).localeCompare(String(value), undefined, {
            numeric: true,
          }) === 0
        ) {
          return;
        }
      }
      result[key] = value;
    }
  });
  if (Object.keys(result).length > 0) {
    return result;
  }
};

/**
 * Gets the FieldMask of a given object.
 * This function supports embedded properties.
 * @see https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask
 * @param {!Object} obj
 * @return {!Array<string>}
 */
const getPropertyNamesAsFieldMask = obj => {
  const result = [];
  Object.keys(obj).forEach(key => {
    const value = obj[key];
    if (typeof value === 'object') {
      result.push(
        ...getPropertyNamesAsFieldMask(value).map(name => `${key}.${name}`),
      );
    } else {
      result.push(key);
    }
  });
  return result;
};

/**
 * Converts a string to be a safe property name.
 * Only keep letters, digits and underscore and replace all other characters
 * with underscores. If the string starts with a digit, add a leading
 * underscore to it.
 *
 * @param {string} str
 * @return {string}
 */
const getSafePropertyName = str => {
  return str
    .trim()
    .replace(/[^a-zA-Z0-9_]/g, '_')
    .replace(/^([0-9])/, '_$1');
};

/**
 * Splits parameters (values) into small batches so it will not exceed the limit
 * of a URL querystring.
 * @param {function(!Array<string>): !Array<*>} fn
 * @param {!Array<string>} values
 * @param {number=} batchSize How many values for each request.
 * @return {!Array<*>}
 */
const splitFilterConditions = (fn, values, batchSize = 50) => {
  return Array(Math.ceil(values.length / batchSize))
    .fill(1)
    .map((unused, i) => fn(values.slice(i * batchSize, (i + 1) * batchSize)))
    .flat();
};

// Functions related to Google Sheets.
/**
 * Returns the sheet Id from the sheet url.
 * @param {string} sheetUrl
 * @return {string}
 */
const getSpreadsheetIdFromUrl = sheetUrl => {
  try {
    return /spreadsheets\/d\/([^/]*)/.exec(sheetUrl)[1];
  } catch (error) {
    console.error('This is not a valid sheet url', sheetUrl);
  }
};

/**
 * Returns the last modified time of the current sheet.
 * @return {!Date}
 */
const getSheetLastModifiedTime = () => {
  const id = SpreadsheetApp.getActiveSpreadsheet().getId();
  const file = DriveApp.getFileById(id);
  return file.getLastUpdated();
};

/**
 * Gets the letter of a given column (0-based index).
 * @param {number} index
 * @return {string}
 */
const getColumnLetter = index => {
  if (index < 0) throw new Error(`Can not find column index for ${index}.`);
  const highPosition = Math.floor(index / 26);
  const remainder = index % 26;
  const lastLetter = String.fromCharCode('A'.charCodeAt(0) + remainder);
  if (highPosition > 0) {
    return getColumnLetter(highPosition - 1) + lastLetter;
  }
  return lastLetter;
};

/**
 * For a cell supposed to be a JSON string. Check whether it is valid and format
 * the JSON string.
 * @param {*} cell
 */
const formatSheetRowForJsonCell = cell => {
  const json = cell.getDisplayValue();
  try {
    const obj = JSON.parse(json);
    const formattedJSON = JSON.stringify(obj, null, 2);
    const lines = formattedJSON.split('\n');
    cell.getSheet().setRowHeight(cell.getRow(), lines.length * 10);
    cell.setValue(formattedJSON);
    cell.setFontColor('black');
    cell.clearNote();
  } catch (error) {
    if (json.trim() === '') {
      cell.setFontColor('black');
      cell.clearNote();
    } else {
      cell.setNote(error.message);
      cell.setFontColor('red');
      console.log(error);
    }
  }
};

/**
 * Creates a data validation of Google Sheets and set it for the given range.
 * @param {!Range} range Google Sheets range.
 * @param {!Array<string>} list A list of values for data validation.
 * @param {boolean=} allowInvalid Whether or not allow values not in the list.
 */
const setSelectDroplist = (range, list, allowInvalid = false) => {
  const dataValidation = SpreadsheetApp.newDataValidation()
    .requireValueInList(list)
    .setAllowInvalid(allowInvalid)
    .build();
  range.setDataValidation(dataValidation);
};

// Functions related to (Apps Script) document properties.
/**
 * Gets the value of a key from the document properties.
 * @see https://developers.google.com/apps-script/guides/properties
 * @param {string} key
 * @param {boolean=} shouldExist
 * @return {string|undefined}
 */
const getDocumentProperty = (key, shouldExist = true) => {
  const value = PropertiesService.getDocumentProperties().getProperty(key);
  if (typeof value === 'undefined' && shouldExist) {
    throw new Error(`Can't find property ${key}.`);
  }
  return value;
};

/**
 * Gets all key and value pairs from document properties.
 * @return {!Object<string,string>}
 */
const getDocumentProperties = () => {
  return PropertiesService.getDocumentProperties().getProperties();
};

/**
 * Clears all key and value pairs in document properties.
 */
const clearDocumentProperties = () => {
  PropertiesService.getDocumentProperties().deleteAllProperties();
};

/**
 * Checks the value of given key in document properties and updates it if it is
 * different. Returns true if the value was updated.
 * @param {string} key
 * @param {string} value
 * @return {boolean} Whether the value was updated.
 */
const updateDocumentPropertyValue = (key, value) => {
  const properties = PropertiesService.getDocumentProperties();
  const existingValue = properties.getProperty(key);
  if (value === existingValue) return false;
  properties.setProperty(key, value);
  return true;
};

/**
 * Returns the latest versions of a given node package.
 * @param {string} packageName package name.
 * @param {number=} limit How many latest versions will be returned. Default 10.
 * @return {!Array<string>} The array of versions.
 */
const getNpmVersions = (packageName, limit = 10) => {
  const url = `https://registry.npmjs.org/${packageName}`;
  const response = UrlFetchApp.fetch(url);
  const versions = Object.keys(JSON.parse(response.getContentText()).versions);
  return versions.reverse().filter((version, index) => index < limit);
};

/**
 * Returns the information object of the specified package and version.
 * @param {string} packageName package name.
 * @param {string=} version package version, by default, it is 'latest'.
 * @return {!Object} The information object of the package.
 */
const getNodePackageInfo = (packageName, version = 'latest') => {
  const url = `https://registry.npmjs.org/${packageName}/${version}`;
  const response = UrlFetchApp.fetch(url);
  return JSON.parse(response.getContentText());
};

/**
 * Gets the BigQuery schema from the Google Sheet column names.
 * The column names are expected to be in snake case naming convention. The last
 * piece of the name will be used to determine the type.
 * By default the type is 'STRING'.
 * @param {!Array<string>} fields Sheet column names.
 * @param {!Object<string, string>=} mappedTypes Column name suffix and BigQuery
 *     type map.
 * @return {!Array<{name:string, type:string}>}
 */
const getBigQuerySchema = (fields, mappedTypes = {}) => {
  if (!fields) return;
  return fields.map(field => {
    const suffix = field.substring(field.lastIndexOf('_') + 1);
    return {
      name: field,
      type: mappedTypes[suffix] || 'STRING',
    };
  });
};

/**
 * Gets the sql for a BigQuery query job. It will get the query, then use the
 * `parameterReplacer` function to replace parameters in the query with
 * `parameters`.
 * @param {string} source Sql source, can be a HTTP link, a key name of the
 *   default `DEFAULT_SQL_FILES` object or sql content.
 * @param {!Object<string, string>} parameters
 * @param {(function(string,!Object<string, string>): string)=}
 *     parameterReplacer
 *   Default is @see replaceParameters
 * @return {string}
 */
const getExecutableSql = (
  source,
  parameters,
  parameterReplacer = replaceParameters,
) => {
  let template = source;
  if (source.substring(0, 5).toLowerCase().startsWith('http')) {
    template = UrlFetchApp.fetch(source).getContentText();
  } else if (typeof DEFAULT_SQL_FILES !== 'undefined') {
    if (DEFAULT_SQL_FILES[source]) template = DEFAULT_SQL_FILES[source];
  }
  return parameterReplacer(template, parameters);
};

/**
 * Returns the name based location object.
 * @param {{displayName:string, locationId: string}} location
 * @return {string}
 */
const getLocationListName = location => {
  const {displayName, locationId} = location;
  return `${displayName} | ${locationId}`;
};

/**
 * Return location Id based on a location string. The string can be location Id
 * or `locationName | locationId`.
 * @param {string} locationListName
 * @return {string}
 */
const getLocationId = locationListName => {
  return locationListName.indexOf('|') === -1
    ? locationListName
    : locationListName.split('|')[1].trim();
};

/**
 * Returns the location object from a list with the specified location Id.
 * @param {!Array<{locationId:string, displayName:string}>} locations
 * @param {string} id
 * @return {{displayName:string, locationId: string}}
 */
const getLocationObject = (locations, id) => {
  return locations.filter(({locationId}) => locationId === id)[0];
};

/**
 *
 * @param {!Object} vars
 * @return {string}
 */
const getEnvVarForCloudRun = vars => {
  return Object.keys(vars)
    .map(key => `${key}=${vars[key]}`)
    .join(',\n');
};
