// Copyright 2026 Google Inc.
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

/** @fileoverview  */

/** Checks the number is a positive number. */
// TODO: deprecated?
MojoFunctions.checkNonNegativeNumber = number => {
  if (number - 0 >= 0) return {status: RESOURCE_STATUS.OK};
  const message = 'Expected a positive number.';
  return {status: RESOURCE_STATUS.ERROR, message};
};

/** Cleans the account number. */
// TODO: deprecated?
MojoFunctions.cleanAccountNumber = (value, resource) => {
  const regex = /^[0-9][0-9-]*[0-9]$/;
  const result = regex.test(value);
  if (result) {
    return {
      status: RESOURCE_STATUS.OK,
      value: value.replaceAll('-', ''),
    };
  }
  const message = `Only digits and dash(-) are allowed in account ID.`;
  return {status: RESOURCE_STATUS.ERROR, message};
};

MojoTemplateRegistry.register('namespace', {
  category: 'General',
  resource: 'Namespace',
  propertyName: 'namespace',
  editType: RESOURCE_EDIT_TYPE.DEFAULT_VALUE,
  checkFn: namespace => {
    const regex = /^[a-z][a-z0-9-_]{1,14}[a-z0-9]$/;
    const result = regex.test(namespace);
    if (result) return {status: RESOURCE_STATUS.OK};
    const message =
      'Expected 3 to 16 of lower letters, digits, hyphens or underscores with a leading letter.';
    return {status: RESOURCE_STATUS.ERROR, message};
  },
});
// Attributes value for the description of the parameter.
MojoTemplateRegistry.register('parameter', {
  category: 'General',
  resource: 'Parameter',
  editType: RESOURCE_EDIT_TYPE.USER_INPUT,
  checkFn: (value, resource) => {
    if (value) return {status: RESOURCE_STATUS.OK};
    const message = `Input the value for parameter: ${resource.attributeValue}`;
    return {status: RESOURCE_STATUS.ERROR, message};
  },
});
