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

/**
 * Checks the status of a Api key.
 * @param {string} key The name of a API key.
 * @param {!Object} resource The resource object.
 * @return {!CheckResult}
 */
MojoFunctions.checkApiKey = (key, resource) => {
  const projectId = getDocumentProperty('projectId');
  const {attributeValue: name} = resource;
  const apiKeys = new ApiKeys(projectId);
  // try {
  const keys = apiKeys.loadKeyByName(name);
  if (keys.length === 0) {
    return {
      status: RESOURCE_STATUS.TO_APPLY,
      value: '',
      message: `Not created yet.`,
    };
  } else {
    const keyId = keys[0].uid;
    const keyString = apiKeys.getKeyString(keyId);
    let message = 'Loaded existing API key.';
    if (keys.length > 1) {
      message += ' [WARN] There are multi keys with the same name.';
    }
    return {
      status: RESOURCE_STATUS.OK,
      value: keyString,
      message,
    };
  }
};

/**
 * Checks the status of a Api key.
 * @param {string} name The name of a API key.
 * @param {!Object} restrictions The restrictions of a API key.
 * @param {string=} propertyName The property name to save the key string.
 * @return {!CheckResult}
 */
MojoFunctions.createApiKey = (name, restrictions, propertyName) => {
  const projectId = getDocumentProperty('projectId');
  const apiKeys = new ApiKeys(projectId);
  const rawResponse = apiKeys.createKeyAndReturnResult(name, restrictions);

  if (rawResponse.error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      value: '',
      message: rawResponse.error.message,
    };
  }

  if (propertyName) {
    updateDocumentPropertyValue(propertyName, rawResponse.keyString);
  }

  return {
    status: RESOURCE_STATUS.OK,
    value: rawResponse.keyString,
    message: 'API key was created successfully',
  };
};

// Demo localized enableFn:
// TODO:
// There are several things need to be optimized:
// 1, Support a configurable restrictions attribute, rather than this hardcoded
//   functions;
// 2, Save the Api key to Secret manager.
/**
 * Creates an API key in the solution.
 * @param {string} keyString The key string of the API key.
 * @param {!Object} resource The resource object.
 * @return {!CheckResult}
 */
function createApiKeyInSolution(keyString, resource) {
  const restrictions = {
    apiTargets: [{service: 'generativelanguage.googleapis.com'}],
  };
  return MojoFunctions.createApiKey(
    resource.attributeValue,
    restrictions,
    resource.propertyName,
  );
}

MojoTemplateRegistry.register('apikeys', {
  category: 'Google Cloud',
  resource: 'API Keys',
  attributeName: 'Key name',
  propertyName: 'apiKey',
  editType: RESOURCE_EDIT_TYPE.READONLY,
  checkFn: MojoFunctions.checkApiKey,
  // enableFn: createApiKeyInSolution,
});
