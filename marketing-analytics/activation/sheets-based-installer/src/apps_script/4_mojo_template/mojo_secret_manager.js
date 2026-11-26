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
 * Checks the status of a specified secret of Secret Manager.
 * @param {string} name Name of secret.
 * @return {!CheckResult}
 */
MojoFunctions.checkSecretManager = name => {
  const projectId = getDocumentProperty('projectId');
  const secretManager = new SecretManager(projectId);
  const {error, state} = secretManager.getSecretVersion(name);
  if (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.message,
    };
  }
  if (state !== 'ENABLED') {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: `Secret state ${state}.`,
    };
  }
  return {
    status: RESOURCE_STATUS.OK,
  };
};

MojoTemplateRegistry.register('secretManager', {
  category: 'Google Cloud',
  resource: 'Secret Manager',
  optionalType: OPTIONAL_TYPE.DEFAULT_UNCHECKED,
  checkFn: MojoFunctions.checkSecretManager,
  propertyName: 'defaultSecretName',
});
