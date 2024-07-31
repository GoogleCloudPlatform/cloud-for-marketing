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

/** @fileoverview This is the class of a colab solution. */

/**
 * Default settings for Cloud Functions. In the detailed solution class, these
 * can be reused or overwritten.
 */
const PYTHON_FUNCTIONS_DEFAULT_SETTINGS = {
  runtime: 'python312',
  timeout: '540s',
  availableMemoryMb: 2048,
  dockerRegistry: 'ARTIFACT_REGISTRY',
};

const PUPA_PROXY_FUNCTION_NAME = 'proxy';
const PUPA_COLAB_PACKAGE = 'colab_function';

/**
 * This class is the base of a Colab solution that have Cloud Functions to be
 * deployed. It helps to create the source code to be deployed
 * and generate the source upload url of Cloud Functions.
 * @see https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions/generateUploadUrl
 */
class ColabSolution {

  /**
   * @param {!SolutionOption} options
   */
  constructor(options) {
    this.projectId = options.projectId;
    this.locationId = options.locationId || 'us-central1';
    this.cloudFunctions = new CloudFunctions(this.projectId, this.locationId);
  }

  /**
   * Deploys a HTTP Cloud Function based on the given piece of Python code from
   * the Colab. This Cloud Function has a fixed entry point function named
   * `PUPA_PROXY_FUNCTION_NAME`.
   * @param {string} functionName The name of the deployed Cloud Function.
   * @param {string} sourceCode The Python code to be deployed.
   * @return {string} The name of operation.
   */
  deployCloudFunctions(functionName, sourceCode) {
    this.sourceCode = sourceCode;
    const entryPoint = PUPA_PROXY_FUNCTION_NAME;
    const httpsTrigger = {};
    return this.deploySingleCloudFunction(functionName,
      { entryPoint, httpsTrigger }
    );
  }

  /**
   * Deploys a Cloud Functions and returns the operation name.
   *
   * @param {string} functionName
   * @param {Object} options
   * @return {string} The name of operation.
   */
  deploySingleCloudFunction(functionName, options) {
    const payload = Object.assign({}, PYTHON_FUNCTIONS_DEFAULT_SETTINGS, {
      sourceUploadUrl: this.getSourceUploadUrl(),
      environmentVariables: this.getEnvironmentVariables(),
    }, options);
    return this.cloudFunctions.createOrUpdate(functionName, payload);
  }

  /**
   * Returns a signed URL for uploading a function source code.
   * Cloud Functions in the same solution can share the same URL.
   * @see https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions/generateUploadUrl
   * @return {string} The uploaded source URL.
   */
  getSourceUploadUrl() {
    if (this.sourceUploadUrl) return this.sourceUploadUrl;
    const files = [
      { file: `${PUPA_COLAB_PACKAGE}.py`, content: this.getDeployableCode() },
      { file: 'main.py', content: this.getMainFile() },
      { file: 'requirements.txt', content: this.getRequirementsFile() },
    ]
    this.sourceUploadUrl = this.cloudFunctions.uploadSourceAndReturnUrl(files);
    return this.sourceUploadUrl;
  }

  /**
   * Returns the modified Python code which can be deployed in a Cloud Function.
   * Shell commands (start with !) are not supported in Cloud Functions, so
   * those lines will be commented. The modified Python code will be deployed
   * as a file named `PUPA_COLAB_PACKAGE`.py in the Cloud Function.
   *
   * @param {string} sourceCode
   * @return {string}
   */
  getDeployableCode(sourceCode = this.sourceCode) {
    const regex = /^! *pip *install /;
    return sourceCode.split('\n').map(
      (line) => regex.test(line) ? `#${line}` : line
    ).join('\n');
  }

  /**
   * Returns the content of `main.py` file. A special function named
   * `PUPA_PROXY_FUNCTION_NAME` is defined here to delegate all requests to
   * the Python function from the Colab. It will set the global variable, pass
   * the arguements to that function and returns the result as a property named
   * `result` in a JSON object.
   *
   * @return {string}
   */
  getMainFile() {
    return `# Copyright 2023 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import flask
import inspect
import functions_framework
import ${PUPA_COLAB_PACKAGE}

@functions_framework.http
def ${PUPA_PROXY_FUNCTION_NAME}(request: flask.Request) -> flask.typing.ResponseReturnValue:
    request_json = request.get_json(silent=True)
    if request_json and "args" in request_json:
        if "vars" in request_json:
          for key in request_json["vars"]:
            setattr(${PUPA_COLAB_PACKAGE}, key , request_json["vars"][key])
        if "functionName" in request_json:
          functionName = request_json["functionName"]
          fn = getattr(${PUPA_COLAB_PACKAGE}, functionName)
        else:
          functions = inspect.getmembers(${PUPA_COLAB_PACKAGE}, predicate=inspect.isfunction)
          if(len(functions) > 1):
            return "Unspecified function name", 400
          fn = functions[0][1]
        return {"result": fn(*request_json["args"])}
    else:
        return "Bad request", 400
`
  }

  /**
   * Returns `requirements.txt` file content at dependencies. It will extract
   * all `!pip` commands in Python code, no matter it is commented or not, as
   * the libraries that need to be specified.
   * In Colab, the libraries might be installed in other code blocks (not
   * the deployed one). In this situation, a commented `!pip` should be placed
   * in this deployed code block to help generate a correct `requirements.txt`
   * file.
   * @return {string}
   */
  getRequirementsFile(sourceCode = this.sourceCode) {
    const libraries = ['functions-framework'];
    const regex = /^#? *! *pip *install (?:-[^ ]+ +)* *(.+)$/gm;
    let lib;
    while ((lib = regex.exec(sourceCode)) !== null) {
      libraries.push(lib[lib.length - 1]);
    }
    return libraries.join('\n');
  }

  /**
   * Gets the enviroment variables for Cloud Functions.
   * @param {object} variables Other env variables for Cloud Functions.
   * @return
   * @private
   */
  getEnvironmentVariables(variables = {}) {
    return Object.assign({}, {
      DEPLOYED_BY: 'cyborg'
    }, variables);
  }

  /**
   * Returns the deployed Python code. This is used to check whether Colab
   * code has been changed since it is deployed as a Cloud Function.
   * @param {string} functionName The name of the deployed Cloud Function.
   * @return {string} The source code of the Python function.
   */
  downloadSourceCode(functionName) {
    try {
      const blobs = this.cloudFunctions.getSourceCode(functionName);
      if (blobs.error) {
        return { error: blobs.error };
      }
      const blob = blobs.filter(
        (blob) => blob.getName() === `${PUPA_COLAB_PACKAGE}.py`)[0];
      if (!blob) {
        return {
          error: {
            message: `Fail to find source code: ${PUPA_COLAB_PACKAGE}.py`,
          }
        };
      }
      return blob.getDataAsString();
    } catch (error) {
      return { error };
    }
  }

}
