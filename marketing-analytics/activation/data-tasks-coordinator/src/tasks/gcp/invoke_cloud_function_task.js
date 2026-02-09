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

/**
 * @fileoverview Invoke a Pupa Cloud Functions task class.
 */

'use strict';

const {
  cloudfunctions: { getIdTokenForFunction },
  utils: { requestWithRetry }
} = require('@google-cloud/nodejs-common');
const { BaseTask } = require('../base_task.js');

//XXX: Copied this definition from Tentacles 'CloudFunctionInvoke' handler.
/**
 * A Cloud Function source. The Cloud Functions here are HTTP Cloud Functions
 * deployed by solution Pupa for Python functions defined in a Colab.
 * The request has a fixed data structure including properties:
 *   `functionName`, the name of Python function that will be invoked if there
 *      are more than one functions;
 *   `args`, an array of arguments for the target Python function;
 *   `vars`, an object of global variables for the Python function.
 * @typedef {{
 *   url:string,
 *   functionName:string|undefined,
 *   args:Array<Object>|undefined,
 *   vars:Object|undefined,
 * }}
 */
let CloudFunctionEndpoint;

/**
 * The configuration object for InvokeCloudFunctionTask.
 *
 * @typedef {{
*   type:TaskType.INVOKE_CLOUD_FUNCTION,
*   service: !CloudFunctionEndpoint,
*   errorOptions:(!ErrorOptions|undefined),
*   appendedParameters:(Object<string,string>|undefined),
*   next:(!TaskGroup|undefined),
* }}
*/
let InvokeCloudFunctionConfig;

/**
 * Invoke Cloud Functions task.
 * This task is usually used to handle a single, sometime heavy, invocation of
 * a Pupa Cloud Function, e.g. doing a prediction. For those have a batch of
 * data(requests) to a Pupa CF, the SpeedControlledTask should be used instead.
 * Due to the same reason, `recordsPerRequest` is not support in this task,
 * which means there is no feature to support merging multiple lines(records)
 * as one request. SpeedControlledTask supports this.
 * @see SpeedControlledTask
 */
class InvokeCloudFunctionTask extends BaseTask {

  /** @constructor */
  constructor(config, parameters, defaultProject = undefined) {
    super(config, parameters, defaultProject);
  }

  /** @override */
  async isDone() {
    return true;
  }

  /** @override */
  async doTask() {
    try {
      const { url, args, vars, functionName } = this.config.service;
      const token = await getIdTokenForFunction(url);
      const options = {
        url,
        headers: { Authorization: `bearer ${token}` },
      }
      //See Tentacles API handler CloudFunctionInvoke as a reference.
      options.data = { args, vars, functionName };
      const responseData = await requestWithRetry(options, this.logger);
      return {
        parameters: this.appendParameter({ responseData }),
      }
    } catch (error) {
      this.logger.error(`CF invoke failed:`, error);
      throw error;
    }
  }
}

module.exports = {
  InvokeCloudFunctionTask,
  InvokeCloudFunctionConfig,
};
