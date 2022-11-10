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
 * @fileoverview Common utility functions for API handlers.
 */

'use strict';
const {
  api: { googleads: { GoogleAds } },
} = require('@google-cloud/nodejs-common');

/**
 * Gets option object to create a new API object.
 * By default, the API classes will figure out the authorization from env
 * variables. The authorization can also be set in the 'config' so each
 * integration can have its own authorization. This function is used to get the
 * authorization related information from the 'config' object and form an
 * 'option' object for the API classes.
 *
 * @param {{secretName:(string|undefined)}} config The connector configuration.
 * @return {{SECRET_NAME:(string|undefined)}}
 */
const getOption = (config) => {
  const options = {};
  if (config.secretName) options.SECRET_NAME = config.secretName;
  return options;
}

/**
 * Gets the boolean value of 'debug' in config.
 * When 'debug' is set with a non-boolean value, it returns ture only if it is
 * a string 'true'.
 * @param {(boolean|string|undefined)} debug The 'debug' value in 'config'.
 * @return {boolean}
 */
const getDebug = (debug) => {
  return typeof debug === 'boolean' ? debug : debug === 'true';
}

/**
 * Gets an instances of GoogleAds class. Google Ads API is used in multiple
 * connectors, including:
 * 1. Click Conversions upload
 * 2. Call Conversions upload
 * 3. Enhanced Conversions upload
 * 4. Customer match user data upload
 * 5.
 *
 * , so put the function here for reuse.
 * @param {{
 *   developerToken:string,
 *   debug:(boolean|undefined),
 *   secretName:(string|undefined),
 * }} config The Google Ads API based connectors configuration.
 * @return {!GoogleAds}
 */
const getGoogleAds = (config) => {
  const { developerToken, debug } = config;
  return new GoogleAds(developerToken, getDebug(debug), getOption(config));
}

module.exports = {
  getOption,
  getDebug,
  getGoogleAds,
};
