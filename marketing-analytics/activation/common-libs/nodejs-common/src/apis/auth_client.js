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
 * @fileoverview This is a Google APIs Auth helper class based on Google auth
 * library.
 */

'use strict';

const fs = require('fs');
const path = require('path');
const {GoogleAuth, OAuth2Client, JWT, Compute} = require('google-auth-library');
/** Environment variable name for OAuth2 key file location. */
const DEFAULT_ENV_OAUTH = 'OAUTH2_TOKEN_JSON';
/** Environment variable name for Service account key file location. */
const DEFAULT_ENV_KEYFILE = 'API_SERVICE_ACCOUNT';

/**
 * Google APIs Auth helper class based on Google auth library which supports
 * Google Application Default Credentials (ADC), OAuth, JSON Web Token (JWT),
 * Computer. ADC fallbacks to JWT on local machine and Computer auth in Cloud.
 * In most cases, ADC is the ideal authentication choice because it doesn't
 * require extra setting in codes. Other authentication methods, e.g. OAuth, JWT
 * will need specific setting to initiate an auth instance.
 *
 * There are two use cases for this authentication helper class:
 * 1. The user only has OAuth access due to some reasons, so ADC can't be used;
 * 2. ADC doesn't work for some external APIs. User-managed service account is
 * the only option. We have to manually initiate the Authentication object by
 * specifying the key files.
 *
 * To solve these challenges, this class tries to probe the OAuth  key file,
 * then service account key file based on the environment variables. It will
 * fallback to ADC if those probing failed.
 *
 * The expected environment variables are:
 * OAUTH2_TOKEN_JSON : the oauth token key files, refresh token and proper API
 * scopes are expected here.
 * API_SERVICE_ACCOUNT : the service account key file. The email in the key file
 * is expected to have the corresponding access in the target system, e.g. CM.
 */
class AuthClient {
  /**
   * Create a new instance with given API scopes.
   * @param {string|!Array<string>|!ReadonlyArray<string>} scopes
   */
  constructor(scopes) {
    this.scopes = scopes;
    this.oauthTokenFile = getFullPathForEnv(DEFAULT_ENV_OAUTH);
    this.serviceAccountKeyFile = getFullPathForEnv(DEFAULT_ENV_KEYFILE);
  }

  /**
   * Generates an authentication client of OAuth, JWT or ADC based on the
   * environment settings. The authentication method is determined by
   * environment variables at runtime. The priorities for different
   * authentications are:
   * 1. OAuth, return an OAuth client if there is a OAuth key file available.
   * 2. JWT, return JWT client if a user managed service account key file is
   * available.
   * 3. ADC if none of these files exists.
   * @return {!OAuth2Client|!JWT|!Compute}
   */
  getDefaultAuth() {
    if (typeof this.oauthTokenFile !== 'undefined') {
      console.log(`Auth mode OAUTH: ${this.oauthTokenFile}`);
      return this.getOAuth2Client();
    } else if (typeof this.serviceAccountKeyFile !== 'undefined') {
      console.log(`Auth mode JWT: ${this.serviceAccountKeyFile}`);
      return this.getServiceAccount();
    } else {
      console.log(`Auth mode ADC`);
      return this.getApplicationDefaultCredentials();
    }
  }

  /**
   * Returns a GoogleAuth client object by ADC. Here are the ADC checking
   * steps:
   * 1. Checks environment variable GOOGLE_APPLICATION_CREDENTIALS to get
   * service account. Returns a JWT if it exists;
   * 2. Uses default service account of Computer Engine/AppEngige/Cloud
   * Functions
   * 3. Otherwise, an error occurs.
   * @see https://cloud.google.com/docs/authentication/production#obtaining_and_providing_service_account_credentials_manually
   * @return {!Compute|!JWT}
   */
  getApplicationDefaultCredentials() {
    return new GoogleAuth({scopes: this.scopes});
  }

  /**
   * Returns an OAuth2 client based on the given key file.
   * @param {string=} keyFile Full path for the OAuth key file.
   *     `client_id`, `client_secret`, `tokens` are expected in that JSON file.
   * @return {!OAuth2Client}
   */
  getOAuth2Client(keyFile = this.oauthTokenFile) {
    const key = JSON.parse(fs.readFileSync(keyFile).toString());
    console.log(`Get OAuth token with Email: ${key.client_id}`);
    const oAuth2Client = new OAuth2Client(key.client_id, key.client_secret);
    oAuth2Client.setCredentials(key.token);
    return oAuth2Client;
  }

  /**
   * Returns a JWT client based on the given service account key file.
   * @param {string=} keyFile Full path for the service account key file.
   * @return {!JWT}
   */
  getServiceAccount(keyFile = this.serviceAccountKeyFile) {
    console.log(`Get Service Account's key file: ${keyFile}`);
    return new JWT({keyFile, scopes: this.scopes,});
  }

  /**
   * Returns an object contains client_id, client_secret, refresh_token of
   * OAuth2 based on the given key file.
   * Some API library (google-ads-api） doesn't support google-auth-library
   * directly and needs plain keys.
   * @param {string=} keyFile Full path for the OAuth key file.
   *     `client_id`, `client_secret`, `tokens` are expected in that JSON file.
   * @return {{
   *   clientId:string,
   *   clientSecret:string,
   *   refreshToken:string,
   * }}
   */
  getOAuth2Token(keyFile = this.oauthTokenFile) {
    const key = JSON.parse(fs.readFileSync(keyFile).toString());
    return {
      clientId: key.client_id,
      clientSecret: key.client_secret,
      refreshToken: key.token.refresh_token,
    };
  }
}

/**
 * Returns the full path of a existent file whose path (relative or
 * absolute) is the value of the given environment variable.
 * @param {string} envName The name of environment variable for the file path.
 * @return {?string} Full path of the file what set as an environment variable.
 */
function getFullPathForEnv(envName) {
  if (typeof process.env[envName] === 'undefined') {
    console.log(`Env[${envName}] doesn't have a value.`);
  } else {
    const fullPath = process.env[envName].startsWith('/') ?
        process.env[envName] :
        path.join(__dirname, process.env[envName]);
    if (fs.existsSync(fullPath)) {
      console.log(`Find file '${fullPath}' set in env as [${envName}].`);
      return fullPath;
    } else {
      console.error(`Can't find '${fullPath}' which set in env[${envName}].`);
    }
  }
}

module.exports = AuthClient;
