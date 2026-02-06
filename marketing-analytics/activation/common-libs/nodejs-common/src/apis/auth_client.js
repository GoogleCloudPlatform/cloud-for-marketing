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
const {
  GoogleAuth,
  OAuth2Client,
  UserRefreshClient,
  JWT,
  Compute,
} = require('google-auth-library');
const { SecretManager } = require('../components/secret_manager.js');
const { getLogger } = require('../components/utils.js');

/** Environment variable name for Secret Manager secret name. */
const DEFAULT_ENV_SECRET = 'SECRET_NAME';
/** Environment variable name for OAuth2 token file location. */
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
 * 2. User-managed service account is required for external APIs for some
 * specific considerations, e.g. security. In this case, a file based key file
 * can be used to generate a JWT auth client.
 *
 * To solve these challenges, this class tries to probe the settings from
 * environment variables, starts from the name of secret (Secret Manager), OAuth
 * token file (deprecated), then service account key file (deprecated). It will
 * fallback to ADC if those probing failed.
 * Note, Secret Manager is the recommended way to store tokens because it is a
 * secure and convenient central storage system to manage access across Google
 * Cloud.
 *
 * The recommended environment variable is:
 * SECRET_NAME: the name of secret. The secret can be a oauth token file or a
 * service account key file. This env var is used to offer a global auth for a
 * solution. If different authentications are required, the value of passed
 * `env` can be set by the runtime.
 *
 * Alternative environment variable but not recommended for prod environment:
 * OAUTH2_TOKEN_JSON : the oauth token key files, refresh token and proper API
 * scopes are expected here.
 * API_SERVICE_ACCOUNT : the service account key file. The email in the key file
 * is expected to have the corresponding access in the target system, e.g. CM.
 */
class AuthClient {
  /**
   * Create a new instance with given API scopes.
   * @param {string|!Array<string>|!ReadonlyArray<string>} scopes
   * @param {!Object<string,string>=} overwrittenEnv The key-value pairs to
   *     over write env variables.
   */
  constructor(scopes, overwrittenEnv = {}) {
    this.logger = getLogger('AUTH');
    this.scopes = scopes;
    this.env = Object.assign({}, process.env, overwrittenEnv);
    this.initialized = false;
  }

  /**
   * Prepares the `oauthToken` object and/or `serviceAccountKey` based on the
   * settings in environment object.
   * A secret name is preferred to offer the token of the OAuth or key of a
   * service account.
   * To be compatible, this function also checks the env for oauth token file
   * and service account key file if there is no secret name was set in the env.
   */
  async prepareCredentials() {
    if (this.initialized === true) {
      this.logger.info(`This authClient has been initialized.`);
      return;
    }
    if (this.env[DEFAULT_ENV_SECRET]) {
      const secretManager = new SecretManager({
        projectId: this.env.GCP_PROJECT,
      });
      const secret = await secretManager.access(this.env[DEFAULT_ENV_SECRET]);
      if (secret) {
        const secretObj = JSON.parse(secret);
        if (secretObj.token) this.oauthToken = secretObj;
        else this.serviceAccountKey = secretObj;
        this.logger.info(`Get secret from SM ${this.env[DEFAULT_ENV_SECRET]}.`);
      } else {
        this.logger.warn(`Cannot find SM ${this.env[DEFAULT_ENV_SECRET]}.`);
      }
    } else {// To be compatible with previous solution.
      const oauthTokenFile = this.getContentFromEnvVar(DEFAULT_ENV_OAUTH);
      if (oauthTokenFile) {
        this.oauthToken = JSON.parse(oauthTokenFile);
      }
      const serviceAccountKeyFile =
        this.getContentFromEnvVar(DEFAULT_ENV_KEYFILE);
      if (serviceAccountKeyFile) {
        this.serviceAccountKey = JSON.parse(serviceAccountKeyFile);
      }
    }
    this.initialized = true;
  }

  /**
   * Factory method to offer a prepared AuthClient instance in an async way.
   * @param {string|!Array<string>|!ReadonlyArray<string>} scopes
   * @param {!Object<string,string>=} env The environment object to hold env
   *     variables.
   * @return {!Promise<!AuthClient>}
   */
  static async build(scopes, env) {
    const instance = new AuthClient(scopes, env);
    await instance.prepareCredentials();
    return instance;
  }

  /**
   * Generates an authentication client of OAuth, JWT or ADC based on the
   * environment settings. The authentication method is determined by the type
   * of available credentials:
   * 1. OAuth, return an OAuth token if available.
   * 2. JWT, return JWT client if a service account key is available.
   * 3. ADC if none of these files exists.
   * @return {!UserRefreshClient|!JWT|!Compute}
   */
  getDefaultAuth() {
    if (typeof this.oauthToken !== 'undefined') {
      return this.getOAuth2Client();
    } else if (typeof this.serviceAccountKey !== 'undefined') {
      return this.getServiceAccount();
    } else {
      return this.getApplicationDefaultCredentials();
    }
  }

  /**
   * Returns a GoogleAuth client object by ADC. Here are the ADC checking
   * steps:
   * 1. Checks environment variable GOOGLE_APPLICATION_CREDENTIALS to get
   * service account. Returns a JWT if it exists;
   * 2. Uses default service account of Computer Engine/AppEngine/Cloud
   * Functions
   * 3. Otherwise, an error occurs.
   * @see https://cloud.google.com/docs/authentication/production#obtaining_and_providing_service_account_credentials_manually
   * @return {!Compute|!JWT}
   */
  getApplicationDefaultCredentials() {
    this.logger.info(`Mode ADC`);
    return new GoogleAuth({scopes: this.scopes});
  }

  /**
   * Returns an OAuth2 client based on the given key file.
   * @return {!UserRefreshClient}
   */
  getOAuth2Client() {
    this.ensureCredentialExists_(this.oauthToken, 'OAuth token');
    const { client_id, client_secret, token: { refresh_token } }
      = this.oauthToken;
    const oAuth2Client =
      new UserRefreshClient(client_id, client_secret, refresh_token);
    return oAuth2Client;
  }

  /**
   * Returns a JWT client based on the given service account key file.
   * @return {!JWT}
   */
  getServiceAccount() {
    this.ensureCredentialExists_(this.serviceAccountKey, 'Service Account key');
    this.logger.info(`Mode JWT`);
    const { private_key_id, private_key, client_email } = this.serviceAccountKey;
    return new JWT({
      email: client_email,
      key: private_key,
      keyId: private_key_id,
      scopes: this.scopes,
    });
  }

  /**
   * Returns an object contains client_id, client_secret, refresh_token of
   * OAuth2 based on the given key file.
   * Some API library (google-ads-api） doesn't support google-auth-library
   * directly and needs plain keys.
   * @return {{
   *   clientId:string,
   *   clientSecret:string,
   *   refreshToken:string,
   * }}
   */
  getOAuth2Token() {
    this.ensureCredentialExists_(this.oauthToken, 'OAuth token');
    this.logger.info(`Mode OAUTH`);
    const { client_id, client_secret, token } = this.oauthToken;
    return {
      clientId: client_id,
      clientSecret: client_secret,
      refreshToken: token.refresh_token,
    };
  }

  /**
   * Some APIs only support one authorization type, so we have to designate a
   * specific auth method rather than the 'getDefaultAuth'. If the related
   * key/token is not available, the auth should fail. The function throws
   * errors with a meaningful message.
   * @param {object} credential - Credential object.
   * @param {string} type Key type name, 'OAuth token' or 'Service Account key'
   * @private
   */
  ensureCredentialExists_(credential, type) {
    if (!credential) throw new Error(`Required ${type} does not exist.`);
  }

  /**
   * Returns the content of a file whose path is set as an env variable.
   * The path can be relative or absolute. The function will append current path
   * before the relative path.
   * @param {string} varName The name of environment variable for the file path.
   * @return {?string} Content of the file what set as an environment variable.
   */
  getContentFromEnvVar(varName) {
    const value = this.env[varName];
    if (value) {
      const fullPath = value.startsWith('/')
        ? value
        : path.join(__dirname, value);
      if (fs.existsSync(fullPath)) {
        this.logger.info(`Find file '${fullPath}' as [${varName}] in env.`);
        return fs.readFileSync(fullPath).toString();
      } else {
        this.logger.error(
          `Cannot find '${fullPath}' which is as env[${varName}].`
        );
      }
    }
  }
}

module.exports = AuthClient;
