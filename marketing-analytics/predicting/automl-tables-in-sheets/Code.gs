/**
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


const AUTH_SCOPES = [
  // GCP scope is required for performing predictions with AutoML Tables.
  'https://www.googleapis.com/auth/cloud-platform',
];
const AUTH_URL_BASE = 'https://accounts.google.com/o/oauth2/auth';
const AUTH_URL_TOKEN = 'https://accounts.google.com/o/oauth2/token';
const SCRIPT_PROPERTIES = PropertiesService.getScriptProperties();
// Make sure that a service account key with the "AutoML Predictor" role
// is stored in this Apps Script's properties under the key "serviceAccountKey".
const SERVICE_ACCOUNT_KEY = JSON.parse(
  SCRIPT_PROPERTIES.getProperty('serviceAccountKey')
);


/**
 * Returns AutoML Tables's prediction for a row's target feature value.
 *
 * Performs an online prediction with a trained & deployed AutoML Tables model.
 *
 * @param {cells} inputs  The non-target feature values for a row,
 *     in the same order as in the AutoML Tables dataset's schema.
 *     Example: Predictions!B4:K4
 * @param {string} modelId The ID of the AutoML Tables model.
 *     Example: TBL12345678901234567890
 * @return {Array} AutoML Tables's prediction & confidence
 *     for the row's target feature value.
 * @customfunction
 */
function predict(inputs, modelId) {
  console.log('inputs:', inputs);
  console.log('modelId:', modelId);
  const oauthService = createOauthService();
  if (!oauthService.hasAccess()) {
    throw new Error(`OAuth Error: ${oauthService.getLastError()[0]}`);
  }
  const oauthToken = oauthService.getAccessToken();
  const projectId = SERVICE_ACCOUNT_KEY.project_id;
  const row = inputs[0];
  const apiUrl = (
    `https://automl.googleapis.com/v1beta1/projects/${projectId}` +
    `/locations/us-central1/models/${modelId}:predict`
  );
  const apiRequest = {
    contentType: 'application/json',
    headers: {Authorization: `Bearer ${oauthToken}`},
    method: 'post',
    payload: JSON.stringify({payload: {row: {values: row}}}),
  };
  const apiResponse = UrlFetchApp.fetch(apiUrl, apiRequest);
  const apiResponseContent = JSON.parse(apiResponse.getContentText());
  const results = apiResponseContent.payload.map((x) => x.tables);
  results.sort((a, b) => b.score - a.score);
  const topResult = results[0];
  const prediction = topResult.value;
  const confidence = topResult.score.toLocaleString('en', {style: 'percent'});
  console.log('prediction:', prediction);
  console.log('confidence:', confidence);
  return [[prediction, confidence]];
}


/**
 * Creates an OAuth2 Service for authenticating GCP API requests.
 *
 * Adapted from https://github.com/gsuitedevs/apps-script-oauth2/blob/master/README.md
 *
 * @return {Service} The created OAuth2.Service.
 */
function createOauthService() {
  return OAuth2.createService('OAuth Service')
    .setAuthorizationBaseUrl(AUTH_URL_BASE)
    .setTokenUrl(AUTH_URL_TOKEN)
    .setPrivateKey(SERVICE_ACCOUNT_KEY.private_key)
    .setIssuer(SERVICE_ACCOUNT_KEY.client_email)
    .setSubject(SERVICE_ACCOUNT_KEY.client_email)
    .setPropertyStore(SCRIPT_PROPERTIES)
    .setScope(AUTH_SCOPES.join(' '));
}
