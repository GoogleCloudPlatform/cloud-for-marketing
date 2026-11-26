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

/** @fileoverview Base class for Google Cloud Platform control-plane APIs. */

class GcpApiBase extends ApiBase {
  /**
   * Lists information about the supported locations for this service.
   * @see https://cloud.google.com/functions/docs/reference/rest/v2/projects.locations/list
   * @see https://cloud.google.com/functions/docs/reference/rest/Shared.Types/ListLocationsResponse#Location
   * @param {string=} version API version to use, default is the class version.
   * @return {!Array<!Location>} An array of resources that represents Google
   *   Cloud Platform location.
   */
  listLocations(version = this.version) {
    const locations = this.list(
      'locations',
      `${this.apiUrl}/${version}/projects/${this.projectId}/locations`,
    );
    return locations;
  }
  /**
   * Retrieves the latest status of a Google Cloud Long-Running Operation.
   * Resolves operations against the API service root URL regardless of relative
   * or fully qualified path prefixes returned by the backend.
   * @param {string} operationName Operation resource identifier.
   * @return {!Object} Operation status response.
   */
  getOperation(operationName) {
    const rootUrl = `${this.apiUrl}/${this.version}`;
    console.log('Polling GCP Operation:', operationName, 'against', rootUrl);
    return this.get(`${rootUrl}/${operationName}`);
  }

  /**
   * Polls a Google Cloud Long-Running Operation (LRO) until completion.
   * Universal LRO resolver inherited by all GCP resource clients.
   * @param {!Object} operation Initial operation object containing operation
   *     name.
   * @param {string=} resourceLabel Descriptive tag for execution logging.
   * @param {number=} intervalMs Polling interval duration in milliseconds,
   *     default 5000ms.
   * @param {number=} timeoutMs Maximum wait time before throwing timeout
   *     exception, default 240000ms.
   * @return {!Object} Unwrapped target resource response object.
   */
  waitAndResolveOperation(
    operation,
    resourceLabel = 'resource',
    intervalMs = 5000,
    timeoutMs = 240000,
  ) {
    if (!operation || !operation.name) {
      return {
        error:
          (operation && operation.error) ||
          new Error(
            `Invalid operation returned when mutating GCP ${resourceLabel}`,
          ),
      };
    }
    let rawResponse = this.getOperation(operation.name);
    let elapsedMs = 0;
    while (rawResponse.done !== true && !rawResponse.error) {
      if (elapsedMs >= timeoutMs) {
        return {
          error: new Error(
            `Operation timed out after ${timeoutMs}ms while polling ${
              operation.name
            } (${resourceLabel})`,
          ),
        };
      }
      Utilities.sleep(intervalMs);
      elapsedMs += intervalMs;
      rawResponse = this.getOperation(operation.name);
      console.log(
        'Waiting for GCP Operation completion:',
        operation.name,
        `(${elapsedMs}ms)`,
      );
    }
    console.log(`Completed GCP Operation for ${resourceLabel}`, rawResponse);
    if (rawResponse.error) {
      return {error: rawResponse.error};
    }
    return rawResponse.response || rawResponse;
  }
}

/**
 * Registry for self-registering GCP service provider classes.
 * Allows dynamic discovery and instantiation of services based on asset type.
 */
const ServiceProviderRegistry = {
  _providers: [],

  /**
   * Registers a service provider class.
   * @param {function(new:GcpApiBase, string=, string=)} providerClass
   */
  register(providerClass) {
    this._providers.push(providerClass);
  },

  /**
   * @return {!Array<string>} List of registered asset types.
   */
  getRegisteredAssetTypes() {
    return this._providers.map(cls => cls.ASSET_TYPE);
  },

  /**
   * @param {string} assetType The GCP asset type.
   * @param {string=} projectId
   * @param {string=} locationId
   * @return {!GcpApiBase|null} An instance of the matching provider, or null.
   */
  createProvider(assetType, projectId, locationId) {
    const Cls = this._providers.find(cls => cls.ASSET_TYPE === assetType);
    return Cls ? new Cls(projectId, locationId) : null;
  },

  /**
   * @param {string=} projectId
   * @return {!GcpApiBase|null} An instance of the first available provider.
   */
  getFirstAvailable(projectId) {
    if (this._providers.length > 0) {
      const Cls = this._providers[0];
      return new Cls(projectId);
    }
    return null;
  },
};
