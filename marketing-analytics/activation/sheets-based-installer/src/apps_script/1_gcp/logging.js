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

/** @fileoverview Cloud Logging API handler class. */

const WRITER_PERMISSION = {
  bigqueryOptions: 'roles/bigquery.dataEditor',
};

class Logging extends GcpApiBase {
  /**
   * @param {string=} projectId
   */
  constructor(projectId) {
    super();
    this.name = 'Cloud Logging API';
    this.api = 'logging.googleapis.com';
    this.permissions = ['logging.sinks.create', 'logging.sinks.get'];
    this.adminRole = 'Logs Configuration Writer';
    this.runtimeRoles = ['roles/logging.logWriter'];
    this.apiUrl = 'https://logging.googleapis.com';
    this.version = 'v2';
    this.projectId = projectId;
  }

  /**
   * @override
   * @return {string}
   */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /**
   * Gets a sink.
   * @see https://cloud.google.com/logging/docs/reference/v2/rest/v2/sinks/get
   * @param {string} sinkName
   * @return {!Object}
   */
  getSink(sinkName) {
    return this.get(`projects/${this.projectId}/sinks/${sinkName}`);
  }

  /**
   * Returns whether a Log Sink with the given name exists.
   * @param {string} sinkName
   * @return {boolean}
   */
  existSink(sinkName) {
    const response = this.getSink(sinkName);
    if (response.name) return true;
    if (
      response.error &&
      (response.error.code === 404 || response.error.status === 'NOT_FOUND')
    ) {
      return false;
    }
    console.error(`Unknown status of Log Sink ${sinkName}`, response);
    throw new Error(`Unknown status of Log Sink ${sinkName}`);
  }

  /**
   * Creates a sink that exports specified log entries to a destination.
   * @see https://cloud.google.com/logging/docs/reference/v2/rest/v2/sinks/create
   * @param {string} sinkName
   * @param {!Object} sinkPayload Declarative LogSink definition payload.
   * @return {!Object} Created LogSink resource.
   */
  createSink(sinkName, sinkPayload) {
    const payload = Object.assign({name: sinkName}, sinkPayload);
    console.log('Creating Log Sink', sinkName, JSON.stringify(payload));
    return this.mutate(
      `projects/${this.projectId}/sinks?uniqueWriterIdentity=true`,
      payload,
    );
  }

  /**
   * Updates a sink.
   * @see https://cloud.google.com/logging/docs/reference/v2/rest/v2/sinks/update
   * @param {string} sinkName
   * @param {!Object} sinkPayload Declarative LogSink definition payload.
   * @return {!Object} Updated LogSink resource.
   */
  updateSink(sinkName, sinkPayload) {
    const payload = Object.assign({name: sinkName}, sinkPayload);
    const maskFields = Object.keys(sinkPayload)
      .filter(key => key !== 'name')
      .join(',');
    const updateMask = encodeURIComponent(
      maskFields || 'destination,filter,bigqueryOptions',
    );
    console.log('Updating Log Sink', sinkName, JSON.stringify(payload));
    return this.mutate(
      `projects/${this.projectId}/sinks/${
        sinkName
      }?uniqueWriterIdentity=true&updateMask=${updateMask}`,
      payload,
      'PUT',
    );
  }

  /**
   * Creates or updates a Log Sink idempotently.
   * @param {string} sinkName
   * @param {!Object} sinkPayload
   * @return {!Object} Resolved LogSink resource.
   */
  createOrUpdateSink(sinkName, sinkPayload) {
    if (this.existSink(sinkName)) {
      console.log('Updating existing Log Sink:', sinkName);
      return this.updateSink(sinkName, sinkPayload);
    }
    console.log('Log Sink not found, creating:', sinkName);
    return this.createSink(sinkName, sinkPayload);
  }

  /**
   * Writes log entries to Logging.
   * @see https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/write
   * @param {string} logName
   * @param {!Object} jsonPayload
   * @param {!Object=} options
   * @return {!Object}
   */
  writeMessage(logName, jsonPayload, options = {}) {
    const entryOption = Object.assign(
      {resource: {type: 'global'}, severity: 'INFO'},
      options,
      {logName: `projects/${this.projectId}/logs/${logName}`},
    );
    const entries = [Object.assign({}, entryOption, {jsonPayload})];
    return this.mutate('entries:write', {entries});
  }

  /**
   * Returns a direct link to the Log Sink Router page in Google Cloud Console.
   * @param {string=} sinkName
   * @return {string}
   */
  getLinkToCloudConsole(sinkName) {
    return `https://console.cloud.google.com/logs/router?project=${
      this.projectId
    }`;
  }
}

Logging.ATTRIBUTE_NAMES = Object.freeze({
  destination: 'Destination',
  filter: 'Log Filter',
  'bigqueryOptions.usePartitionedTables': 'Use Partitioned Tables',
  'bigqueryOptions.usesTimestampColumnPartitioning':
    'Use Timestamp Column Partitioning',
});
