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

class Logging extends ApiBase {

  constructor(projectId) {
    super();
    this.apiUrl = 'https://logging.googleapis.com';
    this.version = 'v2';
    this.projectId = projectId;
  }

  /** @override */
  getBaseUrl() {
    return `${this.apiUrl}/${this.version}`;
  }

  /**
   * Gets a sink.
   * @see https://cloud.google.com/logging/docs/reference/v2/rest/v2/sinks/get
   * @param {string} sinkName
   * @return {!LogSink}
   * @see https://cloud.google.com/logging/docs/reference/v2/rest/v2/billingAccounts.sinks#LogSink
   */
  getSink(sinkName) {
    return super.get(`projects/${this.projectId}/sinks/${sinkName}`);
  }

  /**
   * Creates a sink that exports specified log entries to a destination.
   * @see https://cloud.google.com/logging/docs/reference/v2/rest/v2/sinks/create
   * @param {string} sinkName
   * @param {!LogSink} options
   * @return {!LogSink}
   */
  createSink(sinkName, options) {
    const payload = {
      name: sinkName,
      filter: options.filter,
    }
    if (options.dataset) {
      payload.destination =
        `bigquery.googleapis.com/projects/${this.projectId}/datasets/${options.dataset}`;
      payload.bigqueryOptions = { usePartitionedTables: true };
    } else if (options.topic) {
      payload.destination =
        `pubsub.googleapis.com/projects/${this.projectId}/topics/${options.topic}`;
    }
    return this.mutate(
      `projects/${this.projectId}/sinks?uniqueWriterIdentity=true`, payload);
  }

  //TODO: Update this function to make it more flexible. Currently, it
  // only updates 'filter' and 'destination'.
  /**
   * Updates a sink.
   * @see https://cloud.google.com/logging/docs/reference/v2/rest/v2/sinks/update
   * @param {string} sinkName
   * @param {object} options
   * @return {!LogSink}
   */
  updateSink(sinkName, options) {
    const payload = {
      filter: options.filter,
    }
    if (options.dataset) {
      payload.destination =
        `bigquery.googleapis.com/projects/${this.projectId}/datasets/${options.dataset}`;
      payload.bigqueryOptions = { usePartitionedTables: true };
    } else if (options.topic) {
      payload.destination =
        `pubsub.googleapis.com/projects/${this.projectId}/topics/${options.topic}`;
    }
    return this.mutate(
      `projects/${this.projectId}/sinks/${sinkName}?uniqueWriterIdentity=true&updateMask=filter%2Cdestination`,
      payload, 'PUT');
  }

  /**
   * Writes log entries to Logging.
   * @see https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/write
   * @param {string} logName
   * @param {object} jsonPayload
   * @param {object} options
   * @return
   */
  writeMessage(logName, jsonPayload, options = {}) {
    const entryOption = Object.assign(
      { resource: { type: 'global' }, severity: 'INFO', },
      options,
      { logName: `projects/${this.projectId}/logs/${logName}` }
    );
    const entries = [
      Object.assign({}, entryOption, { jsonPayload }),
    ];
    return this.mutate('entries:write', { entries });
  }
}
