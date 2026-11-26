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
 * Resolves the required IAM role for a Cloud Logging sink writer identity
 * based on the target destination URI.
 * @private
 * @param {string=} destination Target sink destination URI.
 * @return {string} Corresponding IAM role string.
 */
MojoFunctions.resolveLoggingSinkIamRole_ = (destination = '') => {
  const destStr = String(destination).toLowerCase();
  if (destStr.includes('bigquery.googleapis.com')) {
    return 'roles/bigquery.dataEditor';
  }
  if (destStr.includes('pubsub.googleapis.com')) {
    return 'roles/pubsub.publisher';
  }
  if (destStr.includes('storage.googleapis.com')) {
    return 'roles/storage.objectCreator';
  }
  return '';
};
/**
 * Combined one-step check and idempotent update/create for Cloud Logging Sinks.
 * Verifies current sink configuration, creates or updates immediately if
 * needed, and automatically grants required IAM roles to the sink's
 * writerIdentity deduced from the destination URI.
 * @param {string} sinkName The identifier for the Cloud Logging Sink.
 * @param {!Object} resources The resources object containing attributesMap.
 * @return {!CheckResult} Standardized status, descriptive outcome message, and
 *     console link.
 */
MojoFunctions.checkOrUpdateLogsRouter = (sinkName, resources) => {
  const projectId = getDocumentProperty('projectId');
  const logging = new Logging(projectId);
  const statusLink = logging.getLinkToCloudConsole(sinkName);

  const {attributesMap} = resources;
  const expectedPayload = MojoFunctions.buildPayloadFromAttributes(
    attributesMap,
    Logging.ATTRIBUTE_NAMES,
  );
  const requiredRole = MojoFunctions.resolveLoggingSinkIamRole_(
    expectedPayload.destination,
  );

  return MojoFunctions.reconcileDeclarativeResource(
    'Cloud Logging Sink',
    logging.getSink(sinkName),
    targetPayload => logging.createOrUpdateSink(sinkName, targetPayload),
    expectedPayload,
    statusLink,
    res =>
      MojoFunctions.grantIamRoleIfNeeded(
        projectId,
        res.writerIdentity,
        requiredRole,
      ),
  );
};

MojoTemplateRegistry.register('logRouter', {
  category: 'Google Cloud',
  resource: 'Log Router',
  value: '${namespace}_log',
  attributes: [
    {
      attributeName: Logging.ATTRIBUTE_NAMES.destination,
    },
    {
      attributeName: Logging.ATTRIBUTE_NAMES.filter,
    },
  ],
  checkFn: MojoFunctions.checkOrUpdateLogsRouter,
  groupType: GROUP_TYPE.COLLAPSE,
});
