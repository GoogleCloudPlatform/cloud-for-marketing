// Copyright 2022 Google Inc.
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

/** @fileoverview The mojo defnition of solution Sentinel. */

/** The configuration of a logs router for Sentinel main Cloud Functions. */
const SENTINEL_LOGS_ROUTER_SINK = Object.freeze({
  FILTER: 'resource.type="bigquery_resource" AND protoPayload.methodName="jobservice.jobcompleted"',
  ROLE: 'roles/pubsub.publisher',
});

/** Facilitator functions for MojoResources from Sentinel. */
const sentinel = {};

/**
 * Returns the Pub/Sub topic name of Sentinel main Cloud Functions.
 * @param {string} namespace
 * @return {string}
 */
sentinel.getMonitorTopicName = Sentinel.getMonitorTopicName;

/**
 * Returns the latest 10 versions of Sentinel.
 * @return {!Array<string>} The array of versions.
 */
sentinel.getVersions = () => getNpmVersions(SENTINEL_PACKAGE);

/**
 * Updates Sentinel recent 10 versions and set the value as the lastet if
 * there is no explicit version selected.
 * @param {string} version
 * @return
 */
sentinel.checkVersion = getCheckFunctionForVersion(SENTINEL_PACKAGE);

/**
 * Returns an array of Sentinel features based on an array of specified
 * codes. If codes were not specified, it returns all available features.
 * @param {!Array<string>|undefined} enabledFeatures
 * @return {!Array<string>}
 */
sentinel.getFeatureDesc = PrimeSolution.getFeatureDesc(SENTINEL_FEATURES);

/**
 * Checks related resources of a Sentinel feature.
 * It will try to enable the API.
 * @param {string} feature The description of the feature.
 * @return {!CheckResult}
 */
sentinel.checkFeature = (feature) => {
  const projectId = getDocumentProperty('projectId');
  const apiCheckFn =
    PrimeSolution.checkAndEnableApiForFeature(SENTINEL_FEATURES, projectId);
  const result = apiCheckFn(feature);
  if (result) return result;
  return {
    status: RESOURCE_STATUS.OK,
    message: 'ACTIVE',
  };
}

/**
 * Checks Sentinel Cloud Functions' status and version to indicate whether or
 * not need to deploy the Cloud Functions.
 * @param {string} functionName Name of the Cloud Functions
 * @return {!CheckResult}
 */
sentinel.checkCloudFunctions = (name) => {
  const expectedVersion = getDocumentProperty('sentinelVersion');
  return gcloud.checkCloudFunctions(name, expectedVersion);
}

/**
 * Deploys Sentinel Cloud Functions while keeps updating a Sheets Cell to show
 * the progress.
 * @param {string} functionName Name of the Cloud Functions
 * @return {!CheckResult}
 */
sentinel.installCloudFunctions = (functionName) => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const options = {
    projectId,
    locationId,
    namespace: getDocumentProperty('namespace'),
    version: getDocumentProperty('sentinelVersion'),
    databaseId: getDocumentProperty('databaseId', false),
    bucket: getDocumentProperty('sentinelBucket', false),
    inbound: getDocumentProperty('sentinelInbound', false),
    secretName: getDocumentProperty('defaultSecretName', false),
  }
  const operationName = new Sentinel(options).deployCloudFunctions(functionName);
  console.log(`Start to deploy ${functionName}`, operationName);
  return gcloud.getCloudFunctionDeployStatus(
    operationName, locationId, projectId);
}

/**
 * Checks and creates Log Router Sink which will filter information for
 * BigQuery job stauts.
 * @param {string} name
 * @return {!CheckResult}
 */
sentinel.checkOrCreateLogsRouter = (name) => {
  const namespace = getDocumentProperty('namespace');
  const topic = sentinel.getMonitorTopicName(namespace);
  const filter = SENTINEL_LOGS_ROUTER_SINK.FILTER;
  const options = { topic, filter };
  return gcloud.checkOrCreateLogsRouter(
    topic, options, SENTINEL_LOGS_ROUTER_SINK.ROLE);
}

/**
 * Checks and creates Sentinel internal schedule job.
 * @param {string} name
 * @param {Object} resource The property 'attributeValue' stands for the
 *   interval minutes.
 * @return {!CheckResult}
 */
sentinel.checkOrCreateInternalJob = (name, resource) => {
  const schedule = `*/${resource.attributeValue || 5} * * * *`;
  const taskId = 'system';
  const message = JSON.stringify({ intrinsic: 'status_check' });
  const namespace = getDocumentProperty('namespace');
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const timeZone = getDocumentProperty('timeZone', false);
  const topicName = sentinel.getMonitorTopicName(namespace);
  const pubsubTarget = {
    topicName: `projects/${projectId}/topics/${topicName}`,
    attributes: { taskId },
    data: Utilities.base64Encode(
      replaceVariables(message, getDocumentProperties())
    ),
  };
  const options = Object.assign(
    { timeZone: 'Etc/UTC' }, { schedule, timeZone, pubsubTarget });
  const cloudScheduler = new CloudScheduler(projectId, locationId);
  const job = cloudScheduler.createOrUpdateJob(name, options);
  if (job.error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: `Failed to create/update scheduler job: ${error.message}`,
    };
  }
  return { status: RESOURCE_STATUS.OK };
}

// Reusable Sentinel resource configuration for Mojo.
MOJO_CONFIG_TEMPLATE.sentinelPermissions = {
  template: 'permissions',
  value: [
    'cloudfunctions.functions.create',
    'pubsub.subscriptions.create',
    'pubsub.topics.create',
    'storage.buckets.list',
    'storage.buckets.create',
    // 'datastore.databases.get',
    // 'datastore.entities.create',
    // 'appengine.applications.get',
    // datastore.locations.* belongs to role 'datastore.owner' which has
    // privious three permisions.
    'datastore.locations.list',
    'cloudscheduler.jobs.create',
    'logging.sinks.create',

    'serviceusage.services.enable',
    'servicemanagement.services.bind',
    'resourcemanager.projects.get',
    'iam.serviceAccounts.actAs',
    'resourcemanager.projects.setIamPolicy',
  ],
};

MOJO_CONFIG_TEMPLATE.sentinelApis = {
  template: 'apis',
  value: [
    'Cloud Firestore API',
    'Cloud Functions API',
    'Cloud Pub/Sub API',
    'Cloud Scheduler API',
    'Cloud Build API',
    'Artifact Registry API',
    'Secret Manager API',
  ],
};

MOJO_CONFIG_TEMPLATE.sentinelFeatures = {
  category: 'Sentinel',
  resource: 'Features',
  value: sentinel.getFeatureDesc,
  editType: RESOURCE_EDIT_TYPE.READONLY,
  optionalType: OPTIONAL_TYPE.DEFAULT_UNCHECKED,
  checkFn: sentinel.checkFeature,
};

MOJO_CONFIG_TEMPLATE.sentinelVersion = {
  category: 'Sentinel',
  resource: 'Version',
  alwaysCheck: true,
  value_datarange: sentinel.getVersions,
  propertyName: 'sentinelVersion',
  checkFn: sentinel.checkVersion,
};

MOJO_CONFIG_TEMPLATE.sentinelCloudFunctions = {
  category: 'Sentinel',
  template: 'cloudFunctions',
  value: '${namespace}_main',
  attributeName: 'Current Version',
  checkFn: sentinel.checkCloudFunctions,
  enableFn: sentinel.installCloudFunctions,
};

MOJO_CONFIG_TEMPLATE.sentinelLogRouter = {
  category: 'Sentinel',
  resource: 'Log Router',
  value: '${namespace}-monitor',
  attributeName: 'Service Account',
  checkFn: sentinel.checkOrCreateLogsRouter,
};

MOJO_CONFIG_TEMPLATE.sentinelInternJob = {
  category: 'Sentinel',
  resource: 'Scheduler Job',
  value: '${namespace}-intrinsic-cronjob',
  editType: RESOURCE_EDIT_TYPE.READONLY,
  attributeName: 'Interval time (minutes)',
  attributeValue: 5,
  checkFn: sentinel.checkOrCreateInternalJob,
};

/**
 * Configurations of Sentinel Cloud Storage monitor Cloud Funtions.
 */
const SENTINEL_GCS_MONITOR_ITEMS = [
  {
    category: 'Sentinel GCS Monitor',
    resource: 'Enable',
    attributeName: '',
    attributeValue: '',
    optionalType: OPTIONAL_TYPE.DEFAULT_UNCHECKED,
    group: 'sentinelGcsMonitor',
  },
  {
    template: 'cloudStorage',
    category: 'Sentinel GCS Monitor',
    attributeValue: 7,
    propertyName: 'sentinelBucket',
    group: 'sentinelGcsMonitor',
  },
  {
    category: 'Sentinel GCS Monitor',
    resource: 'Monitored Folder',
    value: 'inbound',
    propertyName: 'sentinelInbound',
    checkFn: checkFolderName,
    group: 'sentinelGcsMonitor',
  },
  {
    category: 'Sentinel GCS Monitor',
    template: 'cloudFunctions',
    value: '${namespace}_gcs',
    attributeName: 'Current Version',
    checkFn: sentinel.checkCloudFunctions,
    enableFn: sentinel.installCloudFunctions,
    group: 'sentinelGcsMonitor',
  },
];

/**
 * MojoConfig for Sentinel.
 * @const {!MojoConfig}
 */
const SENTINEL_MOJO_CONFIG = {
  sheetName: 'Sentinel',
  config: [
    { template: 'namespace', value: 'sentinel' },
    { template: 'timeZone', value: 'Australia/Sydney' },
    { template: 'projectId' },
    { template: 'sentinelPermissions' },
    { template: 'sentinelApis' },
    { template: 'location', editType: RESOURCE_EDIT_TYPE.USER_INPUT, },
    { template: 'firestore' },
    { template: 'sentinelFeatures' },
    { template: 'sentinelVersion' },
    { template: 'sentinelCloudFunctions' },
    { template: 'serviceAccountRole', category: 'Sentinel' },
    { template: 'sentinelLogRouter' },
    { template: 'sentinelInternJob' },
    { template: 'secretManager', category: 'Sentinel' },
  ].concat(SENTINEL_GCS_MONITOR_ITEMS),
  oauthScope: [
    OAUTH_API_SCOPE.GAds,
    OAUTH_API_SCOPE.CM,
    OAUTH_API_SCOPE.DV360,
    OAUTH_API_SCOPE.SA360,
    OAUTH_API_SCOPE.Sheets,
    OAUTH_API_SCOPE.ADH,
    OAUTH_API_SCOPE.YouTube,
  ],
  headlineStyle: {
    backgroundColor: '#34A853',
    fontColor: 'white',
  },
};
