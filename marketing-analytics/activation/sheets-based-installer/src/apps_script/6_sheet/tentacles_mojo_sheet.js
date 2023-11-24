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

/** @fileoverview The mojo sheet of solution Tentacles. */

/** @type {string} Http url base for source code. */
const SOURCE_REPO = 'https://raw.githubusercontent.com/GoogleCloudPlatform/cloud-for-marketing/main/marketing-analytics/activation/gmp-googleads-connector/'

/** The options for Tentacles pull subscriptions for 'data' topics. */
const TENTACLES_PULL_SUBSCRIPTION_OPTION = Object.freeze({
  ackDeadlineSeconds: 300,
  expirationPolicy: {}, //default ttl: 31
});

/** The configuration of a logs router for Tentacles dashboard. */
const TENTACLES_LOGS_ROUTER_SINK = Object.freeze({
  FILTER_TAGS: ['TentaclesFile', 'TentaclesTask', 'TentaclesFailedRecord'],
  FILTER_JSONPAYLOAD_KEY: 'message',
  ROLE: 'roles/bigquery.dataEditor',
  TARGET_TABLE: 'winston_log',
});

/** Facilitator functions for MojoResources from Tentacles. */
const tentacles = {};

/**
 * Returns the latest 10 versions of Tentacles.
 * @return {!Array<string>} The array of versions.
 */
tentacles.getVersions = () => getNpmVersions(TENTACLES_PACKAGE);

/**
 * Updates Tentacles recent 10 versions and set the value as the lastet if
 * there is no explicit version selected.
 * @param {string} version
 * @return
 */
tentacles.checkVersion = getCheckFunctionForVersion(TENTACLES_PACKAGE);

/**
 * Returns an array of Tentacles connectors based on an array of specified
 * codes. If codes were not specified, it returns all available connectors.
 * @param {!Array<string>|undefined} enabledFeatures
 * @return {!Array<string>}
 */
tentacles.getConnectorDesc = PrimeSolution.getFeatureDesc(TENTACLES_CONNECTORS);

/**
 * Returns the code of a specified `desc` of a Tentacles connector.
 * @param {!MojoResource} row
 * @return {string}
 */
tentacles.getConnectorCode = (row) => {
  return TENTACLES_CONNECTORS
    .filter(({ desc }) => desc === row.value)
    .map(({ code }) => code)[0];
}

/**
 * Checks related resources of a Tentacles connector.
 * It will try to enable the API and create related Pub/Sub topic and
 * subscription if required.
 * @param {string} connector The description of the connector.
 * @param {!MojoSheetRow} resource
 * @return {!CheckResult}
 */
tentacles.checkConnector = (connector, resource) => {
  const projectId = getDocumentProperty('projectId');
  const apiCheckFn =
    PrimeSolution.checkAndEnableApiForFeature(TENTACLES_CONNECTORS, projectId);
  const result = apiCheckFn(connector);
  if (result) return result;
  const code = resource.attributeValue;
  const namespace = getDocumentProperty('namespace');
  const topicName = `${namespace}-${code}`;
  const subscriptionName = `${topicName}-holder`;
  const pubsub = new PubSub(projectId);
  const hasSub = pubsub.existSubscription(subscriptionName);
  if (hasSub) return { status: RESOURCE_STATUS.OK };
  const sub = pubsub.createOrUpdateSubscription(
    topicName, subscriptionName, TENTACLES_PULL_SUBSCRIPTION_OPTION);
  return {
    status: sub.state === 'ACTIVE' ? RESOURCE_STATUS.OK : RESOURCE_STATUS.ERROR,
    message: sub.error ? sub.error.message : sub.state,
  };
}

/**
 * Checks Tentacles Cloud Functions' status and version to indicate whether or
 * not need to deploy the Cloud Functions.
 * @param {string} functionName Name of the Cloud Functions
 * @return {!CheckResult}
 */
tentacles.checkCloudFunctions = (functionName) => {
  const expectedVersion = getDocumentProperty('tentaclesVersion');
  return gcloud.checkCloudFunctions(functionName, expectedVersion);
}

/**
 * Deploys Tentacles Cloud Functions while keeps updating a Sheets Cell to show
 * the progress.
 * @param {string} functionName
 * @return {!CheckResult}
 */
tentacles.installCloudFunctions = (functionName) => {
  const projectId = getDocumentProperty('projectId');
  const locationId = getDocumentProperty('locationId');
  const options = {
    projectId,
    locationId,
    namespace: getDocumentProperty('namespace'),
    version: getDocumentProperty('tentaclesVersion'),
    databaseId: getDocumentProperty('databaseId', false),
    bucket: getDocumentProperty('tentaclesBucket'),
    outbound: getDocumentProperty('tentaclesOutbound'),
    secretName: getDocumentProperty('defaultSecretName', false),
  }
  const operationName = new Tentacles(options).deployCloudFunctions(functionName);
  console.log(`Start to deploy ${functionName}`, operationName);
  return gcloud.getCloudFunctionDeployStatus(
    operationName, locationId, projectId);
}

/**
 * Checks and creates Log Router Sink which will filter information for
 * Tentacles dashboard.
 * @param {string} name Name of log router.
 * @return {!CheckResult}
 */
tentacles.checkOrCreateLogsRouter = (name) => {
  const dataset = getDocumentProperty('tentaclesDataset');
  const filter =
    TENTACLES_LOGS_ROUTER_SINK.FILTER_TAGS.map(JSON.stringify).join(' OR ');
  const key = TENTACLES_LOGS_ROUTER_SINK.FILTER_JSONPAYLOAD_KEY;
  const options = { dataset, filter: `jsonPayload.${key}=~(${filter})` };
  return gcloud.checkOrCreateLogsRouter(name, options,
    TENTACLES_LOGS_ROUTER_SINK.ROLE);
}

/**
 * Sends a dummy log which will be routed by the log router sink to create the
 * BigQuery table for Tentacles dashboard.
 */
tentacles.sendDummyLog = () => {
  const projectId = getDocumentProperty('projectId');
  const logging = new Logging(projectId);
  const response = logging.writeMessage(
    TENTACLES_LOGS_ROUTER_SINK.TARGET_TABLE,
    { message: TENTACLES_LOGS_ROUTER_SINK.FILTER_TAGS[0] }
  );
  if (JSON.stringify(response) !== '{}') {
    console.error('Failed to send a dummy log', response);
    throw new Error('Failed to send a dummy log');
  }
};

/**
 * Create the BigQuery views based on a given url.
 * @param {string} sql
 * @return {!CheckResult}
 */
tentacles.createViews = (sql) => {
  const url = `${SOURCE_REPO}/sql/${sql}`;
  const projectId = getDocumentProperty('projectId');
  const datasetId = getDocumentProperty('tentaclesDataset');
  const bigquery = new BigQuery(projectId, datasetId);
  if (!bigquery.existDataset()) {
    return {
      status: RESOURCE_STATUS.READY_TO_INSTALL,
      message: `Dataset is not ready. Will continue after it is created.`,
    };
  }
  const parameters = {
    logTable: TENTACLES_LOGS_ROUTER_SINK.TARGET_TABLE,
    projectId,
    datasetId,
    namespace: getDocumentProperty('namespace'),
    bucket: getDocumentProperty('tentaclesBucket'),
  };
  while (!bigquery.existTable(parameters.logTable)) {
    tentacles.sendDummyLog();
    Utilities.sleep(10000);
    console.log('Wait 10 sec before check Tentacles log table.');
  }
  return gcloud.createBigQueryViews(url, {}, datasetId, replaceParameters,
    parameters);
};

/**
 * Configurations of Tentacles dashboard. These are used to generate a link
 * to copy a Looker dashboard.
 */
const TENTACLES_LOOKER_ID = '68b4f0eb-977c-4c7e-9039-a205ac35ae7d';
const TENTACLES_LOOKER_DATASOURCES = [
  {
    // connector: 'bigQuery',
    type: 'TABLE',
    projectId: '${projectId}',
    datasetId: '${tentaclesDataset}',
    keepDatasourceName: 'true',
    aliases: {
      report: { tableId: 'TentaclesReport' },
      blockage: { tableId: 'TentaclesBlockage' },
    },
  },
];

/** Reusable MojoResource configuration for Tentacles. */
MOJO_CONFIG_TEMPLATE.tentaclesBucket = {
  template: 'cloudStorage',
  category: 'Tentacles',
  attributeValue: 7,
  propertyName: 'tentaclesBucket',
};

MOJO_CONFIG_TEMPLATE.tentaclesOutbound = {
  category: 'Tentacles',
  resource: 'Monitored Folder',
  value: 'outbound',
  propertyName: 'tentaclesOutbound',
  checkFn: checkFolderName,
};

MOJO_CONFIG_TEMPLATE.tentaclesConnectors = {
  category: 'Tentacles',
  resource: 'Connectors',
  value: tentacles.getConnectorDesc,
  attributeName: 'API Code',
  attributeValue: tentacles.getConnectorCode,
  editType: RESOURCE_EDIT_TYPE.READONLY,
  optionalType: OPTIONAL_TYPE.DEFAULT_UNCHECKED,
  checkFn: tentacles.checkConnector,
}

MOJO_CONFIG_TEMPLATE.tentaclesVersion = {
  category: 'Tentacles',
  resource: 'Version',
  alwaysCheck: true,
  value_datarange: tentacles.getVersions,
  propertyName: 'tentaclesVersion',
  checkFn: tentacles.checkVersion,
};

MOJO_CONFIG_TEMPLATE.tentaclesCloudFunctions = {
  template: 'cloudFunctions',
  category: 'Tentacles',
  value: [
    '${namespace}_init',
    '${namespace}_tran',
    '${namespace}_api',
  ],
  attributeName: 'Current Version',
  checkFn: tentacles.checkCloudFunctions,
  enableFn: tentacles.installCloudFunctions,
};

const TENTACLES_DASHBOARD_ITEMS = [
  {
    category: 'Tentacles Dashboard',
    resource: 'Enable',
    optionalType: OPTIONAL_TYPE.DEFAULT_CHECKED,
    group: 'tentaclesDashboard',
  },
  {
    template: 'permissions',
    category: 'Tentacles Dashboard',
    value: [
      'bigquery.datasets.create',
      'logging.logEntries.create',
      'logging.sinks.create',
    ],
    group: 'tentaclesDashboard',
  },
  {
    template: 'bigQueryDataset',
    category: 'Tentacles Dashboard',
    value: '${namespace}',
    propertyName: 'tentaclesDataset',
    group: 'tentaclesDashboard',
  },
  {
    category: 'Tentacles Dashboard',
    resource: 'Log Router',
    value: '${namespace}_log',
    attributeName: 'Service Account',
    checkFn: tentacles.checkOrCreateLogsRouter,
    group: 'tentaclesDashboard',
  },
  {
    category: 'Tentacles Dashboard',
    resource: 'BigQuery Views',
    editType: RESOURCE_EDIT_TYPE.READONLY,
    value: 'visualization_views.sql',
    value_link: `${SOURCE_REPO}/sql/visualization_views.sql`,
    checkFn: tentacles.createViews,
    enableFn: tentacles.createViews,
    group: 'tentaclesDashboard',
  },
  {
    category: 'Tentacles Dashboard',
    resource: 'Dashboard',
    editType: RESOURCE_EDIT_TYPE.READONLY,
    value: 'Available after installation',
    attributeName: 'Expected view(s)',
    attributeValue: getRequiredTablesForLooker(TENTACLES_LOOKER_DATASOURCES),
    group: 'tentaclesDashboard',
    checkFn: (value, resource) => {
      const datasetId = getDocumentProperty('tentaclesDataset');
      const result =
        gcloud.checkExpectedTables(resource.attributeValue, datasetId);
      if (result.status !== RESOURCE_STATUS.OK) {
        return Object.assign({ value: 'Available after installation' }, result);
      }
      const dashboardLink = getLookerCreateLink(
        TENTACLES_LOOKER_ID,
        TENTACLES_LOOKER_DATASOURCES
      );
      return Object.assign({
        value: 'Click here to make a copy of the dashboard',
        value_link: dashboardLink,
      }, result);
    },
  },
];

/**
 * MojoConfig for Tentacles.
 * @const {!MojoConfig}
 */
const TENTACLES_MOJO_CONFIG = {
  sheetName: 'Tentacles',
  config: [
    { template: 'namespace', value: 'tentacles' },
    { template: 'projectId' },
    {
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

        'serviceusage.services.enable',
        'servicemanagement.services.bind',
        'resourcemanager.projects.get',
        'iam.serviceAccounts.actAs',
        'resourcemanager.projects.setIamPolicy',
      ],
    },
    {
      template: 'apis',
      value: [
        'Cloud Firestore API',
        'Cloud Functions API',
        'Cloud Pub/Sub API',
        'Cloud Build API',
        'Artifact Registry API',
        'Secret Manager API',
      ],
    },
    { template: 'location', editType: RESOURCE_EDIT_TYPE.USER_INPUT, },
    { template: 'firestore' },
    { template: 'tentaclesBucket' },
    { template: 'tentaclesOutbound' },
    { template: 'tentaclesConnectors' },
    { template: 'tentaclesVersion', },
    { template: 'tentaclesCloudFunctions' },
    { template: 'serviceAccountRole', category: 'Tentacles' },
  ].concat(TENTACLES_DASHBOARD_ITEMS),
  oauthScope: [
    OAUTH_API_SCOPE.GAds,
    OAUTH_API_SCOPE.GA,
    OAUTH_API_SCOPE.CM,
    OAUTH_API_SCOPE.SA360,
    OAUTH_API_SCOPE.Sheets,
  ],
  headlineStyle: {
    backgroundColor: '#34A853',
    fontColor: 'white',
  },
};
