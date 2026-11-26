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

/** @fileoverview The mojo definition of solution Sentinel. */

/** Facilitator functions for MojoResources from Sentinel. */

const DEFAULT_DOCKER_IMAGE = 'node:24.22.1-slim';
const DEFAULT_CLOUD_FUNCTIONS_ENV = {
  DEPLOYED_BY: 'cyborg',
  GCP_PROJECT: '${projectId}',
  PROJECT_NAMESPACE: '${namespace}',
  DATABASE_ID: '${databaseId}',
  DATABASE_MODE: FIRESTORE_MODE_FOR_DAO.FIRESTORE_NATIVE,
  DEBUG: 'false',
  IN_GCP: 'true',
};

const sentinel = {
  PACKAGE_NAME: '@google-cloud/data-tasks-coordinator',
  SOURCE_ATTRIBUTE_NAMES: Object.freeze({
    package: 'Npm Package',
    version: 'Version',
  }),
  LOGS_ROUTER_FILTER: [
    '(resource.type="bigquery_resource" AND protoPayload.methodName="jobservice.jobcompleted")',
    '(resource.type = "cloud_run_job" AND protoPayload.methodName="/Jobs.RunJob")',
  ].join(' OR '),
  MAIN_FUNCTION_ENV: Object.assign({}, DEFAULT_CLOUD_FUNCTIONS_ENV, {
    FUNCTION_TARGET: 'coordinateTask',
    FUNCTION_SIGNATURE_TYPE: 'event',
  }),
  REPORT_FUNCTION_ENV: Object.assign({}, DEFAULT_CLOUD_FUNCTIONS_ENV, {
    FUNCTION_TARGET: 'reportWorkflow',
  }),
  GCS_FUNCTION_ENV: Object.assign({}, DEFAULT_CLOUD_FUNCTIONS_ENV, {
    FUNCTION_TARGET: 'monitorStorage',
    FUNCTION_SIGNATURE_TYPE: 'event',
    SENTINEL_INBOUND: '${sentinelInbound}',
  }),
};

sentinel.getDockerImageBuildBlobs = (packageName, version, namespace = '') => {
  const getIndexFile = () => {
    return `'use strict';

Object.assign(module.exports, require('${packageName}'));
`;
  };

  const getPackageFile = () => {
    const dependencies = {};
    if (version) dependencies[packageName] = version;
    const packageInfo = {
      name: `${namespace}-for-cyborg`,
      version: '0.0.1',
      description: `Auto-generated package for ${packageName}.`,
      author: 'Google Inc.',
      license: 'Apache-2.0',
      main: 'index.js',
      dependencies: dependencies,
      'cloud-repo-tools': {
        requiresKeyFile: true,
        requiresProjectId: true,
      },
    };
    return JSON.stringify(packageInfo, null, 2);
  };

  const dockerStr = `ARG BASE_IMAGE=${DEFAULT_DOCKER_IMAGE}
FROM \${BASE_IMAGE}
WORKDIR /usr/src/app
COPY package.json ./
RUN npm install
RUN npm install @google-cloud/functions-framework
COPY index.js ./
CMD ["npx", "@google-cloud/functions-framework"]`;

  const blobs = [
    Utilities.newBlob(getIndexFile(), 'text/plain', 'index.js'),
    Utilities.newBlob(getPackageFile(), 'application/json', 'package.json'),
    Utilities.newBlob(dockerStr, 'text/plain', 'Dockerfile'),
  ];
  return blobs;
};

sentinel.checkOrUploadSourceCode = (bucketAndName, resource) => {
  const {bucket, fileName} = MojoFunctions.getBucketAndFilename_(bucketAndName);

  const projectId = getDocumentProperty('projectId');
  const attributeNames = sentinel.SOURCE_ATTRIBUTE_NAMES;
  const {attributes, attributesMap} = resource;
  const package = attributesMap[attributeNames.package].attributeValue;
  const versionAttr = attributesMap[attributeNames.version];

  const versions = getNpmVersions(package);
  const latest = versions[0];
  let message = '';
  if (!versionAttr.attributeValue) {
    message = 'By default, using the latest version.';
    versionAttr.attributeValue = latest;
  } else if (versionAttr.attributeValue !== latest) {
    message = `New version ${latest} is available.`;
  }
  const version = versionAttr.attributeValue;
  if (!version) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: 'Fail to load package versions.',
    };
  }
  const namespace = getDocumentProperty('namespace') || 'sentinel';

  const blobs = sentinel.getDockerImageBuildBlobs(package, version, namespace);
  let sourcePayload = '';
  blobs.forEach(b => (sourcePayload += b.getDataAsString()));
  const sourceMd5 = Utilities.base64Encode(
    Utilities.computeDigest(
      Utilities.DigestAlgorithm.MD5,
      Utilities.newBlob(sourcePayload).getBytes(),
    ),
  );

  const zipBlob = Utilities.zip(blobs, fileName);
  const storage = new Storage(projectId);
  if (storage.getBucketStatus(bucket) === BUCKET_STATUS.NON_EXIST) {
    return {
      status: RESOURCE_STATUS.PENDING,
      message: `Bucket [${bucket}] is not ready yet.`,
      attributes,
    };
  }
  const statusLink = storage.getLinkToFile(bucket, fileName);

  try {
    const remoteMeta = storage.getObjectMetadata(bucket, fileName);
    console.log(
      'remote sourceMd5',
      remoteMeta.metadata ? remoteMeta.metadata.sourceMd5 : null,
    );
    console.log('local sourceMd5', sourceMd5);
    if (
      !remoteMeta.error &&
      remoteMeta.metadata &&
      remoteMeta.metadata.sourceMd5 === sourceMd5
    ) {
      return {
        status: RESOURCE_STATUS.OK,
        message: `${message} File [${fileName}] on gs://${bucket} is up-to-date.`,
        status_link: statusLink,
        attributes,
      };
    }
    const uploadResult = storage.uploadBlob(fileName, bucket, zipBlob);
    if (uploadResult.error) {
      return {
        status: RESOURCE_STATUS.ERROR,
        message: `Failed to upload [${fileName}].`,
        status_link: statusLink,
        attributes,
      };
    }

    storage.mutate(
      `b/${bucket}/o/${encodeURIComponent(fileName)}`,
      {metadata: {sourceMd5: sourceMd5}},
      'PATCH',
    );

    return {
      status: RESOURCE_STATUS.OK,
      message: `Uploaded [${fileName}] to gs://${bucket}. ${message}`,
      status_link: statusLink,
      attributes,
    };
  } catch (error) {
    return {
      status: RESOURCE_STATUS.ERROR,
      message: error.toString(),
      status_link: statusLink,
      attributes,
    };
  }
};

// MojoTemplateRegistry.register('sentinelVersion', {
//   category: 'Sentinel',
//   resource: 'Version',
//   alwaysCheck: true,
//   value_datarange: () => getNpmVersions(sentinel.PACKAGE_NAME),
//   propertyName: 'sentinelVersion',
//   checkFn: getCheckFunctionForVersion(sentinel.PACKAGE_NAME),
// });

MojoTemplateRegistry.register('sentinelPubSubTopic', {
  category: 'Sentinel',
  template: 'pubsubTopic',
  value: '${namespace}-monitor',
  propertyName: 'sentinelTopic',
});

MojoTemplateRegistry.register('sentinelSourceCode', {
  category: 'Sentinel',
  resource: 'Source Code (Zip)',
  value: 'gs://${bucket}/source/sentinel-source.zip',
  attributes: [
    {
      attributeName: sentinel.SOURCE_ATTRIBUTE_NAMES.package,
      attributeValue: sentinel.PACKAGE_NAME,
    },
    {
      attributeName: sentinel.SOURCE_ATTRIBUTE_NAMES.version,
      attributeValue_datarange: () => getNpmVersions(sentinel.PACKAGE_NAME),
    },
  ],
  propertyName: 'sentinelSource',
  checkFn: sentinel.checkOrUploadSourceCode,
});

MojoTemplateRegistry.register('sentinelDockerImage', {
  template: 'dockerImage',
  category: 'Sentinel',
  value: '${localRepo}/sentinel:latest',
  attributes: [
    {
      attributeName: CloudBuild.ATTRIBUTE_NAMES.storageSource,
      attributeValue: '${sentinelSource}',
    },
    {
      attributeName: CloudBuild.ATTRIBUTE_NAMES.baseImage,
      attributeValue: DEFAULT_DOCKER_IMAGE,
    },
  ],
  propertyName: 'sentinelImage',
});

MojoTemplateRegistry.register('sentinelMainService', {
  template: 'cloudRunService',
  category: 'Sentinel',
  value: '${namespace}-main',
  attributes: [
    {
      attributeName:
        CloudRunService.ATTRIBUTE_NAMES['template.containers.0.image'],
      attributeValue: '${sentinelImage}',
    },
    {
      attributeName: CloudRunService.ATTRIBUTE_NAMES['template.timeout'],
      attributeValue: '600',
    },
    {
      attributeName:
        CloudRunService.ATTRIBUTE_NAMES['template.containers.0.env'],
      attributeValue: getEnvVarForCloudRun(sentinel.MAIN_FUNCTION_ENV),
    },
  ],
  propertyName: 'sentinelMain',
});
MojoTemplateRegistry.register('sentinelMainTrigger', {
  template: 'eventarcTrigger',
  category: 'Sentinel',
  value: '${namespace}-main-trig',
  propertyName: 'sentinelMainService',
  attributes: [
    {
      attributeName: Eventarc.ATTRIBUTE_NAMES['destination.cloudRun.service'],
      attributeValue: '${namespace}-main',
    },
    {
      attributeName: Eventarc.ATTRIBUTE_NAMES['eventFilters.0'],
      attributeValue: 'google.cloud.pubsub.topic.v1.messagePublished',
    },
    {
      attributeName: Eventarc.ATTRIBUTE_NAMES['transport.pubsub.topic'],
      attributeValue: 'projects/${projectId}/topics/${sentinelTopic}',
    },
  ],
});
MojoTemplateRegistry.register('sentinelLogRouter', {
  template: 'logRouter',
  category: 'Sentinel',
  value: '${namespace}-monitor',
  attributes: [
    {
      attributeName: Logging.ATTRIBUTE_NAMES.destination,
      attributeValue:
        'pubsub.googleapis.com/projects/${projectId}/topics/${sentinelTopic}',
    },
    {
      attributeName: Logging.ATTRIBUTE_NAMES.filter,
      attributeValue: sentinel.LOGS_ROUTER_FILTER,
    },
  ],
});
MojoTemplateRegistry.register('sentinelInternJob', {
  template: 'schedulerPubsubJob',
  category: 'Sentinel',
  value: '${namespace}-intrinsic-cronjob',
  attributes: [
    {
      attributeName: CloudScheduler.ATTRIBUTE_NAMES.schedule,
      attributeValue: '*/5 * * * *',
    },
    {
      attributeName: CloudScheduler.ATTRIBUTE_NAMES['pubsubTarget.topicName'],
      attributeValue: 'projects/${projectId}/topics/${sentinelTopic}',
    },
    {
      attributeName: CloudScheduler.ATTRIBUTE_NAMES['pubsubTarget.attributes'],
      attributeValue: '{"taskId":"system"}',
    },
    {
      attributeName: CloudScheduler.ATTRIBUTE_NAMES['pubsubTarget.data'],
      attributeValue: '{"intrinsic":"status_check"}',
    },
  ],
});

MojoTemplateRegistry.register('sentinelGcsService', {
  template: 'cloudRunService',
  category: 'Sentinel GCS Monitor',
  value: '${namespace}-gcs',
  attributes: [
    {
      attributeName:
        CloudRunService.ATTRIBUTE_NAMES['template.containers.0.image'],
      attributeValue: '${sentinelImage}',
    },
    {
      attributeName:
        CloudRunService.ATTRIBUTE_NAMES['template.containers.0.env'],
      attributeValue: getEnvVarForCloudRun(sentinel.GCS_FUNCTION_ENV),
    },
  ],
  propertyName: 'sentinelGcs',
});
MojoTemplateRegistry.register('sentinelGcsTrigger', {
  template: 'eventarcTrigger',
  category: 'Sentinel GCS Monitor',
  value: '${namespace}-gcs-trig',
  attributes: [
    {
      attributeName: Eventarc.ATTRIBUTE_NAMES['destination.cloudRun.service'],
      attributeValue: '${sentinelGcs}',
    },
    {
      attributeName: Eventarc.ATTRIBUTE_NAMES['destination.cloudRun.path'],
      attributeValue: '/ingress/gcs',
    },
    {
      attributeName: Eventarc.ATTRIBUTE_NAMES['eventFilters.0'],
      attributeValue: 'google.cloud.storage.object.v1.finalized',
    },
    {
      attributeName: Eventarc.ATTRIBUTE_NAMES['eventFilters.1'],
      attributeValue: '${sentinelBucket}',
    },
  ],
});

MojoTemplateRegistry.register('sentinelReportService', {
  template: 'cloudRunService',
  category: 'Sentinel',
  value: '${namespace}-report',
  optionalType: OPTIONAL_TYPE.DEFAULT_CHECKED,
  resource: 'Workflow Viewer',
  attributes: [
    {
      attributeName:
        CloudRunService.ATTRIBUTE_NAMES['template.containers.0.image'],
      attributeValue: '${sentinelImage}',
    },
    {
      attributeName:
        CloudRunService.ATTRIBUTE_NAMES['template.containers.0.env'],
      attributeValue: getEnvVarForCloudRun(sentinel.REPORT_FUNCTION_ENV),
    },
    {
      attributeName: CloudRunService.ATTRIBUTE_NAMES.ingress,
      attributeValue: CloudRunService.INGRESS_TRAFFIC.ALL, // required
      attributeValue_datarange: Object.values(CloudRunService.INGRESS_TRAFFIC),
    },
  ],
  propertyName: 'sentinelReport',
});

/**
 * Configurations of Sentinel Cloud Storage monitor Cloud Functions.
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
    value: '${bucket}',
    propertyName: 'sentinelBucket',
    group: 'sentinelGcsMonitor',
  },
  {
    category: 'Sentinel GCS Monitor',
    resource: 'Monitored Folder',
    value: 'inbound',
    propertyName: 'sentinelInbound',
    checkFn: MojoFunctions.checkFolderName,
    group: 'sentinelGcsMonitor',
  },
  {
    template: 'sentinelGcsService',
    group: 'sentinelGcsMonitor',
  },
  {
    template: 'sentinelGcsTrigger',
    group: 'sentinelGcsMonitor',
  },
];

/**
 * Configurations of Tentacles module in Sentinel.
 */
// const SENTINEL_TENTACLES_MODULE = [
//   {
//     category: 'Tentacles',
//     resource: 'Enable',
//     attributeName: '',
//     attributeValue: '',
//     optionalType: OPTIONAL_TYPE.DEFAULT_CHECKED,
//     group: 'sentinelMultipleTask',
//   },
//   {
//     template: 'tentaclesVersion',
//     category: 'Tentacles',
//     resource: 'Version',
//     group: 'sentinelMultipleTask',
//   },
//   {
//     template: 'tentaclesConnectors',
//     category: 'Tentacles',
//     optionalType: OPTIONAL_TYPE.DEFAULT_CHECKED,
//     value: PrimeSolution.getFeatureDesc(TENTACLES_CONNECTORS)(['PB']),
//     group: 'sentinelMultipleTask',
//   },
//   {
//     template: 'tentaclesCloudFunctions',
//     category: 'Tentacles',
//     resource: 'Cloud Functions',
//     group: 'sentinelMultipleTask',
//     value: ['${namespace}_http', '${namespace}_tran', '${namespace}_api'],
//   },
// ];

// sentinel.getCustomizedConfig_ = configCustomization => {
//   const baseConfig = [
//     {template: 'namespace', value: 'sentinel'},
//     {template: 'timeZone', value: 'Australia/Sydney'},
//     {template: 'projectId'},
//     {template: 'permissions'}, // {template: 'sentinelPermissions'},
//     {template: 'apis'}, // {template: 'sentinelApis'},
//     {template: 'location', editType: RESOURCE_EDIT_TYPE.USER_INPUT},
//     {template: 'firestore'},
//     {
//       template: 'cloudStorage',
//       attributeValue: 0,
//       optionalType: OPTIONAL_TYPE.DEFAULT_CHECKED,
//     },
//     {
//       template: 'cloudStorage',
//       resource: 'Cloud Storage Temporary Bucket',
//       attributeValue: 7,
//       value: '${namespace}-${projectId|normalized}-temp',
//       optionalType: OPTIONAL_TYPE.DEFAULT_CHECKED,
//       propertyName: 'tempBucket',
//     },
//     {template: 'bigQueryDataset', optionalType:
//     OPTIONAL_TYPE.DEFAULT_CHECKED},

//     {template: 'sentinelVersion'},
//     {template: 'sentinelFeatures'},
//     {template: 'sentinelCloudFunctions'},
//     {template: 'sentinelLogRouter'},
//     {template: 'sentinelInternJob'},
//     {template: 'serviceAccountRole', category: 'Sentinel'},
//     {
//       template: 'sentinelCloudFunctions',
//       resource: 'Workflow Viewer',
//       optionalType: OPTIONAL_TYPE.DEFAULT_CHECKED,
//       value: '${namespace}_report',
//       propertyName: 'sentinelReport',
//     },
//     {
//       category: 'Sentinel Guest Mode',
//       template: 'gcpServiceDelegator',
//       optionalType: OPTIONAL_TYPE.DEFAULT_UNCHECKED,
//     },
//     ...SENTINEL_GCS_MONITOR_ITEMS,
//     ...SENTINEL_TENTACLES_MODULE,
//   ];
//   if (!configCustomization) return baseConfig;
//   const baseConfigCompleted = baseConfig.map(completeMojoConfigFromTemplate);
//   const completedCustomization = [configCustomization]
//     .flat()
//     .map(completeMojoConfigFromTemplate);
//   completedCustomization.forEach(config => {
//     const {category, resource} = config;
//     let foundCategory = false,
//       processed = false;
//     for (let i = 0; i < baseConfigCompleted.length; i++) {
//       if (!foundCategory) {
//         // Not enter the target category yet.
//         if (baseConfigCompleted[i].category !== category) continue; // Quit
//         for non target category. foundCategory = true;
//       }
//       if (baseConfigCompleted[i].category === category) {
//         // In the target category.
//         if (baseConfigCompleted[i].resource !== resource) continue; // Quit
//         for non target resource.
//         // Find the target resource, config will be merged.
//         baseConfigCompleted[i] = Object.assign(baseConfigCompleted[i],
//         config); if (config.appendValue) {
//           baseConfigCompleted[i].value = baseConfigCompleted[i].value.concat(
//             config.appendValue,
//           );
//         }
//         processed = true;
//         break; // Quit inner loop as this 'customization' has been done.
//       } else {
//         // Out of the target category with no resource matches
//         baseConfigCompleted.splice(i, 0, config); // Insert this config at
//         the end of its category. processed = true; break;
//       }
//     }
//     if (!processed) {
//       // Have gone through all configs and there is no such category
//       baseConfigCompleted.push(config); // Append the config at the end.
//     }
//   });
//   const finalConfig = baseConfigCompleted.filter(
//     ({ignored}) => ignored !== true,
//   );
//   console.log('Generated all mojo config:', finalConfig.length);
//   return finalConfig;
// };

// /**
//  * Returns a customized and complete Mojo config for a Sentinel based
//  solution.
//  * By default, the config will be loaded when it is required.
//  * @const {!MojoConfig}
//  */
// sentinel.getCustomizedMojo = (
//   configCustomization,
//   mojoBasic,
//   lazyLoad = true,
// ) => {
//   const template = {
//     oauthScope: Object.values(OAUTH_API_LIST),
//     sheetName: '0. Infrastructure',
//     headlineStyle: {
//       backgroundColor: '#202124',
//       fontColor: 'white',
//     },
//   };
//   const customizedMojo = Object.assign(template, mojoBasic);
//   if (lazyLoad) {
//     customizedMojo.config = () =>
//       sentinel.getCustomizedConfig_(configCustomization);
//   } else {
//     customizedMojo.config =
//     sentinel.getCustomizedConfig_(configCustomization);
//   }
//   return customizedMojo;
// };
