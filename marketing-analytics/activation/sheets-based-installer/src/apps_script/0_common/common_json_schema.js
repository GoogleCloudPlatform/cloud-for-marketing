// Copyright 2024 Google Inc.
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

/** @fileoverview Common functions to generate JSON schema for JSON Editor .*/

const JSON_SCHEMA_FUNCTIONS = {};
// Deep merge 'options' immutably without mutating template or extra.
JSON_SCHEMA_FUNCTIONS.mergeDeepOptions = (template, extra) => {
  const result = Object.assign({}, template, extra);
  if ((template && template.options) || (extra && extra.options)) {
    result.options = Object.assign(
      {},
      template ? template.options : {},
      extra ? extra.options : {},
    );
  }
  return result;
};

// Return top level options: no title, no added prop, default table layout.
JSON_SCHEMA_FUNCTIONS.getTopOptions = (layout = 'table') => {
  return {
    titleHidden: true,
    no_additional_properties: true,
    object_layout: layout,
  };
};
// Returns options for a fixed object: no json, no properties or collapse.
JSON_SCHEMA_FUNCTIONS.getFixedObject = extra => {
  return Object.assign(
    {
      disable_properties: true,
      disable_edit_json: true,
      disable_collapse: true,
    },
    extra,
  );
};
// Returns options for a object with optional properties.
JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject = extra => {
  return Object.assign(
    {
      no_additional_properties: true,
      disable_edit_json: true,
      disable_collapse: true,
    },
    extra,
  );
};
// Returns options for a object with properties editor.
JSON_SCHEMA_FUNCTIONS.getOnlyPropertiesObject = extra => {
  return Object.assign(
    {
      disable_edit_json: true,
      disable_collapse: true,
    },
    extra,
  );
};
// Returns property of 'secretName'.
JSON_SCHEMA_FUNCTIONS.getSecretName = extra => {
  return JSON_SCHEMA_FUNCTIONS.mergeDeepOptions(
    {
      type: 'string',
      title: 'Name of Secret for the OAuth Token',
      options: {grid_columns: 4},
      links: [
        {
          rel: 'For more information, see Secret Manager',
          href: 'https://cloud.google.com/security/products/secret-manager',
        },
      ],
    },
    extra,
  );
};
JSON_SCHEMA_FUNCTIONS.getSqlObject = extra => {
  return JSON_SCHEMA_FUNCTIONS.mergeDeepOptions(
    {
      type: 'string',
      format: 'sql',
      title: 'String of Query',
      options: {
        grid_columns: 12,
        ace: {
          theme: 'ace/theme/vibrant_ink',
          tabSize: 2,
          useSoftTabs: true,
          wrap: true,
        },
      },
      propertyOrder: 10,
    },
    extra,
  );
};

// Return speed control properties.

JSON_SCHEMA_FUNCTIONS.getNumberOfThreads = extra => {
  const template = {
    type: 'number',
    title: 'Number of Threads',
    options: {
      infoText: 'Maximum Number of Concurrent Requests',
      grid_columns: 4,
    },
  };
  return JSON_SCHEMA_FUNCTIONS.mergeDeepOptions(template, extra);
};
JSON_SCHEMA_FUNCTIONS.getQps = extra => {
  const template = {
    type: 'number',
    title: 'QPS',
    options: {
      infoText: 'Number of Requests per Second',
      grid_columns: 4,
    },
  };
  return JSON_SCHEMA_FUNCTIONS.mergeDeepOptions(template, extra);
};
JSON_SCHEMA_FUNCTIONS.getRecordsPerRequest = extra => {
  const template = {
    type: 'number',
    title: 'Records Per Request',
    options: {
      infoText: 'Number of Records per Requests',
      grid_columns: 4,
    },
  };
  return JSON_SCHEMA_FUNCTIONS.mergeDeepOptions(template, extra);
};

JSON_SCHEMA_FUNCTIONS.cloudFunctionServiceProperties = {
  url: {
    type: 'string',
    title: 'Full Url of the Pupa Cloud Function',
    default: 'https://#locationId#-#projectId#.cloudfunctions.net/',
    options: {
      grid_columns: 9,
      infoText:
        'Pupa can deploy a block of Python code in a Colab as a standalone HTTP Cloud Function. This is the full Url of the Cloud Function.',
    },
    propertyOrder: 1,
  },
  functionName: {
    type: 'string',
    title: 'Target Function Name',
    options: {
      grid_columns: 3,
      infoText:
        'In a Pupa Cloud Function, there might be multiple Python functions. Using this to specify the name of the Python function to be invoked.',
    },
    propertyOrder: 10,
  },
  vars: {
    type: 'object',
    title: 'Global Variables for the Python Function',
    options: {grid_columns: 12},
    required: [],
    properties: {
      // PROJECT_ID: { type: 'string', default: '#projectId#' },
      // LOCATION: { type: 'string', default: '#locationId#' },
    },
    propertyOrder: 100,
  },
};

JSON_SCHEMA_FUNCTIONS.cloudFunctionInvokeProperties = {
  service: {
    type: 'object',
    title: 'Pupa Cloud Function Configuration',
    format: 'grid-strict',
    options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject({}),
    required: [],
    defaultProperties: ['url', 'functionName', 'vars'],
    properties: JSON_SCHEMA_FUNCTIONS.cloudFunctionServiceProperties,
    propertyOrder: 1,
  },
  output: {
    type: 'object',
    title: 'Cloud Storage for Output',
    format: 'grid',
    options: JSON_SCHEMA_FUNCTIONS.getFixedObject({grid_columns: 12}),
    properties: {
      projectId: {
        type: 'string',
        default: '#projectId#',
        options: {grid_columns: 3},
      },
      bucket: {
        type: 'string',
        default: '#tentaclesBucket#',
        options: {grid_columns: 3},
      },
      folder: {
        type: 'string',
        options: {
          grid_columns: 6,
          infoText:
            'There might be multiple Tentacle Tasks which will generate multiple result files.',
        },
      },
    },
    propertyOrder: 10,
  },
  qps: JSON_SCHEMA_FUNCTIONS.getQps({default: 1}),
  recordsPerRequest: JSON_SCHEMA_FUNCTIONS.getRecordsPerRequest({default: 1}),
  numberOfThreads: JSON_SCHEMA_FUNCTIONS.getNumberOfThreads({default: 1}),
};

JSON_SCHEMA_FUNCTIONS.getPubSubMessageConfigOption = () => {
  return {
    topic: {
      type: 'string',
      title: 'Pub/Sub Topic',
      default: '#PROJECT_NAMESPACE#-monitor',
      propertyOrder: 1,
    },
    qps: JSON_SCHEMA_FUNCTIONS.getQps({
      default: 1,
      minimum: 0.0001,
      maximum: 1,
      propertyOrder: 2,
    }),
    numberOfThreads: JSON_SCHEMA_FUNCTIONS.getNumberOfThreads({
      default: 1,
      minimum: 1,
      maximum: 10,
      propertyOrder: 3,
    }),
    message: {
      type: 'object',
      title: 'Message Body',
      format: 'grid-strict',
      options: JSON_SCHEMA_FUNCTIONS.getOnlyPropertiesObject({
        grid_columns: 12,
        grid_break: true,
      }),
      required: [],
      properties: {},
      propertyOrder: 10,
    },
    attributes: {
      type: 'object',
      title: 'Message Attributes',
      options: JSON_SCHEMA_FUNCTIONS.getOnlyPropertiesObject({
        grid_columns: 12,
        grid_break: true,
      }),
      propertyOrder: 20,
      required: [],
      properties: {
        // taskId: { type: 'string', default: '${taskId}' },
        // multipleTag: { type: 'string', default: '${multipleTag}' },
      },
    },
  };
};
