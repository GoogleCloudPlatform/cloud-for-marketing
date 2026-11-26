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

/**
 * Cached full Task Schema object and serialized JSON string.
 */
let cachedSentinelTaskSchemas = null;
let cachedSentinelTaskSchemaStr = null;

/**
 * Builds all Sentinel task JSON schemas.
 * @return {!Object<string, !Object>}
 * @private
 */
function buildSentinelTaskSchemas_() {
  /** Returns 'ancestor' property. */
  function getAncestor() {
    return {
      type: 'string',
      title: 'Ancestor Task',
      propertyOrder: 1,
    };
  }

  /** Returns 'errorOptions' property. */
  function getErrorOptions() {
    return {
      type: 'object',
      title: 'Error Handling Options',
      options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject(),
      format: 'grid-strict',
      properties: {
        retryTimes: {
          type: 'number',
          default: 3,
          options: {
            grid_columns: 4,
            infoText:
              'Retry how many times before failing the task. Note, retries only attempted after a RetryableError.',
          },
          propertyOrder: 1,
        },
        ignoreError: {
          type: 'boolean',
          format: 'checkbox',
          options: {
            grid_columns: 4,
            infoText:
              'When a task is failed, whether to continue to its next task.',
          },
          propertyOrder: 2,
        },
      },
      propertyOrder: 1090,
    };
  }

  /** Returns 'appendedParameters' property. */
  function getAppendedParameters(extraOptions) {
    return {
      type: 'object',
      title: 'Appended Parameters',
      options: Object.assign({disable_collapse: false}, extraOptions),
      propertyOrder: 1100,
    };
  }

  /** Returns a group of tasks, mainly used for 'next' property. */
  function getTaskGroup(title = 'Next Task(s)') {
    return {
      title,
      options: {
        keep_oneof_values: false,
        disable_collapse: false,
        grid_columns: 12,
      },
      format: 'grid-strict',
      anyOf: [
        {type: 'string', title: 'Single Task Id'},
        {
          type: 'array',
          title: 'Multiple Task Ids',
          items: {type: 'string', title: 'Task Id'},
        },
        {
          type: 'array',
          title: 'Multiple Tasks with Parameters',
          items: {
            type: 'object',
            title: 'Task',
            options: JSON_SCHEMA_FUNCTIONS.getFixedObject(),
            properties: {
              taskId: {
                type: 'string',
                title: 'Task Id',
                options: {grid_columns: 4},
              },
              appendedParameters: getAppendedParameters({grid_columns: 8}),
            },
          },
        },
      ],
      propertyOrder: 1200,
    };
  }

  /** Returns BigQuery writeDisposition property. */
  function getWriteDisposition(extra) {
    return Object.assign(
      {
        type: 'string',
        enum: ['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY'],
        options: {
          grid_columns: 4,
          infoText:
            'Specifies the action that occurs if the destination table already exists.',
        },
        links: [
          {
            rel: 'More details',
            href: 'https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata.WriteDisposition',
          },
        ],
      },
      extra,
    );
  }

  /** Returns BigQuery table object. */
  function getBigQueryTable(title = 'BigQuery Table', extraOptions) {
    return {
      type: 'object',
      title,
      format: 'grid-strict',
      options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject(
        Object.assign({grid_columns: 12}, extraOptions),
      ),
      required: [],
      defaultProperties: ['projectId', 'datasetId', 'tableId'],
      properties: {
        projectId: {
          type: 'string',
          title: 'GCP Project Id',
          options: {grid_columns: 4},
          propertyOrder: 1,
        },
        datasetId: {
          type: 'string',
          title: 'Dataset Id',
          options: {grid_columns: 4},
          propertyOrder: 2,
        },
        tableId: {
          type: 'string',
          title: 'Table Id',
          options: {grid_columns: 4},
          propertyOrder: 3,
        },
        location: {
          type: 'string',
          title: 'Location',
          options: {grid_columns: 4},
          propertyOrder: 4,
        },
        // keyFilename: {type: 'string'},
      },
      propertyOrder: 1,
    };
  }

  /** Returns BigQuery table object with deleting information. */
  function getBigQueryToDelete(title) {
    const bigquery = getBigQueryTable(title);
    Object.assign(bigquery.properties.tableId, {
      description: 'Without Table Id, this task will delete the whole dataset.',
    });
    return bigquery;
  }
  /** Returns Cloud Storage bucket object. */
  function getStorageBucket(title = 'Cloud Storage Bucket', extraOptions) {
    return {
      type: 'object',
      title,
      format: 'grid-strict',
      options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject(extraOptions),
      required: [],
      defaultProperties: ['projectId', 'bucket'],
      properties: {
        projectId: {
          type: 'string',
          title: 'GCP Project Id',
          options: {grid_columns: 4},
          propertyOrder: 1,
        },
        bucket: {
          type: 'string',
          title: 'Bucket',
          options: {grid_columns: 4},
          propertyOrder: 2,
        },
        // keyFilename: {type: 'string'},
      },
      propertyOrder: 1,
    };
  }

  /** Returns Cloud Storage folder or file object. */
  function getStorageFile(title = 'Cloud Storage File', extraOptions) {
    const result = getStorageBucket(title, extraOptions);
    result.defaultProperties.push('name');
    result.properties.name = {
      type: 'string',
      title: 'File Name',
      options: {grid_columns: 8},
      propertyOrder: 4,
    };
    return result;
  }

  /** Returns a data property with separate year, month and day properties. */
  function getDateProperties(extraOptions) {
    return Object.assign(
      {
        type: 'object',
        options: JSON_SCHEMA_FUNCTIONS.getFixedObject(),
        format: 'grid',
        properties: {
          year: {type: 'string', options: {grid_columns: 4}},
          month: {type: 'string', options: {grid_columns: 4}},
          day: {type: 'string', options: {grid_columns: 4}},
        },
      },
      extraOptions,
    );
  }

  /**
   * Returns BigQuery Table Fields schema property.
   * By default, it supports up to 6 layers.
   */
  function getFieldsSchema(layer = 1) {
    const schema = {
      type: 'array',
      title: `Level${layer} Fields`,
      options: {titleHidden: true},
      items: {
        type: 'object',
        title: `Level${layer} Field`,
        options: JSON_SCHEMA_FUNCTIONS.getFixedObject(),
        properties: {
          name: {type: 'string'},
          mode: {
            type: 'string',
            enum: ['NULLABLE', 'REQUIRED', 'REPEATED'],
          },
          type: {
            type: 'string',
            enum: [
              'STRING',
              'INTEGER',
              'FLOAT',
              'BOOLEAN',
              'TIMESTAMP',
              'DATE',
              'TIME',
              'DATETIME',
              'JSON',
              'RECORD',
            ],
          },
        },
      },
    };
    if (layer < 6) {
      schema.items.properties.fields = getFieldsSchema(layer + 1);
      schema.items.properties.fields.options = Object.assign(
        schema.items.properties.fields.options,
        {
          dependencies: {type: 'RECORD'},
        },
      );
    }
    return schema;
  }

  /** Returns BigQuery Table schema property. */
  function getBigQuerySchema(extra) {
    return Object.assign(
      {
        type: 'object',
        title: 'BigQuery Table Schema',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject({
          grid_columns: 12,
        }),
        required: [],
        properties: {
          schema: {
            type: 'object',
            title: 'Schema Definition',
            options: JSON_SCHEMA_FUNCTIONS.getFixedObject(),
            properties: {
              fields: getFieldsSchema(),
            },
            propertyOrder: 1,
          },
          timePartitioning: {
            type: 'object',
            title: 'Partition Table Settings',
            options: JSON_SCHEMA_FUNCTIONS.getFixedObject(),
            format: 'grid-strict',
            properties: {
              type: {
                type: 'string',
                enum: ['DAY'],
                default: 'DAY',
                options: {grid_columns: 4},
              },
            },
            propertyOrder: 2,
          },
        },
      },
      extra,
    );
  }

  /** Returns asynchronized task time options. */
  function getAsyncTaskTimeOptions() {
    return {
      estimateRunningTime: {
        type: 'number',
        options: {
          grid_columns: 4,
          infoText: `Skip checking whether the task is finished before it has been 'estimateRunningTime' minutes.`,
        },
        propertyOrder: 1,
      },
      dueTime: {
        type: 'number',
        options: {
          grid_columns: 4,
          grid_break: true,
          infoText: `If the task has been run more then 'dueTime' minutes, a 'Timeout' error will be thrown out.`,
        },
        propertyOrder: 2,
      },
    };
  }

  /** Returns Tentacles Http service property. */
  function getTentaclesHttpService(extra) {
    return Object.assign(
      {
        type: 'object',
        title: 'Tentacles HTTP Service',
        oneOf: [
          {
            type: 'object',
            title: 'Configuration',
            format: 'grid-strict',
            description:
              'By default, the final URL would be: https://${locationId}-${projectId}.cloudfunctions.net/${namespace}_http',
            options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject(),
            required: [],
            properties: {
              projectId: {
                type: 'string',
                options: {grid_columns: 4},
                propertyOrder: 1,
              },
              locationId: {
                type: 'string',
                options: {grid_columns: 4},
                propertyOrder: 2,
              },
              namespace: {
                type: 'string',
                options: {grid_columns: 4},
                propertyOrder: 3,
              },
            },
            additionalProperties: false,
          },
          {
            type: 'object',
            title: 'URL',
            options: JSON_SCHEMA_FUNCTIONS.getFixedObject(),
            properties: {
              url: {
                type: 'string',
                title: 'Full URL',
                options: {grid_columns: 12},
              },
            },
            additionalProperties: false,
          },
        ],
      },
      extra,
    );
  }

  const TASK_SCHEMA = {};

  TASK_SCHEMA.query = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: [],
    defaultProperties: ['source', 'destination'],
    properties: {
      ancestor: getAncestor(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      source: {
        type: 'object',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject({
          disable_collapse: false,
        }),
        required: [],
        defaultProperties: ['sql', 'external'],
        properties: {
          file: getStorageFile('SQL file in Cloud Storage'),
          sql: JSON_SCHEMA_FUNCTIONS.getSqlObject({propertyOrder: 2}),
          external: {
            type: 'boolean',
            title: 'Query includes Google Sheets based table',
            format: 'checkbox',
            propertyOrder: 3,
          },
        },
        dependencies: {
          sql: {not: {required: ['file']}},
          file: {not: {required: ['sql']}},
        },
        propertyOrder: 10,
      },
      destination: {
        type: 'object',
        options: JSON_SCHEMA_FUNCTIONS.getFixedObject({
          disable_collapse: false,
        }),
        format: 'grid-strict',
        required: [],
        defaultProperties: ['table', 'writeDisposition'],
        properties: {
          table: getBigQueryTable(),
          writeDisposition: getWriteDisposition({propertyOrder: 2}),
        },
        propertyOrder: 20,
      },
    },
  };

  TASK_SCHEMA.query_adh = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: [],
    defaultProperties: ['source', 'destination'],
    properties: {
      ancestor: getAncestor(),
      errorOptions: getErrorOptions(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      source: {
        type: 'object',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject({
          disable_collapse: false,
        }),
        required: [],
        defaultProperties: ['customerId', 'title', 'sql'],
        properties: {
          customerId: {
            type: 'string',
            title: 'ADH Customer Id',
            options: {grid_columns: 4},
            propertyOrder: 1,
          },
          title: {
            type: 'string',
            title: 'ADH Query Title',
            options: {grid_columns: 8},
            propertyOrder: 2,
          },
          sql: JSON_SCHEMA_FUNCTIONS.getSqlObject({propertyOrder: 3}),
          file: Object.assign(getStorageFile('SQL file in Cloud Storage'), {
            propertyOrder: 4,
          }),
          // XXX: ADH API has a complicated structure of parameters. Keep
          //  edit_json true to make it easier to edit.
          parameterTypes: {
            type: 'object',
            title: 'Parameter Definition',
            options: {
              grid_columns: 12,
              disable_properties: true,
            },
            required: [],
            links: [
              {
                rel: 'More details about ParameterType',
                href: 'https://developers.google.com/ads-data-hub/reference/rest/v1/ParameterType',
              },
            ],
            propertyOrder: 5,
          },
          spec: {
            type: 'object',
            title: 'Query Execution Parameters',
            format: 'grid-strict',
            required: [],
            defaultProperties: ['startDate', 'endDate'],
            properties: {
              startDate: getDateProperties({propertyOrder: 1}),
              endDate: getDateProperties({propertyOrder: 2}),
              parameterValues: {
                type: 'object',
                options: {
                  grid_columns: 12,
                  disable_properties: true,
                },
                required: [],
                propertyOrder: 10,
              },
              privacyMode: {
                type: 'string',
                enum: [
                  'QUERY_PRIVACY_MODE_UNSPECIFIED',
                  'DIFFERENCE_CHECKS',
                  'NOISE',
                ],
                options: {grid_columns: 4},
                links: [
                  {
                    rel: 'Types of privacy checks',
                    href: 'https://developers.google.com/ads-data-hub/reference/rest/v1/QueryExecutionSpec#QueryPrivacyMode',
                  },
                ],
                propertyOrder: 20,
              },
            },
            propertyOrder: 6,
          },
        },
        dependencies: {
          sql: {not: {required: ['file']}},
          file: {not: {required: ['sql']}},
        },
      },
      destination: {
        type: 'object',
        options: JSON_SCHEMA_FUNCTIONS.getFixedObject({
          disable_collapse: false,
        }),
        format: 'grid-strict',
        required: [],
        defaultProperties: ['table'],
        properties: {
          table: getBigQueryTable('ADH Output BigQuery'),
        },
      },
    },
  };

  TASK_SCHEMA.export = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: [],
    defaultProperties: ['source', 'destination', 'options'],
    properties: {
      ancestor: getAncestor(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      source: Object.assign(
        getBigQueryTable('Source (BigQuery Table)', {disable_collapse: false}),
        {propertyOrder: 1000},
      ),
      destination: Object.assign(
        getStorageFile('Export to Cloud Storage', {disable_collapse: false}),
        {propertyOrder: 1001},
      ),
      options: {
        type: 'object',
        format: 'grid-strict',
        options: JSON_SCHEMA_FUNCTIONS.getOnlyPropertiesObject({
          disable_collapse: false,
        }),
        required: [],
        defaultProperties: ['destinationFormat', 'printHeader'],
        properties: {
          destinationFormat: {
            type: 'string',
            enum: ['NEWLINE_DELIMITED_JSON', 'CSV'],
            options: {
              grid_columns: 4,
              infoText: 'The exported file format.',
            },
            propertyOrder: 1,
          },
          printHeader: {
            type: 'boolean',
            format: 'checkbox',
            options: {
              grid_columns: 2,
              dependencies: {destinationFormat: 'CSV'},
              infoText: 'Whether to print out a header row in the results.',
            },
            propertyOrder: 2,
          },
          allowMultiple: {
            type: 'boolean',
            title: 'Allowed multiple files to be exported',
            options: {
              grid_columns: 6,
              infoText:
                'If the source table is too large and export target is a single file, with this enabled, Sentinel will try to export data with a wildcard in the file name.',
            },
            format: 'checkbox',
            propertyOrder: 3,
          },
        },
        links: [
          {
            rel: 'For more settings, see BigQuery JobConfigurationExtract',
            href: 'https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationExtract',
          },
        ],
      },
    },
  };

  TASK_SCHEMA.load = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: [],
    defaultProperties: ['source', 'destination', 'options'],
    properties: {
      ancestor: getAncestor(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      source: {
        type: 'object',
        format: 'grid-strict',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject({
          disable_collapse: false,
        }),
        required: [],
        defaultProperties: ['file'],
        properties: {
          file: getStorageFile('Cloud Storage File(s) to be Loaded'),
          fileNamePattern: {
            type: 'string',
            options: {
              grid_columns: 6,
              infoText:
                'If there is no task[TASK_NAME] in the file name, Sentinel will try to find a load task that its fileNamePattern can match the file name.',
            },
          },
        },
      },
      destination: {
        type: 'object',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject({
          disable_collapse: false,
        }),
        format: 'grid-strict',
        required: [],
        defaultProperties: ['table'],
        properties: {
          table: getBigQueryTable('Load to BigQuery Table'),
          tableSchema: getBigQuerySchema({propertyOrder: 2}),
          schemaFile: Object.assign(
            getStorageFile('Schema File in Cloud Storage'),
            {propertyOrder: 3},
          ),
          schemaSource: {
            type: 'string',
            options: {
              grid_columns: 4,
              infoText: 'Schema based on a Google Ads Report Task Id',
            },
            propertyOrder: 4,
          },
          csvSafeColumnNames: {
            type: 'boolean',
            format: 'checkbox',
            options: {
              grid_columns: 4,
              infoText:
                'Generate BigQuery-safe schema based on the CSV headline.',
            },
            propertyOrder: 5,
          },
        },
        dependencies: {
          schemaSource: {not: {required: ['schemaFile']}},
          schemaFile: {not: {required: ['csvSafeColumnNames']}},
          csvSafeColumnNames: {not: {required: ['schemaSource']}},
        },
      },
      options: {
        type: 'object',
        format: 'grid-strict',
        options: JSON_SCHEMA_FUNCTIONS.getOnlyPropertiesObject({
          disable_collapse: false,
        }),
        required: [],
        defaultProperties: [
          'sourceFormat',
          'writeDisposition',
          'skipLeadingRows',
        ],
        properties: {
          writeDisposition: getWriteDisposition({propertyOrder: 1}),
          autodetect: {
            type: 'boolean',
            format: 'checkbox',
            options: {
              grid_columns: 4,
              infoText: 'Automatically detect schema for CSV and JSON sources.',
            },
            propertyOrder: 2,
          },
          ignoreUnknownValues: {
            type: 'boolean',
            format: 'checkbox',
            options: {
              grid_columns: 4,
              infoText:
                'Indicates if BigQuery should allow extra values that are not represented in the table schema.',
            },
            propertyOrder: 3,
          },
          sourceFormat: {
            type: 'string',
            enum: ['NEWLINE_DELIMITED_JSON', 'CSV'],
            options: {
              grid_columns: 4,
              infoText: ' The format of the data files.',
            },
            propertyOrder: 10,
          },
          skipLeadingRows: {
            type: 'number',
            default: 1,
            options: {
              dependencies: {sourceFormat: 'CSV'},
              grid_columns: 4,
              grid_break: true,
              infoText:
                'The number of rows at the top of a CSV file that BigQuery will skip when loading the data.',
            },
            propertyOrder: 11,
          },
        },
        links: [
          {
            rel: 'For more settings, see BigQuery JobConfigurationLoad',
            href: 'https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload',
          },
        ],
      },
    },
  };

  TASK_SCHEMA.create_external = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: [],
    defaultProperties: ['source', 'destination'],
    properties: {
      ancestor: getAncestor(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      source: {
        type: 'object',
        options: JSON_SCHEMA_FUNCTIONS.getFixedObject({
          disable_collapse: false,
        }),
        required: [],
        defaultProperties: ['sheet'],
        properties: {
          sheet: {
            type: 'object',
            format: 'grid-strict',
            options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject(),
            required: ['url', 'sheetName', 'skipLeadingRows'],
            properties: {
              url: {
                type: 'string',
                options: {grid_columns: 9},
                propertyOrder: 1,
              },
              sheetName: {
                type: 'string',
                options: {grid_columns: 3},
                propertyOrder: 2,
              },
              skipLeadingRows: {
                type: 'number',
                default: 1,
                minimum: 0,
                options: {grid_columns: 3},
                propertyOrder: 3,
              },
              safeColumnNames: {
                type: 'boolean',
                format: 'checkbox',
                options: {
                  grid_columns: 3,
                  infoText:
                    'Generate BigQuery-safe schema based on the Sheets headline.',
                },
                propertyOrder: 4,
              },
            },
          },
        },
      },
      destination: {
        type: 'object',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject({
          disable_collapse: false,
        }),
        required: [],
        defaultProperties: ['table'],
        properties: {
          table: getBigQueryTable('Create as BigQuery Table'),
          tableSchema: getBigQuerySchema({propertyOrder: 2}),
        },
      },
    },
  };

  TASK_SCHEMA.export_schema = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: [],
    defaultProperties: ['source', 'destination'],
    properties: {
      ancestor: getAncestor(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      source: Object.assign(
        getBigQueryTable('Source (BigQuery Table)', {disable_collapse: false}),
        {propertyOrder: 1000},
      ),
      destination: Object.assign(
        getStorageFile('Export Schema to Cloud Storage', {
          disable_collapse: false,
        }),
        {propertyOrder: 1001},
      ),
    },
  };

  TASK_SCHEMA.delete_bigquery = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: [],
    defaultProperties: ['source'],
    properties: {
      ancestor: getAncestor(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      source: Object.assign(
        getBigQueryToDelete('Target BigQuery Dataset or Table'),
        {propertyOrder: 1000},
      ),
    },
  };

  TASK_SCHEMA.data_transfer = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: [],
    defaultProperties: ['source'],
    properties: {
      ancestor: getAncestor(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      source: {
        type: 'object',
        title: 'BigQuery Data Transfer Config',
        format: 'grid-strict',
        options: JSON_SCHEMA_FUNCTIONS.getFixedObject(),
        required: [],
        defaultProperties: ['projectId', 'transferId', 'location'],
        properties: {
          projectId: {
            type: 'string',
            title: 'GCP Project Id',
            options: {grid_columns: 3},
            propertyOrder: 1,
          },
          transferId: {
            type: 'string',
            title: 'BigQuery Transfer Config Id',
            options: {grid_columns: 6},
            propertyOrder: 2,
          },
          location: {
            type: 'string',
            title: 'BigQuery Location',
            options: {grid_columns: 3},
            propertyOrder: 4,
          },
        },
      },
    },
  };

  TASK_SCHEMA.copy_gcs = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: [],
    defaultProperties: ['source', 'destination'],
    properties: {
      ancestor: getAncestor(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      source: Object.assign(
        getStorageFile('source (Cloud Storage Files)', {
          disable_collapse: false,
        }),
        {propertyOrder: 1000},
      ),
      destination: Object.assign(
        getStorageBucket('destination (Cloud Storage Bucket)', {
          disable_collapse: false,
        }),
        {propertyOrder: 1001},
      ),
    },
  };

  TASK_SCHEMA.download = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: [],
    defaultProperties: ['source', 'destination'],
    properties: {
      ancestor: getAncestor(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      source: {
        type: 'object',
        title: 'Source URL',
        options: JSON_SCHEMA_FUNCTIONS.getOnlyPropertiesObject({
          disable_collapse: false,
        }),
        required: [],
        defaultProperties: ['url'],
        properties: {
          url: {
            type: 'string',
            options: {
              grid_columns: 12,
            },
            propertyOrder: 1,
          },
          userAgent: {
            type: 'string',
            options: {
              grid_columns: 12,
            },
            propertyOrder: 2,
          },
        },
      },
      destination: Object.assign(
        getStorageFile('Download to Cloud Storage', {disable_collapse: false}),
        {propertyOrder: 1001},
      ),
    },
  };

  TASK_SCHEMA.knot = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: [],
    defaultProperties: ['embedded'],
    properties: {
      ancestor: getAncestor(),
      errorOptions: getErrorOptions(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      embedded: {
        type: 'object',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject({
          disable_collapse: false,
        }),
        format: 'grid-strict',
        required: [],
        defaultProperties: ['estimateRunningTime', 'dueTime', 'tasks'],
        properties: Object.assign(getAsyncTaskTimeOptions(), {
          tasks: getTaskGroup('Tasks'),
        }),
      },
    },
  };

  TASK_SCHEMA.multiple = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: [],
    defaultProperties: ['source', 'destination'],
    properties: {
      ancestor: getAncestor(),
      errorOptions: getErrorOptions(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      source: {
        type: 'object',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject({
          disable_collapse: false,
        }),
        required: [],
        defaultProperties: ['csv'],
        properties: {
          file: getStorageFile('Cloud Storage File'),
          csv: {
            type: 'object',
            title: 'CSV Data',
            options: JSON_SCHEMA_FUNCTIONS.getFixedObject(),
            properties: {
              header: {
                type: 'string',
                options: {
                  grid_columns: 12,
                },
                propertyOrder: 1,
              },
              records: {
                type: 'string',
                format: 'textarea',
                options: {
                  grid_columns: 12,
                },
                propertyOrder: 2,
              },
            },
            propertyOrder: 2,
          },
        },
        dependencies: {
          csv: {not: {required: ['file']}},
          file: {not: {required: ['csv']}},
        },
      },
      destination: {
        type: 'object',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject({
          disable_collapse: false,
        }),
        format: 'grid-strict',
        required: [],
        defaultProperties: [
          'taskId',
          'target',
          'recordSize',
          'qps',
          'message',
          'http',
        ],
        properties: {
          taskId: {
            type: 'string',
            title: 'Sentinel Task Id',
            options: {
              grid_columns: 4,
            },
            propertyOrder: 1,
          },
          target: {
            type: 'string',
            title: 'Distribute through',
            enum: ['pubsub', 'gcs'],
            options: {
              grid_columns: 4,
              enum_titles: ['Directly Pub/Sub Messages', 'Tentacles'],
            },
            propertyOrder: 2,
          },
          recordSize: {
            title: 'Number of Records Per Task',
            type: 'number',
            default: 1,
            minimum: 1,
            options: {
              grid_columns: 4,
              grid_break: true,
            },
            propertyOrder: 3,
          },
          qps: JSON_SCHEMA_FUNCTIONS.getQps({
            default: 1,
            options: {
              dependencies: {target: 'pubsub'},
              grid_break: true,
            },
            propertyOrder: 10,
          }),
          message: {
            type: 'object',
            title: 'Message Body',
            options: JSON_SCHEMA_FUNCTIONS.getFixedObject({grid_columns: 12}),
            required: [],
            properties: {},
            options: {
              grid_columns: 8,
              dependencies: {target: 'pubsub'},
            },
            propertyOrder: 11,
          },
          http: {
            type: 'object',
            title: 'Distributing Tasks via Tentacles',
            options: JSON_SCHEMA_FUNCTIONS.getFixedObject({
              grid_columns: 12,
              dependencies: {target: 'gcs'},
            }),
            required: [],
            properties: {
              service: getTentaclesHttpService({propertyOrder: 1}),
              config: {
                type: 'object',
                title: 'Pub/Sub message configuration',
                format: 'grid-strict',
                options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject(),
                required: [],
                defaultProperties: ['message'],
                properties:
                  JSON_SCHEMA_FUNCTIONS.getPubSubMessageConfigOption(),
              },
            },
          },
          // Deprecate generate file to trigger Tentacles GCS monitor.
          // file: Object.assign(
          //   getStorageFile(
          //     'Generate a Cloud Storage File to Trigger Tentacles',
          //     { dependencies: { target: 'gcs' }, }),
          //   { propertyOrder: 1001 }
          // ),
        },
      },
      multiple: {
        type: 'object',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject(),
        required: [],
        properties: getAsyncTaskTimeOptions(),
      },
    },
  };

  TASK_SCHEMA.speed_controlled = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: [],
    defaultProperties: ['source'],
    properties: {
      ancestor: getAncestor(),
      errorOptions: getErrorOptions(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      source: {
        type: 'object',
        title: 'Speed Controlling via Tentacles',
        format: 'grid-strict',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject({
          disable_collapse: false,
        }),
        required: [],
        defaultProperties: ['service', 'fileUrl', 'config'],
        properties: {
          service: getTentaclesHttpService({propertyOrder: 1}),
          fileUrl: {
            title: 'Tentacles File: gsutil URI',
            type: 'string',
            options: {
              grid_columns: 12,
            },
            propertyOrder: 2,
          },
          file: Object.assign(
            getStorageFile('Tentacles File: Cloud Storage Configuration'),
            {propertyOrder: 3},
          ),
          config: {
            type: 'object',
            title: 'Tentacles Task: Configuration',
            format: 'grid-strict',
            options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject({
              disable_collapse: false,
            }),
            required: [],
            defaultProperties: ['service', 'output'],
            properties: JSON_SCHEMA_FUNCTIONS.cloudFunctionInvokeProperties,
            propertyOrder: 20,
          },
          attributes: {
            type: 'object',
            title: 'Tentacles Task: Attributes',
            options: JSON_SCHEMA_FUNCTIONS.getOnlyPropertiesObject({
              grid_break: true,
            }),
            properties: {
              api: {
                type: 'string',
                options: {grid_columns: 6},
                default: 'CF',
              },
            },
            links: [
              {
                rel: 'Attributes from Tentacles naming convention',
                href: 'https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/main/marketing-analytics/activation/gmp-googleads-connector#33-name-convention-of-data-files',
              },
            ],
            propertyOrder: 100,
          },
        },
        dependencies: {
          fileUrl: {not: {required: ['file']}},
          file: {not: {required: ['fileUrl']}},
        },
      },
    },
  };

  TASK_SCHEMA.notify = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: [],
    defaultProperties: ['method', 'message'],
    properties: {
      ancestor: getAncestor(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      method: {
        type: 'object',
        title: 'Email service',
        format: 'grid-strict',
        options: JSON_SCHEMA_FUNCTIONS.getFixedObject({
          disable_collapse: false,
        }),
        required: [],
        defaultProperties: ['target', 'secretName', 'apiKey'],
        properties: {
          target: {
            type: 'string',
            enum: ['gmail', 'sendgrid'],
            options: {
              grid_columns: 4,
            },
            propertyOrder: 1,
          },
          secretName: {
            type: 'string',
            options: {
              grid_columns: 4,
              dependencies: {target: 'gmail'},
            },
            propertyOrder: 2,
          },
          apiKey: {
            type: 'string',
            options: {
              grid_columns: 4,
              dependencies: {target: 'sendgrid'},
            },
            propertyOrder: 3,
          },
        },
        propertyOrder: 1,
      },
      message: {
        type: 'object',
        title: 'Email Configuration',
        format: 'grid-strict',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject({
          disable_collapse: false,
        }),
        required: [],
        defaultProperties: ['from', 'to', 'type', 'subject', 'content'],
        properties: {
          from: {
            type: 'string',
            options: {
              grid_columns: 4,
            },
            propertyOrder: 1,
          },
          'reply-to': {
            type: 'string',
            options: {
              grid_columns: 4,
            },
            propertyOrder: 2,
          },
          type: {
            type: 'string',
            enum: ['text/plain', 'text/html'],
            options: {
              grid_columns: 4,
            },
            propertyOrder: 3,
          },
          to: {
            type: 'string',
            options: {
              grid_columns: 4,
            },
            propertyOrder: 10,
          },
          cc: {
            type: 'string',
            options: {
              grid_columns: 4,
            },
            propertyOrder: 11,
          },
          bcc: {
            type: 'string',
            options: {
              grid_columns: 4,
            },
            propertyOrder: 12,
          },
          subject: {
            type: 'string',
            options: {
              grid_columns: 12,
            },
            propertyOrder: 20,
          },
          content: {
            type: 'string',
            options: {
              grid_columns: 12,
              expand_height: true,
            },
            format: 'textarea',
            propertyOrder: 30,
          },
        },
      },
    },
  };

  TASK_SCHEMA.report = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    dynamic: {
      'source.target': 'source.config',
    },
    definitions: {
      CM: {
        type: 'object',
        title: 'Campaign Manager 360 Reports',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject(),
        required: [],
        defaultProperties: ['accountId', 'reportId', 'startDate', 'endDate'],
        properties: {
          accountId: {
            type: 'string',
            options: {
              grid_columns: 4,
            },
            propertyOrder: 1,
          },
          profileId: {
            type: 'string',
            options: {
              grid_columns: 4,
            },
            propertyOrder: 2,
          },
          reportId: {
            type: 'string',
            options: {
              grid_columns: 4,
              grid_break: true,
            },
            propertyOrder: 3,
          },
          startDate: {
            type: 'string',
            options: {
              grid_columns: 4,
              infoText: 'Expected format: YYYY-MM-DD',
            },
            propertyOrder: 10,
          },
          endDate: {
            type: 'string',
            options: {
              grid_columns: 4,
              infoText: 'Expected format: YYYY-MM-DD',
            },
            propertyOrder: 11,
          },
          secretName: JSON_SCHEMA_FUNCTIONS.getSecretName(),
        },
      },
      DV360: {
        type: 'object',
        title: 'Display & Video 360 Reports',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject(),
        required: [],
        defaultProperties: ['queryId', 'requestBody'],
        properties: {
          queryId: {
            type: 'string',
            options: {
              grid_columns: 4,
            },
            propertyOrder: 1,
          },
          secretName: JSON_SCHEMA_FUNCTIONS.getSecretName({propertyOrder: 2}),
          requestBody: {
            type: 'object',
            options: JSON_SCHEMA_FUNCTIONS.getFixedObject({titleHidden: true}),
            properties: {
              dataRange: {
                type: 'object',
                options: JSON_SCHEMA_FUNCTIONS.getFixedObject(),
                required: [],
                defaultProperties: [
                  'range',
                  'customStartDate',
                  'customEndDate',
                ],
                properties: {
                  range: {
                    type: 'string',
                    enum: [
                      'CUSTOM_DATES',
                      'CURRENT_DAY',
                      'PREVIOUS_DAY',
                      'WEEK_TO_DATE',
                      'MONTH_TO_DATE',
                      'QUARTER_TO_DATE',
                      'YEAR_TO_DATE',
                      'PREVIOUS_WEEK',
                      'PREVIOUS_MONTH',
                      'PREVIOUS_QUARTER',
                      'PREVIOUS_YEAR',
                      'LAST_7_DAYS',
                      'LAST_30_DAYS',
                      'LAST_90_DAYS',
                      'LAST_365_DAYS',
                      'ALL_TIME',
                      'LAST_14_DAYS',
                      'LAST_60_DAYS',
                    ],
                    propertyOrder: 1,
                  },
                  customStartDate: getDateProperties({
                    options: JSON_SCHEMA_FUNCTIONS.getFixedObject({
                      dependencies: {range: 'CUSTOM_DATES'},
                    }),
                    propertyOrder: 10,
                  }),
                  customEndDate: getDateProperties({
                    options: JSON_SCHEMA_FUNCTIONS.getFixedObject({
                      dependencies: {range: 'CUSTOM_DATES'},
                    }),
                    propertyOrder: 11,
                  }),
                },
                links: [
                  {
                    rel: 'DataRange in DV360 RunQueryRequest',
                    href: 'https://developers.google.com/bid-manager/reference/rest/v2/queries#DataRange',
                  },
                ],
              },
            },
            propertyOrder: 10,
          },
        },
      },
      SA360: {
        type: 'object',
        title: 'Search Ads 360 Reports',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject(),
        required: [],
        defaultProperties: [
          'loginCustomerId',
          'customerId',
          'snakeCase',
          'query',
        ],
        properties: {
          loginCustomerId: {
            type: 'string',
            options: {
              grid_columns: 4,
            },
            propertyOrder: 1,
          },
          customerId: {
            type: 'string',
            options: {
              grid_columns: 4,
            },
            propertyOrder: 2,
          },
          secretName: JSON_SCHEMA_FUNCTIONS.getSecretName({propertyOrder: 3}),
          customColumnOutput: {
            type: 'string',
            title: 'Custom Columns Output Format',
            enum: ['json', 'struct', 'raw'],
            options: {
              grid_columns: 4,
              infoText:
                'Format of custom column values in report files and the schema when the data is loaded as a BigQuery table.',
            },
            propertyOrder: 4,
          },
          snakeCase: {
            type: 'boolean',
            format: 'checkbox',
            options: {
              grid_columns: 4,
              infoText:
                'Convert JSON objects in the output files to snake case.',
            },
            propertyOrder: 5,
          },
          query: JSON_SCHEMA_FUNCTIONS.getSqlObject(),
          file: Object.assign(getStorageFile('Query file in Cloud Storage'), {
            propertyOrder: 11,
          }),
        },
        dependencies: {
          query: {not: {required: ['file']}},
          file: {not: {required: ['query']}},
        },
      },
      ADS: {
        type: 'object',
        title: 'Google Ads Reports',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject(),
        required: [],
        defaultProperties: [
          'loginCustomerId',
          'customerId',
          'developerToken',
          'snakeCase',
          'query',
        ],
        properties: {
          loginCustomerId: {
            type: 'string',
            options: {
              grid_columns: 4,
            },
            propertyOrder: 1,
          },
          customerId: {
            type: 'string',
            options: {
              grid_columns: 4,
            },
            propertyOrder: 2,
          },
          developerToken: {
            type: 'string',
            options: {
              grid_columns: 4,
            },
            propertyOrder: 3,
          },
          secretName: JSON_SCHEMA_FUNCTIONS.getSecretName({propertyOrder: 10}),
          snakeCase: {
            type: 'boolean',
            format: 'checkbox',
            options: {
              grid_columns: 4,
              infoText:
                'Convert JSON objects in the output files to snake case.',
            },
            propertyOrder: 11,
          },
          query: JSON_SCHEMA_FUNCTIONS.getSqlObject({propertyOrder: 20}),
          file: Object.assign(getStorageFile('Query file in Cloud Storage'), {
            propertyOrder: 21,
          }),
          reportQuery: {
            type: 'object',
            title: '(Legacy) ReportQuery',
            options: {
              grid_columns: 12,
              disable_collapse: false,
            },
            required: ['entity'],
            defaultProperties: ['entity', 'attributes', 'segments', 'metrics'],
            properties: {
              entity: {
                type: 'string',
                options: {
                  grid_columns: 12,
                },
              },
              attributes: {
                type: 'array',
                options: {
                  grid_columns: 4,
                  disable_collapse: false,
                },
                items: {
                  type: 'string',
                  title: 'Attribute',
                },
              },
              segments: {
                type: 'array',
                options: {
                  grid_columns: 4,
                  disable_collapse: false,
                },
                items: {
                  type: 'string',
                  title: 'Segment',
                },
              },
              metrics: {
                type: 'array',
                options: {
                  grid_columns: 4,
                  disable_collapse: false,
                },
                items: {
                  type: 'string',
                  title: 'Metric',
                },
              },
              constraints: {
                options: {
                  grid_columns: 12,
                  disable_collapse: false,
                },
                oneOf: [
                  {
                    type: 'array',
                    items: {
                      type: 'object',
                      title: 'Constrain',
                      properties: {
                        key: {type: 'string', options: {grid_columns: 6}},
                        op: {type: 'string', options: {grid_columns: 2}},
                        val: {type: 'string', options: {grid_columns: 4}},
                      },
                    },
                  },
                  {type: 'object'},
                ],
              },
              from_date: {
                type: 'string',
                options: {
                  grid_columns: 4,
                },
              },
              to_date: {
                type: 'string',
                options: {
                  grid_columns: 4,
                },
              },
              date_constant: {
                type: 'string',
                options: {
                  grid_columns: 4,
                },
              },
              limit: {
                type: 'number',
                options: {
                  grid_columns: 4,
                },
              },
            },
            propertyOrder: 22,
          },
        },
        dependencies: {
          query: {not: {required: ['file']}},
          file: {not: {required: ['reportQuery']}},
          reportQuery: {not: {required: ['query']}},
        },
      },
      API: {
        type: 'object',
        title: 'General Google API data',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject(),
        required: [],
        defaultProperties: ['api', 'resource', 'entityPath'],
        properties: {
          api: {
            type: 'string',
            enum: ['YouTube', 'DisplayVideo', 'Analytics', 'DfaReporting'],
            options: {
              grid_columns: 4,
              enum_titles: [
                'YouTube API',
                'Display & Video 360 API',
                'Google Analytics API',
                'Campaign Manager 360 API',
              ],
            },
            propertyOrder: 1,
          },
          resource: {
            type: 'string',
            options: {grid_columns: 4},
            propertyOrder: 2,
          },
          secretName: JSON_SCHEMA_FUNCTIONS.getSecretName({propertyOrder: 4}),
          entityPath: {
            type: 'string',
            options: {grid_columns: 4},
            propertyOrder: 10,
          },
          pageTokenPath: {
            type: 'string',
            options: {grid_columns: 4},
            propertyOrder: 11,
          },
          functionName: {
            type: 'string',
            default: 'list',
            options: {grid_columns: 2},
            propertyOrder: 12,
          },
          limit: {
            type: 'number',
            options: {
              grid_columns: 2,
              infoText:
                'The maximum number of resources will be downloaded. 0 stands for no limit.',
            },
            propertyOrder: 13,
          },
          fieldMask: {
            type: 'string',
            options: {grid_columns: 12},
            format: 'textarea',
            propertyOrder: 20,
          },
          args: {
            type: 'object',
            options: {grid_columns: 12},
            format: 'textarea',
            propertyOrder: 30,
          },
        },
      },
    },
    required: [],
    defaultProperties: ['source', 'destination'],
    properties: {
      ancestor: getAncestor(),
      errorOptions: getErrorOptions(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      source: {
        type: 'object',
        format: 'grid-strict',
        options: JSON_SCHEMA_FUNCTIONS.getFixedObject({
          disable_collapse: false,
        }),
        title: 'Report Definition',
        required: [],
        defaultProperties: ['target', 'config'],
        properties: {
          target: {
            type: 'string',
            title: 'Target System',
            enum: ['DV360', 'CM', 'SA360', 'ADS', 'API'],
            options: {
              grid_columns: 6,
              grid_break: true,
              enum_titles: [
                'Display & Video 360 Reports',
                'Campaign Manager 360 Reports',
                'Search Ads 360 Reports',
                'Google Ads Reports',
                'General Google API Data, e.g. YouTube, DV360, etc.',
              ],
            },
          },
          config: {
            type: 'object',
            title: 'Report Configuration',
            options: {titleHidden: true},
            format: 'grid-strict',
            $ref: '#/definitions/CM',
          },
        },
        propertyOrder: 10,
      },
      destination: Object.assign(
        getStorageFile('Report Output to Cloud Storage', {
          disable_collapse: false,
        }),
        {propertyOrder: 20},
      ),
    },
  };

  TASK_SCHEMA.predict = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    definitions: {
      resource: {
        type: 'object',
        format: 'grid-strict',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject({
          disable_collapse: false,
        }),
        required: [],
        defaultProperties: ['table'],
        properties: {
          table: getBigQueryTable(),
          file: getStorageFile('Cloud Storage File'),
          format: {
            type: 'string',
            title: 'File Format',
            options: {grid_columns: 4},
            propertyOrder: 4,
          },
        },
        dependencies: {
          table: {not: {required: ['file']}},
          file: {not: {required: ['table']}},
        },
      },
    },
    required: [],
    defaultProperties: ['model', 'source', 'destination'],
    properties: {
      ancestor: getAncestor(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      model: {
        type: 'object',
        format: 'grid-strict',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject({
          disable_collapse: false,
        }),
        required: [],
        defaultProperties: ['api', 'projectId', 'location', 'modelId'],
        properties: {
          api: {
            type: 'string',
            title: 'Prediction Service',
            enum: ['vertex'],
            options: {
              grid_columns: 4,
              enum_titles: ['Vertex AI'],
            },
            propertyOrder: 1,
          },
          projectId: {
            type: 'string',
            title: 'GCP Project Id',
            options: {grid_columns: 4},
            propertyOrder: 2,
          },
          location: {
            type: 'string',
            title: 'Location',
            options: {grid_columns: 4},
            propertyOrder: 3,
          },
          modelId: {
            type: 'string',
            title: 'Model Id',
            options: {grid_columns: 6},
            propertyOrder: 10,
          },
          displayName: {
            type: 'string',
            title: 'Display Name',
            options: {grid_columns: 6},
            propertyOrder: 11,
          },
        },
        propertyOrder: 1,
      },
      source: {
        $ref: '#/definitions/resource',
      },
      destination: {
        $ref: '#/definitions/resource',
      },
    },
  };

  TASK_SCHEMA.execute_notebook = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: ['source'],
    properties: {
      ancestor: getAncestor(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      source: {
        type: 'object',
        format: 'grid-strict',
        options: JSON_SCHEMA_FUNCTIONS.getFixedObject({
          titleHidden: true,
        }),
        required: ['projectId', 'locationId', 'template'],
        properties: {
          projectId: {
            type: 'string',
            title: 'GCP Project Id',
            options: {grid_columns: 6},
            propertyOrder: 1,
          },
          locationId: {
            type: 'string',
            title: 'Location',
            options: {grid_columns: 6},
            propertyOrder: 2,
          },
          template: {
            title: 'ExecutionTemplate',
            options: JSON_SCHEMA_FUNCTIONS.getFixedObject(),
            format: 'grid-strict',
            required: [
              'inputNotebookFile',
              'outputNotebookFolder',
              'vertexAiParameters',
            ],
            properties: {
              inputNotebookFile: {
                type: 'string',
                options: {
                  grid_columns: 12,
                  infoText:
                    'Path to the notebook file to execute. Must be in a Google Cloud Storage bucket.',
                },
                propertyOrder: 1,
              },
              outputNotebookFolder: {
                type: 'string',
                options: {
                  grid_columns: 12,
                  infoText:
                    'Path to the notebook folder to write to. Must be in a Google Cloud Storage bucket path.',
                },
                propertyOrder: 2,
              },
              vertexAiParameters: {
                type: 'object',
                options: JSON_SCHEMA_FUNCTIONS.getFixedObject(),
                propertyOrder: 3,
                properties: {
                  env: {
                    type: 'object',
                    options: {grid_columns: 12},
                  },
                },
              },
            },
            links: [
              {
                rel: 'For more details, see ExecutionTemplate',
                href: 'https://cloud.google.com/vertex-ai/docs/workbench/reference/rest/v1/ExecutionTemplate',
              },
            ],
          },
        },
      },
    },
  };

  TASK_SCHEMA.invoke_cloud_function = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: ['service'],
    properties: {
      ancestor: getAncestor(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      service: {
        type: 'object',
        format: 'grid-strict',
        options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject(),
        required: ['url'],
        properties: Object.assign(
          {},
          JSON_SCHEMA_FUNCTIONS.cloudFunctionServiceProperties,
          {
            args: {
              type: 'array',
              title: 'Arguments for the Python Function',
              options: {grid_columns: 12},
              items: {
                title: 'Argument',
                anyOf: [{type: 'string'}, {type: 'number'}, {type: 'object'}],
              },
              propertyOrder: 20,
            },
          },
        ),
      },
    },
  };

  TASK_SCHEMA.execute_run_job = {
    type: 'object',
    options: JSON_SCHEMA_FUNCTIONS.getTopOptions(),
    required: ['source'],
    properties: {
      ancestor: getAncestor(),
      appendedParameters: getAppendedParameters(),
      next: getTaskGroup(),
      source: {
        type: 'object',
        format: 'grid-strict',
        // options: JSON_SCHEMA_FUNCTIONS.getOptionalPropertiesObject(),

        options: JSON_SCHEMA_FUNCTIONS.getFixedObject({
          titleHidden: true,
        }),

        required: ['projectId', 'locationId', 'runJobId'],
        properties: Object.assign({
          projectId: {
            type: 'string',
            title: 'GCP Project Id',
            options: {grid_columns: 4},
            propertyOrder: 2,
          },
          locationId: {
            type: 'string',
            title: 'Location',
            options: {grid_columns: 4},
            propertyOrder: 3,
          },
          runJobId: {
            type: 'string',
            title: 'Cloud Run Job Name',
            options: {grid_columns: 4},
            propertyOrder: 10,
          },
          jobParameters: {
            type: 'object',
            title: 'Job Parameters',
            options: {grid_columns: 12},
            propertyOrder: 11,
            properties: {
              taskParameters: getStorageFile(
                'Task Parameters File in Cloud Storage',
              ),
              args: {
                type: 'object',
                title: 'Common Parameters for the Cloud Run Job',
                options: {grid_columns: 12},
                propertyOrder: 20,
              },
            },
          },
        }),
      },
    },
  };

  return TASK_SCHEMA;
}

/**
 * Returns the Sentinel task JSON schema string for a specific task type
 * or all task types if omitted. Uses memoized cache for performance.
 * @param {string=} taskType
 * @return {string}
 */
function getSentinelTaskSchema(taskType) {
  if (!cachedSentinelTaskSchemas) {
    cachedSentinelTaskSchemas = buildSentinelTaskSchemas_();
    cachedSentinelTaskSchemaStr = JSON.stringify(cachedSentinelTaskSchemas);
  }
  if (taskType) {
    return JSON.stringify(
      cachedSentinelTaskSchemas[taskType] || {type: 'object'},
    );
  }
  return cachedSentinelTaskSchemaStr;
}
