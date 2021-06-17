# Data Tasks Coordinator

<!--* freshness: { owner: 'lushu' reviewed: '2021-06-03' } *-->

Disclaimer: This is not an official Google product.

Data Tasks Coordinator (code name **Sentinel**) is a framework that organizes
different tasks into an automatic data pipeline, plus a library that conducts
interactive installation processes based on configurations.

This solution is not a general purposed orchestration solution. It is designed
for marketing solutions with some specified tasks, mainly related to BigQuery.

## 1. Key Concepts

### 1.1. Terminology

`Task`: A data processing element for a specific objective, e.g. loading data
from Cloud Storage to BigQuery.

[`Data pipeline`](https://en.wikipedia.org/wiki/Pipeline_(computing)): A series
of connected data processing tasks. The output of one task is the input of the
next one. The tasks of a pipeline are often executed in parallel or in
time-sliced fashion.

[`Event-driven programming`](
https://en.wikipedia.org/wiki/Event-driven_programming): A programming paradigm
in which events, such as user activities or execution results from other
programming threads, determine the flow of a program's execution.

### 1.2. To start a task

A `task` can be triggered by a Pub/Sub message.

In most cases, a Cloud Scheduler job created by `deploy.sh` will send those
messages regularly to trigger the first task. After that, Sentinel will send
messages to trigger next tasks.

* Message attribute: `taskId`
* Message body: the JSON string of the parameter object that will be passed into
  the task to start.

Task definitions and sql files support parameters in this format:
`${parameterName}`. The placeholders will be replaced with the value of the
`parameterName` in the passed-in parameter JSON object.

Embedded parameters are supported, e.g. `${parameter.name}`.

Each task will pass a parameter object to its next task(s). The passed-on
parameter object is the merge result of the parameter that it receives, the new
parameters that it generates, and the parameters from the configuration item
`appendedParameters` if it exists.

### 1.3. General task definition

```text
{
 "foo": {
   "type": "query",
   "source": {
     ...
   },
   "destination": {
     ...
   },
   "options": {
     ...
   },
   "next": "bar"
 }
}
```

Properties:

1. `foo` is the task name.
1. `type`, task type. Different types define the details of the task also have
   different configurations.
1. `source`, `destination` and `options` are configurations. Refer to the
   detailed tasks for more information.
1. `next`, defines what next task(s) will be started after the current one
   completed, in this case, task `bar` will be started after `foo`.

See `config_task.json.template` for templates of tasks.

### 1.4. To install the solution

In a [Cloud Shell](https://cloud.google.com/shell/):

1. clone the source code;
1. enter the source code folder, edit the task configuration JSON file;
1. run `chmod a+x ./deploy.sh; ./deploy.sh`.

## 2. Task Configuration Examples

### 2.1. Load Task

#### 2.1.1. Load CSV file(s) with given schema

```json
{
  "load_job": {
    "type": "load",
    "source": {
      "bucket": "[YOUR_STORAGE_BUCKET_ID]",
      "name": "[YOUR_FILENAME]"
    },
    "destination": {
      "table": {
        "projectId": "[YOUR_CLOUD_PROJECT_ID]",
        "datasetId": "[YOUR_BIGQUERY_DATASET_ID]",
        "tableId": "[YOUR_BIGQUERY_TABLE_ID]",
        "location": "[YOUR_BIGQUERY_LOCATION_ID]"
      },
      "tableSchema": {
        "schema": {
          "fields": [
            {
              "mode": "NULLABLE",
              "name": "[YOUR_BIGQUERY_TABLE_COLUMN_1_NAME]",
              "type": "[YOUR_BIGQUERY_TABLE_COLUMN_1_TYPE]"
            },
            {
              "mode": "NULLABLE",
              "name": "[YOUR_BIGQUERY_TABLE_COLUMN_2_NAME]",
              "type": "[YOUR_BIGQUERY_TABLE_COLUMN_2_TYPE]"
            }
          ]
        }
      }
    },
    "options": {
      "sourceFormat": "CSV",
      "writeDisposition": "WRITE_TRUNCATE",
      "skipLeadingRows": 1,
      "autodetect": false
    }
  }
}
```

#### 2.1.2. Load CSV file(s) with autodetected schema

```json
{
  "load_job": {
    "type": "load",
    "source": {
      "bucket": "[YOUR_STORAGE_BUCKET_ID]",
      "name": "[YOUR_FILENAME]"
      ]
    },
    "destination": {
      "table": {
        "projectId": "[YOUR_CLOUD_PROJECT_ID]",
        "datasetId": "[YOUR_BIGQUERY_DATASET_ID]",
        "tableId": "targetTable$${partitionDay}",
        "location": "[YOUR_BIGQUERY_LOCATION_ID]"
      }
    },
    "options": {
      "sourceFormat": "CSV",
      "writeDisposition": "WRITE_TRUNCATE",
      "skipLeadingRows": 1,
      "autodetect": true
    }
  }
}
```

### 2.2. Query task

#### 2.2.1. Query through a simple SQL string

```json
{
  "query_job_sql": {
    "type": "query",
    "source": {
      "sql": "[YOUR_QUERY_SQL]"
    },
    "destination": {
      "table": {
        "projectId": "[YOUR_CLOUD_PROJECT_ID]",
        "datasetId": "[YOUR_BIGQUERY_DATASET_ID]",
        "tableId": "[YOUR_BIGQUERY_TABLE_ID]"
      },
      "writeDisposition": "WRITE_TRUNCATE"
    }
  }
}
```

#### 2.2.2. Query through a Cloud Storage file

```json
{
  "query_job_gcs": {
    "type": "query",
    "source": {
      "file": {
        "bucket": "[YOUR_BUCKET_FOR_SQL_FILE]",
        "name": "[YOUR_SQL_FILE_FULL_PATH_NAME]"
      }
    },
    "destination": {
      "table": {
        "projectId": "[YOUR_CLOUD_PROJECT_ID]",
        "datasetId": "[YOUR_BIGQUERY_DATASET_ID]",
        "tableId": "[YOUR_BIGQUERY_TABLE_ID]"
      },
      "writeDisposition": "WRITE_TRUNCATE"
    }
  }
}
```

### 2.3. Export task

#### 2.3.1. Export a table to Cloud Storage

```json
{
  "export_job": {
    "type": "export",
    "source": {
      "projectId": "[YOUR_CLOUD_PROJECT_ID]",
      "datasetId": "[YOUR_BIGQUERY_DATASET_ID]",
      "tableId": "[YOUR_BIGQUERY_TABLE_ID]",
      "location": "[YOUR_BIGQUERY_LOCATION_ID]"
    },
    "destination": {
      "bucket": "[YOUR_BUCKET_FOR_EXPORTED_FILE]",
      "name": "[YOUR_FULL_PATH_NAME_FOR_EXPORTED_FILE]"
    },
    "options": {
      "destinationFormat": "NEWLINE_DELIMITED_JSON",
      "printHeader": false
    }
  }
}
```

#### 2.3.2. Export a file to trigger Tentacles (usually after a ‘Query’ task)

```json
{
  "export_for_tentacles": {
    "type": "export",
    "source": {
      "projectId": "${destinationTable.projectId}",
      "datasetId": "${destinationTable.datasetId}",
      "tableId": "${destinationTable.tableId}",
      "location": "#DATASET_LOCATION#"
    },
    "destination": {
      "bucket": "#GCS_BUCKET#",
      "name": "#OUTBOUND#/API[MP]_config[test]_${partitionDay}.ndjson"
    },
    "options": {
      "destinationFormat": "NEWLINE_DELIMITED_JSON",
      "printHeader": false
    }
  }
}
```
