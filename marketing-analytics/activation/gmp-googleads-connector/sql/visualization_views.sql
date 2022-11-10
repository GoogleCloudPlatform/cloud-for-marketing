-- Copyright 2022 Google LLC.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

CREATE OR REPLACE VIEW `${datasetId}.RawJson` AS
  SELECT
    timestamp,
    REGEXP_EXTRACT(message, r"\[([^]]+)\]")   AS entity,
    REGEXP_EXTRACT(message, r"\[[^]]+\](.*)") AS json
  FROM (
    SELECT
      REPLACE(jsonpayload.message, "\n","")   AS message,
      timestamp
    FROM
      `${projectId}.${datasetId}.${logTable}`
  );

CREATE OR REPLACE VIEW `${datasetId}.RawTask` AS
  SELECT
    timestamp,
    JSON_VALUE(json, "$.action")         AS action,
    JSON_VALUE(json, "$.fileId")         AS fileId,
    JSON_VALUE(json, "$.id")             AS id,
    JSON_VALUE(json, "$.api")            AS api,
    JSON_VALUE(json, "$.config")         AS config,
    JSON_VALUE(json, "$.start")          AS startIndex,
    JSON_VALUE(json, "$.end")            AS endIndex,
    JSON_VALUE(json, "$.status")         AS status,
    JSON_VALUE(json, "$.dryRun")         AS dryRun,
    JSON_VALUE(json, "$.createdAt")      AS createdAt,
    JSON_VALUE(json, "$.dataMessageId")  AS dataMessageId,
    JSON_VALUE(json, "$.startSending")   AS startSending,
    JSON_VALUE(json, "$.apiMessageId")   AS apiMessageId,
    JSON_VALUE(json, "$.finishedTime")   AS finishedTime,
    JSON_VALUE(json, "$.error")          AS error,
    JSON_VALUE(json, "$.numberOfLines")  AS numberOfLines,
    JSON_VALUE(json, "$.numberOfFailed") AS numberOfFailed
  FROM
    `${projectId}.${datasetId}.RawJson`
  WHERE
    entity = "TentaclesTask"
  ORDER BY
    timestamp DESC;


CREATE OR REPLACE VIEW `${datasetId}.RawFile` AS
  SELECT
    timestamp,
    JSON_VALUE(json, "$.action")            AS action,
    JSON_VALUE(json, "$.fileId")            AS fileId,
    JSON_VALUE(json, "$.name")              AS name,
    JSON_VALUE(json, "$.size")              AS fileSize,
    JSON_VALUE(json, "$.updated")           AS updated,
    JSON_VALUE(json, "$.error")             AS error,
    JSON_VALUE(json, "$.attributes.api")    AS api,
    JSON_VALUE(json, "$.attributes.config") AS config,
    JSON_VALUE(json, "$.attributes.dryRun") AS dryRun,
    JSON_VALUE(json, "$.attributes.size")   AS splitSize,
    JSON_VALUE(json, "$.attributes.gcs")    AS gcs
  FROM
    `${projectId}.${datasetId}.RawJson`
  WHERE
    entity = "TentaclesFile"
  ORDER BY
    timestamp DESC;

CREATE OR REPLACE VIEW `${datasetId}.RawFailedRecord` AS
  SELECT
    timestamp                            AS recordUpdatedTime,
    JSON_VALUE(json, "$.taskId")         AS taskId,
    JSON_VALUE(json, "$.error")          AS recordError,
    JSON_VALUE_ARRAY(json, "$.records")  AS records
  FROM
    `${projectId}.${datasetId}.RawJson`
  WHERE
    entity = "TentaclesFailedRecord"
  ORDER BY
    timestamp DESC;

CREATE OR REPLACE VIEW `${datasetId}.TentaclesTask` AS
  SELECT
    "${namespace}"                                 AS namespace,
    "${projectId}"                                 AS projectId,
    main.* EXCEPT (error, numberOfLines, numberOfFailed),
    status,
    CASE
      WHEN status = "sending"
        AND DATETIME_DIFF(CURRENT_DATETIME(), startSending, MINUTE) > 9
      THEN "error"
      ELSE status
    END AS taskStatus,
    CASE
      WHEN status = "sending"
        AND DATETIME_DIFF(CURRENT_DATETIME(), startSending, MINUTE) > 9
      THEN "Timeout"
      ELSE error
    END AS taskError,
    DATETIME_DIFF(finishedTime,startSending,SECOND) AS sendingTime,
    DATETIME_DIFF(startSending,createdAt,SECOND)    AS waitingTime,
    CAST(numberOfLines AS INT64)                    AS numberOfLines,
    CAST(numberOfFailed AS INT64)                   AS numberOfFailed
  FROM (
    SELECT
      id                                                       AS taskId,
      ANY_VALUE(fileId)                                        AS fileId,
      ANY_VALUE(api)                                           AS api,
      ANY_VALUE(config)                                        AS config,
      ANY_VALUE(dryRun)                                        AS dryRun,
      ANY_VALUE(error)                                         AS error,
      CAST(ANY_VALUE(startIndex) AS INT64)                     AS startIndex,
      CAST(ANY_VALUE(endIndex) AS INT64)                       AS endIndex,
      PARSE_DATETIME("%FT%H:%M:%E3SZ",ANY_VALUE(createdAt))    AS createdAt,
      PARSE_DATETIME("%FT%H:%M:%E3SZ",ANY_VALUE(startSending)) AS startSending,
      PARSE_DATETIME("%FT%H:%M:%E3SZ",ANY_VALUE(finishedTime)) AS finishedTime,
      ANY_VALUE(dataMessageId)                                 AS dataMessageId,
      ANY_VALUE(apiMessageId)                                  AS apiMessageId,
      MAX(timestamp)                                           AS lastTimestamp,
      ANY_VALUE(numberOfLines)                                 AS numberOfLines,
      ANY_VALUE(numberOfFailed)                                AS numberOfFailed
    FROM
      `${projectId}.${datasetId}.RawTask`
    GROUP BY
      id
    ) AS main
  LEFT JOIN (
    SELECT
      id, timestamp, status
    FROM
      `${projectId}.${datasetId}.RawTask`) AS latest
  ON
    main.taskId=latest.id AND main.lastTimestamp=latest.timestamp
  ORDER BY
    lastTimestamp DESC;

CREATE OR REPLACE VIEW `${datasetId}.TentaclesReport` AS
  SELECT
    "${namespace}"                                 AS namespace,
    "${projectId}"                                 AS projectId,
    "${bucket}"                                    AS bucket,
    file.* EXCEPT (name, fileSize),
    name                                               AS fileFullName,
    REGEXP_REPLACE(file.name, "processed/[^/]*/", "")  AS fileName,
    CAST(file.fileSize AS INT64)                       AS fileSize,
    PARSE_DATETIME("%FT%H:%M:%E3SZ", file.updatedTime) As fileUpdatedTime,
    IFNULL(taskLastUpdatedTime, PARSE_DATETIME("%FT%H:%M:%E3SZ",
      file.updatedTime))                               AS fileLastUpdatedTime,
    taskSummary.* EXCEPT (fileId, failedTask, taskNumber),
    IFNULL(taskNumber, 0)                              AS taskNumber,
    IFNULL(taskError, fileError)                       AS errorMessage,
    CASE
      WHEN (IFNULL(failedTask, fileError)  IS NOT NULL)
      THEN "error"
      WHEN (taskNumber = doneNumber)
      THEN "done"
      ELSE "processing"
    END                                                AS fileStatus,
    task.* EXCEPT (fileId, api, config, dryRun, namespace, projectId),
    failedRecord.* EXCEPT (taskId)
  FROM (
    SELECT
      fileId,
      ANY_VALUE(name)                    AS name,
      ANY_VALUE(fileSize)                AS fileSize,
      ANY_VALUE(updated)                 AS updatedTime,
      ANY_VALUE(api)                     AS api,
      ANY_VALUE(config)                  AS config,
      CAST(ANY_VALUE(dryRun) AS BOOLEAN) AS dryRun,
      ANY_VALUE(error)                   AS fileError
    FROM
      `${projectId}.${datasetId}.RawFile`
    WHERE
      fileId IS NOT NULL
    GROUP BY
      fileId
  ) AS file
  LEFT OUTER JOIN (
    SELECT
      fileId,
      COUNT(taskId)                        AS taskNumber,
      COUNTIF(taskStatus = "done")         AS doneNumber,
      COUNTIF(taskStatus = "queuing")      AS queuingNumber,
      COUNTIF(taskStatus = "sending")      AS sendingNumber,
      COUNTIF(taskStatus = "error")        AS errorNumber,
      COUNTIF(taskStatus = "failed")       AS failedNumber,
      ANY_VALUE(taskError)                 AS failedTask,
      CAST(MAX(lastTimestamp) AS DATETIME) AS taskLastUpdatedTime
    FROM
      `${projectId}.${datasetId}.TentaclesTask`
    GROUP BY
      fileId
  )  AS taskSummary
  ON
    file.fileId = taskSummary.fileId
  LEFT OUTER JOIN (
    SELECT
      *
    FROM
      `${projectId}.${datasetId}.TentaclesTask`
  )  AS task
  ON
    file.fileId = task.fileId
  LEFT OUTER JOIN (
    SELECT
      *
    FROM
      `${projectId}.${datasetId}.RawFailedRecord` as record
  ) AS failedRecord
  ON
    task.taskId = failedRecord.taskId
  WHERE
    file.api IS NOT NULL
  ORDER BY
    updatedTime DESC;

CREATE OR REPLACE VIEW `${datasetId}.TentaclesBlockage` AS
  WITH blockedApis AS (
    SELECT
      api,
      DATETIME_DIFF(CURRENT_TIMESTAMP(), MAX(lastTimestamp), MINUTE) AS minutes
    FROM
      `${projectId}.${datasetId}.TentaclesTask`
    WHERE
      api IN (
        SELECT
          DISTINCT api
        FROM
          `${projectId}.${datasetId}.TentaclesTask`
        WHERE
          taskStatus = "queuing"
      )
    GROUP BY
      api
    HAVING
      minutes>9 )
  SELECT
    *
  FROM
    `${projectId}.${datasetId}.TentaclesTask`
  WHERE
    taskStatus = "queuing" AND api IN (SELECT api FROM blockedApis);
