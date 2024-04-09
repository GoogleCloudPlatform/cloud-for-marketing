#!/usr/bin/env bash
#
# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Only in standalone mode, import the basic install script.
if [[ "${BASH_SOURCE[0]}" -ef "$0" ]]; then
  source "${BASE_DIR}/../common-libs/nodejs-common/bin/install_functions.sh"
fi

# Solution name.
SOLUTION_NAME="tentacles"

# Project namespace will be used as prefix of the name of Cloud Functions,
# Pub/Sub topics, etc.
# Default project namespace is SOLUTION_NAME.
# Note: only lowercase letters, numbers and dashes(-) are allowed.
PROJECT_NAMESPACE="${SOLUTION_NAME}"

# Project configuration file.
CONFIG_FILE="./config.json"

# Parameter name used by functions to load and save config.
CONFIG_ITEMS=(
  "PROJECT_NAMESPACE"
  "REGION"
  "GCS_BUCKET"
  "DATABASE_ID"
  "DATABASE_MODE"
  "SECRET_NAME"
  "OUTBOUND"
  "ENABLE_VISUALIZATION"
  "DATASET"
  "DATASET_LOCATION"
)

# Default name for BigQuery dataset to receive logs.
DATASET="${SOLUTION_NAME}"
# BigQuery table name for logs.
BIGQUERY_LOG_TABLE="winston_log"

# The Google Cloud APIs that will be used in Tentacles.
GOOGLE_CLOUD_APIS["firestore.googleapis.com"]="Cloud Firestore API"
GOOGLE_CLOUD_APIS["cloudfunctions.googleapis.com"]="Cloud Functions API"
GOOGLE_CLOUD_APIS["pubsub.googleapis.com"]="Cloud Pub/Sub API"

# Description of external APIs.
INTEGRATION_APIS_DESCRIPTION=(
  "Google Analytics Measurement Protocol"
  "Google Analytics 4 Measurement Protocol"
  "Google Analytics Data Import"
  "Campaign Manager Conversions Upload"
  "Search Ads 360 Conversions Upload"
  "Google Ads API for Customer Match Upload"
  "Google Ads API for Call Conversions Upload"
  "Google Ads API for Click Conversions Upload"
  "Google Ads API for Enhanced Conversions Upload"
  "Google Ads API for Offline UserData Upload (OfflineUserDataService)"
  "Google Sheets API for Google Ads Conversions Upload based on Google Sheets"
  "SFTP Upload for Search Ads 360 Business Data Upload"
  "Pub/Sub Messages Send"
)

# All build-in external APIs.
INTEGRATION_APIS=(
  "N/A"
  "N/A"
  "analytics.googleapis.com"
  "dfareporting.googleapis.com doubleclicksearch.googleapis.com"
  "doubleclicksearch.googleapis.com"
  "googleads.googleapis.com"
  "googleads.googleapis.com"
  "googleads.googleapis.com"
  "googleads.googleapis.com"
  "googleads.googleapis.com"
  "sheets.googleapis.com"
  "N/A"
  "N/A"  # Pub/Sub will use ADC auth instead of OAuth or JWT.
)

# Code of external APIs. Used to create different Pub/Sub topics and
# subscriptions for different APIs.
INTEGRATION_APIS_CODE=(
  "MP"
  "MP_GA4"
  "GA"
  "CM"
  "SA"
  "ACM"
  "CALL"
  "ACLC"
  "ACA"
  "AOUD"
  "GS"
  "SFTP"
  "PB"
)

# Codes of selected APIs, for creating the topics and subscriptions.
SELECTED_APIS_CODES=()

# Common permissions to install Tentacles.
# https://cloud.google.com/service-usage/docs/access-control
# https://cloud.google.com/storage/docs/access-control/iam-roles
# https://cloud.google.com/pubsub/docs/access-control
# https://cloud.google.com/iam/docs/understanding-roles#service-accounts-roles
# https://cloud.google.com/functions/docs/reference/iam/roles
# https://cloud.google.com/firestore/docs/security/iam#roles
GOOGLE_CLOUD_PERMISSIONS=(
  ["Service Management Administrator"]="servicemanagement.services.bind"
  ["Service Usage Admin"]="serviceusage.services.enable"
  ["Storage Admin"]="storage.buckets.create storage.buckets.list"
  ["Pub/Sub Editor"]="pubsub.subscriptions.create pubsub.topics.create"
  ["Service Account User"]="iam.serviceAccounts.actAs"
  ["Cloud Functions Developer"]="cloudfunctions.functions.create"
  ["Cloud Datastore User"]="appengine.applications.get \
    datastore.databases.get \
    datastore.entities.create \
    resourcemanager.projects.get"
)

#######################################
# Save the codes of selected APIs to 'SELECTED_APIS_CODES'. This will be
# used to create Pub/Sub topics and subscriptions.
# Globals:
#   INTEGRATION_APIS_CODE
#   SELECTED_APIS_CODES
# Arguments:
#   Selected API index
#######################################
extra_operation_for_confirm_api() {
  SELECTED_APIS_CODES+=("${INTEGRATION_APIS_CODE["${1}"]}")
}

#######################################
# Confirm that APIs that Tentacles will integrate.
# Globals:
#   None
# Arguments:
#   None
#######################################
confirm_integration_api() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Confirm the integration with external APIs..."
  confirm_apis "extra_operation_for_confirm_api"
}

#######################################
# Checks if the user want to use visualization feature of Tentacles. This
# feature requires BigQuery.
# Globals:
#   ENABLE_VISUALIZATION
# Arguments:
#   none
#######################################
confirm_visualization() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Confirm enabling the visualization feature..."
  printf '%s' "    Tentacles can provide a structured overview of processed \
files and errors based on BigQuery and Data Studio. \
Do you want to enable it? [Y/n]: "
  local input
  read -r input
  if [[ ${input} == 'n' || ${input} == 'N' ]]; then
    declare -g "ENABLE_VISUALIZATION=false"
    printf '%s\n' "Skipped visualization feature."
  else
    declare -g "ENABLE_VISUALIZATION=true"
    GOOGLE_CLOUD_PERMISSIONS["BigQuery Data Editor"]="bigquery.datasets.create"
    GOOGLE_CLOUD_PERMISSIONS["Logs Writer"]="logging.logEntries.create"
    GOOGLE_CLOUD_PERMISSIONS["Logs Configuration Writer"]=\
"logging.sinks.create"
# Role 'Project IAM Admin' or 'Security Admin' has the permission
# 'bigquery.datasets.setIamPolicy'
    GOOGLE_CLOUD_PERMISSIONS["Project IAM Admin"]=\
"resourcemanager.projects.setIamPolicy"
    printf '%s\n' "OK. Visualization feature has been confirmed."
  fi
}

#######################################
# Create Pub/Sub topics and subscriptions based on the selected APIs.
# Globals:
#   PROJECT_NAMESPACE
#   SELECTED_APIS_CODES
# Arguments:
#   None
# Returns:
#   0 if all topics and subscriptions are created, non-zero on error.
#######################################
create_subscriptions() {
  (( STEP += 1 ))
  cat <<EOF
Step ${STEP}: Creating topics and subscriptions for Pub/Sub...
  Pub/Sub subscribers will not receive messages until subscriptions are created.
EOF

  node -e "require('./index.js').initPubsub(process.argv[1], \
process.argv.slice(2))" "${PROJECT_NAMESPACE}"  "${SELECTED_APIS_CODES[@]}"
  if [[ $? -gt 0 ]]; then
    echo "Failed to create Pub/Sub topics or subscriptions."
    return 1
  else
    echo "OK. Successfully created Pub/Sub topics and subscriptions."
    return 0
  fi
}

#######################################
# Deploy Cloud Functions 'Initiator'.
# Globals:
#   PROJECT_NAMESPACE
#   GCS_BUCKET
#   OUTBOUND
# Arguments:
#   None
#######################################
deploy_cloud_functions_initiator(){
  local cf_flag=()
  cf_flag+=(--entry-point=initiate)
  cf_flag+=(--trigger-bucket="${GCS_BUCKET}")
  cf_flag+=(--set-env-vars=TENTACLES_OUTBOUND="${OUTBOUND}")
  set_cloud_functions_default_settings cf_flag
  printf '%s\n' " 1. '${PROJECT_NAMESPACE}_init' is triggered by new files \
from Cloud Storage bucket [${GCS_BUCKET}]."
  gcloud functions deploy "${PROJECT_NAMESPACE}"_init "${cf_flag[@]}"
  quit_if_failed $?
}

#######################################
# Deploy Cloud Functions 'Transporter'.
# Globals:
#   PROJECT_NAMESPACE
# Arguments:
#   None
#######################################
deploy_cloud_functions_transporter(){
  local cf_flag=()
  cf_flag+=(--entry-point=transport)
  cf_flag+=(--trigger-topic="${PROJECT_NAMESPACE}-trigger")
  cf_flag+=(--retry)
  set_cloud_functions_default_settings cf_flag
  printf '%s\n' " 2. '${PROJECT_NAMESPACE}_tran' is triggered by new messages \
from Pub/Sub topic [${PROJECT_NAMESPACE}-trigger]."
  gcloud functions deploy "${PROJECT_NAMESPACE}"_tran "${cf_flag[@]}"
  quit_if_failed $?
}

#######################################
# Deploy Cloud Functions 'Api Requester'.
# Globals:
#   PROJECT_NAMESPACE
# Arguments:
#   None
#######################################
deploy_cloud_functions_api_requester(){
  local cf_flag=()
  cf_flag+=(--entry-point=requestApi)
  cf_flag+=(--trigger-topic="${PROJECT_NAMESPACE}-push")
  cf_flag+=(--retry)
  set_authentication_env_for_cloud_functions cf_flag
  set_cloud_functions_default_settings cf_flag
  printf '%s\n' " 3. '${PROJECT_NAMESPACE}_api' is triggered by new messages \
from Pub/Sub topic [${PROJECT_NAMESPACE}-push]."
  gcloud functions deploy "${PROJECT_NAMESPACE}"_api "${cf_flag[@]}"
  quit_if_failed $?
}

#######################################
# Deploy Cloud Functions 'Transporter'.
# Globals:
#   PROJECT_NAMESPACE
# Arguments:
#   None
#######################################
deploy_cloud_functions_file_job_manager(){
  local cf_flag=()
  cf_flag+=(--entry-point=manageFile)
  cf_flag+=(--trigger-http)
  set_cloud_functions_default_settings cf_flag
  printf '%s\n' " 4. '${PROJECT_NAMESPACE}_http' support HTTP request to create\
 or return status of a TentaclesFile."
  gcloud functions deploy "${PROJECT_NAMESPACE}"_http "${cf_flag[@]}"
  quit_if_failed $?
}

#######################################
# Deploy three Cloud Functions for Tentacles.
# Globals:
#   REGION
#   CF_RUNTIME
#   PROJECT_NAMESPACE
#   GCS_BUCKET
#   OUTBOUND
#   SA_KEY_FILE
# Arguments:
#   None
# Returns:
#   0 if all Cloud Functions deployed, non-zero on error.
#######################################
deploy_tentacles() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Starting to deploy Tentacles..."
  printf '%s\n' "Tentacles is composed of three Cloud Functions."
  printf '%s\n' "The Cloud Functions will be deployed to ${REGION}."

  deploy_cloud_functions_initiator
  deploy_cloud_functions_transporter
  deploy_cloud_functions_api_requester
}

#######################################
# Reselect APIs to integrate.
# Globals:
#   None
# Arguments:
#   None
#######################################
reselect_api() {
  load_config
  check_in_cloud_shell
  prepare_dependencies
  confirm_integration_api
  confirm_auth_method
  enable_apis
  create_subscriptions
  do_authentication
  deploy_cloud_functions_api_requester
  post_installation
}

#######################################
# Generate a default Log Router sink's name based on the project namespace.
# Globals:
#   PROJECT_NAMESPACE
# Arguments:
#   Any string.
# Returns:
#   The default sink name.
#######################################
get_default_sink_name() {
  printf '%s' "${PROJECT_NAMESPACE}_log"
}

#######################################
# Set up visualization features for Tentacles, including:
# 1. Create or update the sink;
# 2. Create or update the dataset;
# 3. Send out a dummy log to trigger the creation of BigQuery table.
# Globals:
#   ENABLE_VISUALIZATION
# Arguments:
#   None
#######################################
prepare_visualization() {
  while [[ -z "${ENABLE_VISUALIZATION}" ]]; do
    confirm_visualization
  done
  if [[ "${ENABLE_VISUALIZATION}" == "false" ]]; then
    return
  fi
  # Create the dataset
  confirm_located_dataset DATASET DATASET_LOCATION REGION
  printf '\n'
  # Create the sink
  create_sink_for_visualization
  # If table exists, confirm to delete it or use it.
  local table_name="${DATASET}.${BIGQUERY_LOG_TABLE}"
  if [[ $(check_existence_in_bigquery "${table_name}") -eq 0 ]]; then
    printf '%s' "  The table [${table_name}] exists. Do you want to delete \
it? [y/N]: "
    local input
    read -r input
    if [[ -z ${input} || ${input} == 'n' || ${input} == 'N' ]]; then
      # Select to use the existing table.
      printf '%s\n' "OK. Continue using the existing table.";
      return 0
    else
      printf '%s\n' "Deleting table: ${table_name}...";
      bq rm -f -t "${table_name}"
    fi
  fi
  send_dummy_log_to_create_sink_table
}

#######################################
# Send a dummy log to create target BigQuery table if it doesn't exist.
# Globals:
#   BIGQUERY_LOG_TABLE
# Arguments:
#   None
#######################################
send_dummy_log_to_create_sink_table(){
  gcloud logging write ${BIGQUERY_LOG_TABLE} '{"message": "[TentaclesFile]"}' \
--payload-type=json --severity=INFO
}

#######################################
# Complete visualization features for Tentacles, including:
# 1. Make sure the log table exists.
# 2. Create or update all views.
# Globals:
#   ENABLE_VISUALIZATION
# Arguments:
#   None
#######################################
complete_visualization() {
  while [[ -z "${ENABLE_VISUALIZATION}" ]]; do
    confirm_visualization
  done
  if [[ "${ENABLE_VISUALIZATION}" == "false" ]]; then
    return
  fi
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Creating Views for visualization..."
  # Confirm table is ready
  local table_name="${DATASET}.${BIGQUERY_LOG_TABLE}"
  while [[ $(check_existence_in_bigquery "${table_name}") -gt 0 ]]; do
    send_dummy_log_to_create_sink_table
    printf '%s\n' "The table ${table_name} is not ready."
    printf '\n%s' "Press any key to check again..."
    local any
    read -n1 -s any
  done
  create_visualization_views
  printf '%s\n' "OK. Views for visualization have been created."
}

#######################################
# Create Log Export to capture BigQuery events and granted the permission to
# publish message to Cloud Pub/Sub.
# This function requires two permissions beyond the default role 'Editor':
# logging.sinks.create - sample role: Logs Configuration Writer
# resourcemanager.projects.setIamPolicy - sample role: Project IAM Admin
# Globals:
#   GCP_PROJECT
#   PROJECT_NAMESPACE
# Arguments:
#   None
# Returns:
#   1 if failed to create log sink
#######################################
create_sink_for_visualization() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Creating Logs router sink..."
  printf '%s\n' "  Tentacles leverages Logs router to send logs to BigQuery."
  local sinkName logFilter sinkDest
  sinkName=$(get_default_sink_name)
  logFilter='jsonPayload.message=~("TentaclesFile" OR "TentaclesTask" OR
    "TentaclesFailedRecord")'
  sinkDest="bigquery.googleapis.com/projects/${GCP_PROJECT}/datasets/${DATASET}"

  create_or_update_sink ${sinkName} "${logFilter}" \
"${sinkDest} --use-partitioned-tables"
  confirm_sink_service_account_permission ${sinkName} "bigquery.dataEditor" \
"BigQuery Data Editor"
}

#######################################
# Creates all visualization views.
# Globals:
#  GCP_PROJECT
#  DATASET
#  BIGQUERY_LOG_TABLE
# Arguments:
#  none
#######################################
create_visualization_views() {
  create_or_update_view RawJson '
    SELECT
      timestamp,
      REGEXP_EXTRACT(message, r"\[([^]]+)\]")   AS entity,
      REGEXP_EXTRACT(message, r"\[[^]]+\](.*)") AS json
    FROM (
      SELECT
        REPLACE(jsonpayload.message, "\n","")   AS message,
        timestamp
      FROM
        `'${GCP_PROJECT}'.'${DATASET}'.'${BIGQUERY_LOG_TABLE}'`
    )
  '

  create_or_update_view RawTask '
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
      `'${GCP_PROJECT}'.'${DATASET}'.RawJson`
    WHERE
      entity = "TentaclesTask"
    ORDER BY
      timestamp DESC
  '

  create_or_update_view RawFile '
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
      `'${GCP_PROJECT}'.'${DATASET}'.RawJson`
    WHERE
      entity = "TentaclesFile"
    ORDER BY
      timestamp DESC
  '

  create_or_update_view RawFailedRecord '
    SELECT
      timestamp                            AS recordUpdatedTime,
      JSON_VALUE(json, "$.taskId")         AS taskId,
      JSON_VALUE(json, "$.error")          AS recordError,
      JSON_VALUE_ARRAY(json, "$.records")  AS records
    FROM
      `'${GCP_PROJECT}'.'${DATASET}'.RawJson`
    WHERE
      entity = "TentaclesFailedRecord"
    ORDER BY
      timestamp DESC
  '

  create_or_update_view TentaclesTask '
    SELECT
      "'${PROJECT_NAMESPACE}'"                           AS namespace,
      "'${GCP_PROJECT}'"                                 AS projectId,
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
        `'${GCP_PROJECT}'.'${DATASET}'.RawTask`
      GROUP BY
        id
      ) AS main
    LEFT JOIN (
      SELECT
        id, timestamp, status
      FROM
        `'${GCP_PROJECT}'.'${DATASET}'.RawTask`) AS latest
    ON
      main.taskId=latest.id AND main.lastTimestamp=latest.timestamp
    ORDER BY
      lastTimestamp DESC
  '

  create_or_update_view TentaclesReport '
    SELECT
      "'${PROJECT_NAMESPACE}'"                           AS namespace,
      "'${GCP_PROJECT}'"                                 AS projectId,
      "'${GCS_BUCKET}'"                                  AS bucket,
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
        `'${GCP_PROJECT}'.'${DATASET}'.RawFile`
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
        `'${GCP_PROJECT}'.'${DATASET}'.TentaclesTask`
      GROUP BY
       fileId
    )  AS taskSummary
    ON
      file.fileId = taskSummary.fileId
    LEFT OUTER JOIN (
      SELECT
        *
      FROM
        `'${GCP_PROJECT}'.'${DATASET}'.TentaclesTask`
    )  AS task
    ON
      file.fileId = task.fileId
    LEFT OUTER JOIN (
      SELECT
        *
      FROM
        `'${GCP_PROJECT}'.'${DATASET}'.RawFailedRecord` as record
    ) AS failedRecord
    ON
      task.taskId = failedRecord.taskId
    WHERE
      file.api IS NOT NULL
    ORDER BY
      updatedTime DESC
  '

  create_or_update_view TentaclesBlockage '
    WITH blockedApis AS (
      SELECT
        api,
        DATETIME_DIFF(CURRENT_TIMESTAMP(), MAX(lastTimestamp), MINUTE) AS minutes
      FROM
        `'${GCP_PROJECT}'.'${DATASET}'.TentaclesTask`
      WHERE
        api IN (
          SELECT
            DISTINCT api
          FROM
            `'${GCP_PROJECT}'.'${DATASET}'.TentaclesTask`
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
      `'${GCP_PROJECT}'.'${DATASET}'.TentaclesTask`
    WHERE
      taskStatus = "queuing" AND api IN (SELECT api FROM blockedApis)
  '
}

#######################################
# Check Firestore status and print next steps information after installation.
# Globals:
#   NEED_SERVICE_ACCOUNT
#   SA_KEY_FILE
# Arguments:
#   None
#######################################
post_installation() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Post-installation actions..."
  if [[ ${NEED_AUTHENTICATION} == 'true' ]]; then
    local account="YOUR_OAUTH_EMAIL"
    if [[ ${NEED_SERVICE_ACCOUNT} == 'true' ]]; then
      account=$(get_service_account)
    fi
    cat <<EOF
Some enabled APIs require authentication. Extra steps are required to grant \
access to the service account's email or your OAuth email in external systems, \
for example, Google Analytics or Campaign Manager.
You need to grant access to the email before Tentacles can send out data to \
the target APIs.

  1. Google Analytics Data Import:
   * Set up dataset for Data Import, see: \
https://support.google.com/analytics/answer/3191417?hl=en
   * Grant the 'Edit' access to [${account}]
  2. Campaign Manager:
   * DCM/DFA Reporting and Trafficking API's Conversions service, see: \
https://developers.google.com/doubleclick-advertisers/guides/conversions_overview
   * Create User Profile for [${account}] and grant the access to 'Insert \
offline conversions'
  3. Google Ads API for Google Ads Click Conversions Upload:
   * Upload click conversions, see: \
https://developers.google.com/adwords/api/docs/guides/conversion-tracking#upload_click_conversions
   * Add [${account}] as a 'Standard access' user of Google Ads.
  4. Google Sheets API for Google Ads Conversions Upload based on Google Sheets:
   * Import conversions from ad clicks into Google Ads, see: \
https://support.google.com/google-ads/answer/7014069
   * Add [${account}] as an Editor of the Google Spreadsheet that Google Ads \
will take conversions from.
EOF
  fi
  cat <<EOF

Follow the document \
https://github.com/GoogleCloudPlatform/cloud-for-marketing/blob/master/marketing-analytics/activation/gmp-googleads-connector/README.md#4-api-details\
 to create a configuration of the integration.
Save the configuration to a JSON file, for example, './config_api.json', and \
then run the following command:
  ./deploy.sh update_api_config
This command updates the configuration of Firestore/Datastore before Tentacles \
can use them.
EOF
}

#######################################
# Upload API configuration in local JSON file to Firestore or Datastore.
# The uploading process adapts to Firestore or Datastore automatically.
# Globals:
#   None
# Arguments:
#   Optional string for the configuration file path and name.
#######################################
update_api_config(){
  check_authentication
  quit_if_failed $?
  check_firestore_existence
  local configFile
  configFile="${1}"
  while [[ ! -s "${configFile}" ]]; do
    local defaultConfigFile='./config_api.json'
    printf '%s' "Enter the configuration file [${defaultConfigFile}]: "
    read -r configFile
    configFile=${configFile:-"${defaultConfigFile}"}
    printf '\n'
  done
  printf '%s\n' "Updating API integration configurations in into Firestore..."
  node -e "require('./index.js').uploadApiConfig(require(process.argv[1]), \
'${PROJECT_NAMESPACE}')" "${configFile}"
}

#######################################
# Trigger Cloud Function 'Transport'
# Globals:
#   PROJECT_NAMESPACE
# Arguments:
#   API name, e.g. MP, CM, etc.
#######################################
trigger_transport(){
  printf '%s\n' "=========================="
  printf '%s\n' "Trigger the Cloud Function 'Transport'."
  check_authentication
  quit_if_failed $?

  local api
  if [[ -n "${1}" ]]; then
    api="${1}"
  else
    printf '%s' "Enter the API to trigger: "
    read -r api
    api="${api}"
    printf '\n'
  fi
  node -e "require('./index.js').triggerTransport('${api}', \
'${PROJECT_NAMESPACE}')"
}

#######################################
# Invoke the Cloud Function 'API requester' directly based on a local file.
# Note: The API configuration is still expected to be on Google Cloud. The API
# configuration needs to be updated to Firestore for any modification.
# Exampales:
# 1. Test based on a GCS file:
#    ./deploy.sh run_test_locally 'file_path_in_gcs' 'bucket_name'
# 2. Test based on a local file (only works for Pub/sub based APIs):
#    ./deploy.sh run_test_locally './loacl_file_path'
# Globals:
#   SA_KEY_FILE
# Arguments:
#   File to be sent out, a path.
#######################################
run_test_locally(){
  printf '%s\n' "=========================="
  cat <<EOF
Invoke the Cloud Function 'API requester' directly based on a local file.
Note: The API configuration is still expected to be on Google Cloud. The API
configuration needs to be updated to Firestore for any modification.
EOF
  check_authentication
  quit_if_failed $?
  check_firestore_existence
  local auth
  if [[ -n "${SECRET_NAME}" ]]; then
    auth="SECRET_NAME=${SECRET_NAME}"
  elif [[ -f "$(pwd)/${OAUTH2_TOKEN_JSON}" ]]; then
    auth="OAUTH2_TOKEN_JSON=$(pwd)/${OAUTH2_TOKEN_JSON}"
  elif [[ -f "$(pwd)/${SA_KEY_FILE}" ]]; then
    auth="API_SERVICE_ACCOUNT=$(pwd)/${SA_KEY_FILE}"
  fi
  printf '%s\n' "  Setting environment variable of auth: ${auth}"
  env "${auth}" "DEBUG=true" node -e "require('./index.js').localApiRequester(\
    '${PROJECT_NAMESPACE}', process.argv[1], process.argv[2])" "$@"
}

#######################################
# Start process by copying a file to target folder.
# Note: The API configuration is still expected to be on Google Cloud. The API
# configuration needs to be updated to Firestore for any modification.
# Globals:
#   CONFIG_FILE
#   OUTBOUND
# Arguments:
#   File to be sent out, a path.
#######################################
copy_file_to_start(){
  printf '%s\n' "=========================="
  cat <<EOF
Copy local file(s) to Cloud Storage to start process.
Note: The API configuration is still expected to be on Google Cloud. The API
configuration needs to be updated to Firestore for any modification.
EOF
  local source bucket folder target
  # gsutil support wildcard name. Use '*' to replace '[' here.
  source="$(printf '%s' "${1}" | sed -r 's/\[/\*/g' )"
  bucket=$(get_value_from_json_file "${CONFIG_FILE}" "GCS_BUCKET")
  folder=$(get_value_from_json_file "${CONFIG_FILE}" "OUTBOUND" | sed -r 's/\/$//')
  target="gs://${bucket}/${folder}"
  printf '%s\n' "  Source: ${1}"
  printf '%s\n' "  Target: ${target}"
  printf '%s' "Confirm? [Y/n]: "
  local input
  read -n1 -s input
  printf '%s\n' "${input}"
  if [[ -z ${input} || ${input} == 'y' || ${input} == 'Y' ]]; then
    copy_to_gcs "${source}" "${target}"
  else
    printf '%s\n' "User cancelled."
  fi
}

# For compatibility to the published doc.
copy_file_to_gcs(){
  copy_file_to_start "$@"
}

DEFAULT_INSTALL_TASKS=(
  "print_welcome Tentacles"
  load_config
  check_in_cloud_shell
  prepare_dependencies
  confirm_project
  confirm_namespace
  confirm_region
  confirm_integration_api
  confirm_auth_method
  confirm_visualization
  check_permissions
  enable_apis
  "confirm_located_bucket GCS_BUCKET BUCKET_LOC REGION"
  "confirm_folder OUTBOUND"
  confirm_firestore
  prepare_visualization
  save_config
  create_subscriptions
  do_authentication
  deploy_tentacles
  complete_visualization
  post_installation
  "print_finished Tentacles"
)

if [[ "${BASH_SOURCE[0]}" -ef "$0" ]]; then
  run_default_function "$@"
else
  printf '%s\n' "Tentacles Bash Library is loaded."
fi
