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
SOLUTION_NAME="sentinel"

# Project namespace will be used as prefix of the name of Cloud Functions,
# Pub/Sub topics, etc.
# Default project namespace is SOLUTION_NAME.
# Note: only lowercase letters, numbers and dashes(-) are allowed.
PROJECT_NAMESPACE="${SOLUTION_NAME}"

# Project configuration file.
CONFIG_FILE="./config.json"

# Parameter name used by functions to load and save config.
CONFIG_FOLDER_NAME="INBOUND"
CONFIG_ITEMS=("PROJECT_NAMESPACE" "GCS_BUCKET" "${CONFIG_FOLDER_NAME}")

# The Google Cloud APIs that will be used in Sentinel.
GOOGLE_CLOUD_APIS["firestore.googleapis.com"]="Cloud Firestore API"
GOOGLE_CLOUD_APIS["cloudfunctions.googleapis.com"]="Cloud Functions API"
GOOGLE_CLOUD_APIS["pubsub.googleapis.com"]="Cloud Pub/Sub API"
GOOGLE_CLOUD_APIS["cloudscheduler.googleapis.com"]="Cloud Scheduler API"

# Description of external APIs.
INTEGRATION_APIS_DESCRIPTION=(
  "Batch Prediction on Cloud AutoML API" # GCP API won't use OAuth.
  "Download Google Ads Reports"
  "Download Campaign Manager Reports"
  "Download Display & Video 360 Reports"
  "Download Search Ads 360 Reports"
  "BigQuery query external tables based on Google Sheet"
  "Run Ads Data Hub Queries"
)

# All build-in external APIs.
INTEGRATION_APIS=(
  "automl.googleapis.com"
  "googleads.googleapis.com"
  "dfareporting.googleapis.com"
  "doubleclickbidmanager.googleapis.com"
  "doubleclicksearch.googleapis.com"
  "drive.googleapis.com"
  "adsdatahub.googleapis.com"
)

# Common permissions to install Sentinel.
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
  ["Logs Configuration Writer"]="logging.sinks.create"
  ["Project IAM Admin"]="resourcemanager.projects.setIamPolicy"
  ["Cloud Scheduler Admin"]="cloudscheduler.jobs.create \
    cloudscheduler.jobs.update"
)

#######################################
# Generate a default Log Router name based on the project namespace.
# Globals:
#   PROJECT_NAMESPACE
# Arguments:
#   Any string.
# Returns:
#   The default sink name.
#######################################
get_default_sink_name() {
  printf '%s' "${PROJECT_NAMESPACE}-monitor"
}

#######################################
# Create Log Export to capture BigQuery events and granted the permission to
# publish message to Cloud Pub/Sub.
# This function requires two permissions beyond the default role 'Editor':
# logging.sinks.create - sample role: Logs Configuration Writer
# resourcemanager.projects.setIamPolicy - sample role: Project IAM Admin
# Globals:
#   PROJECT_NAMESPACE
#   GCP_PROJECT
# Arguments:
#   None
# Returns:
#   0 if created, non-zero on error.
#######################################
create_sink() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Creating Logs router sink..."
  printf '%s\n' "  Sentinel leverages Logs router to monitor the finish events \
of tasks and then trigger next ones."
  # Check, create or update sink
  local sinkName logFilter existingFilter
  sinkName=$(get_default_sink_name)
  logFilter="resource.type=\"bigquery_resource\" AND protoPayload.methodName=\"\
jobservice.jobcompleted\""
  existingFilter=$(gcloud logging sinks list --filter="name:${sinkName}" \
--format="value(filter)")
  if [[ "${existingFilter}" != "${logFilter}" ]];then
    local sinkDest
    sinkDest="pubsub.googleapis.com/projects/${GCP_PROJECT}/topics/${sinkName}"
    if [[ -z "${existingFilter}" ]];then
      printf '%s\n' "  Logging Export [${sinkName}] doesn't exist. Creating..."
      gcloud -q logging sinks create "${sinkName}" "${sinkDest}" \
--description="${PROJECT_NAMESPACE} Monitor"  --log-filter="${logFilter}"
    else
      printf '%s\n' "  Logging Export [${sinkName}] exists with a different \
filter. Updating..."
      gcloud -q logging sinks update "${sinkName}" "${sinkDest}" \
--description="${PROJECT_NAMESPACE} Monitor"  --log-filter="${logFilter}"
    fi
  else
    printf '%s\n' "  Logging Export [${sinkName}] exists. Continue..."
  fi
  if [[ $? -gt 0 ]];then
    printf '%s\n' "Failed to create or update Logs router sink."
    return 1
  fi
  # Grant service account of sink the role of Pub/Sub Publisher
  local serviceAccount existingRole
  serviceAccount=$(gcloud logging sinks describe "${sinkName}" \
--format="get(writerIdentity)")
  while :;do
    printf '\n%s\n' "  Checking the role of the sink's service account \
[${serviceAccount}]..."
    existingRole=$(gcloud projects get-iam-policy "${GCP_PROJECT}" \
--flatten=bindings --filter="bindings.members:${serviceAccount} AND \
bindings.role:roles/pubsub.publisher" --format="get(bindings.members)")
    if [[ -z "${existingRole}" ]];then
      printf '%s\n'  "  Granting Role 'Pub/Sub Publisher' to the service \
account..."
      gcloud -q projects add-iam-policy-binding "${GCP_PROJECT}" --member \
"${serviceAccount}" --role roles/pubsub.publisher
      if [[ $? -gt 0 ]];then
        printf '%s\n' "Failed to grant the role. Use this link \
https://console.cloud.google.com/iam-admin/iam?project=${GCP_PROJECT} to \
manually grant Role 'Pub/Sub Publisher' to ${serviceAccount}."
        printf '%s' "Press any key to continue after you grant the access..."
        local any
        read -n1 -s any
        continue
      fi
    else
      printf '%s\n'  "OK. The role has already been granted."
      return 0
    fi
  done
}

#######################################
# Confirm that whether enable Sentinel to support Cloud Storage Bucket monitor.
# If confirmed, continue to create bucket and select the monitor folder.
# Globals:
#   None
# Arguments:
#   None
#######################################
confirm_monitor_bucket() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Confirm the usage of Cloud Storage monitor..."
  cat <<EOF
  This solution can monitor a Cloud Storage Bucket to fulfil a 'Load' \
task which will automatically load incoming files to BigQuery. To enable that, \
a Cloud Storage Bucket and a folder in it are required.
EOF
  printf '\n%s' "Are you going to enable 'Load' task? [Y/n]: "
  local continue
  read -r continue
  continue=${continue:-"Y"}
  if [[ ${continue} = "Y" || ${continue} = "y" ]]; then
    printf '%s\n\n' "OK. Cloud Storage monitor selected."
    create_bucket
    confirm_folder
  else
    printf '%s\n\n' "Skipped to create Cloud Storage monitor."
  fi
}

#######################################
# Confirm external tasks.
# Globals:
#   None
# Arguments:
#   None
#######################################
confirm_external_tasks() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Confirm the integration with external tasks..."
  cat <<EOF
  This solution can do different external tasks, including batch prediction of \
AutoML Tables API, or downloading reports from Google Ads, Campaign Manager or \
Display & Video 360, etc. To enable them, corresponding APIs need to be \
enabled and an authentication is required.
EOF
  confirm_apis
}

#######################################
# Deploy Cloud Functions 'Task coordinator'.
# Globals:
#   PROJECT_NAMESPACE
#   SA_KEY_FILE
# Arguments:
#   None
#######################################
deploy_cloud_functions_task_coordinator(){
  local cf_flag=()
  cf_flag+=(--entry-point=coordinateTask)
  cf_flag+=(--trigger-topic="${PROJECT_NAMESPACE}"-monitor)
  set_authentication_env_for_cloud_functions cf_flag
  set_cloud_functions_default_settings cf_flag
  printf '%s\n' " 1. '${PROJECT_NAMESPACE}_main' is triggered by messages from \
Pub/Sub topic [${PROJECT_NAMESPACE}-monitor]."
  gcloud functions deploy "${PROJECT_NAMESPACE}"_main "${cf_flag[@]}"
  quit_if_failed $?
}

#######################################
# Deploy Cloud Functions 'Cloud Storage Monitor'.
# Globals:
#   PROJECT_NAMESPACE
#   GCS_BUCKET
#   CONFIG_FOLDER_NAME
# Arguments:
#   None
#######################################
deploy_cloud_functions_storage_monitor(){
  local cf_flag=()
  cf_flag+=(--entry-point=monitorStorage)
  cf_flag+=(--trigger-bucket="${GCS_BUCKET}")
  cf_flag+=(--set-env-vars=SENTINEL_INBOUND="${!CONFIG_FOLDER_NAME}")
  set_cloud_functions_default_settings cf_flag
  printf '%s\n' " 2. '${PROJECT_NAMESPACE}_gcs' is triggered by new files from \
Cloud Storage bucket [${GCS_BUCKET}]."
  gcloud functions deploy "${PROJECT_NAMESPACE}"_gcs "${cf_flag[@]}"
  quit_if_failed $?
}

#######################################
# Deploy Cloud Functions of Sentinel.
# Globals:
#   REGION
#   GCS_BUCKET
#   CONFIG_FOLDER_NAME
# Arguments:
#   None
# Returns:
#   0 if all cloud functions deployed, non-zero on error.
#######################################
deploy_sentinel() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Starting to deploy Sentinel..."
  printf '%s\n' "Sentinel is composed of Cloud Functions."
  ensure_region
  printf '%s\n' "OK. Cloud Functions will be deployed to ${REGION}."

  deploy_cloud_functions_task_coordinator
  if [[ -n ${GCS_BUCKET} && -n ${!CONFIG_FOLDER_NAME} ]]; then
    deploy_cloud_functions_storage_monitor
  fi
}

#######################################
# Create or update a Cloud Schedular to trigger the default check task of
# Sentinel.
# Globals:
#   PROJECT_NAMESPACE
# Arguments:
#   None
#######################################
set_internal_task() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Starting to create or update Cloud Scheduler \
for Sentinel status check task..."
  local job_name=${PROJECT_NAMESPACE}-intrinsic-cronjob
  create_or_update_cloud_scheduler_for_pubsub \
    $job_name \
    "*/5 * * * *" \
    "Australia/Sydney" \
    ${PROJECT_NAMESPACE}-monitor \
    '{"intrinsic": "status_check"}' \
    taskId=system
  printf '\n'
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
  printf '%s\n' "Step ${STEP}: Post-installation checks..."
  check_firestore_existence
  printf '%s\n' "[ok] Firestore/Datastore is ready."
  if [[ ${NEED_AUTHENTICATION} = 'true' ]]; then
    local account="YOUR_OAUTH_EMAIL"
    if [[ ${NEED_SERVICE_ACCOUNT} = 'true' ]]; then
      account=$(get_value_from_json_file "${SA_KEY_FILE}" 'client_email')
    fi
#TODO add details of different APIs.
    cat <<EOF
External tasks require authentication. Extra steps are required to grant \
access to the service account's email or your OAuth email in external systems, \
for example, Google Ads or Campaign Manager.
You need to grant access to the email before Sentinel can execute external \
tasks.
EOF
  fi
#TODO update the link here.
  cat <<EOF

Follow the document \
(TBD) to create a configuration of the integration.
Save the configuration to a JSON file, for example, './config_task.json', and \
then run the following command:
  ./deploy.sh update_task_config
This command updates the configuration of Firestore/Datastore before Sentinel \
can use them.
EOF
}

#######################################
# Upload tasks configuration in local JSON file to Cloud Firestore or Datastore.
# It will adapt to Firestore or Datastore automatically.
# The task configuration file supports using the properties that have been saved
# in the configuration 'config.json'. For example, to use the PROJECT_ID, just
# put the string #PROJECT_ID# in the tasks configuration json file.
# Globals:
#   CONFIG_FILE
# Arguments:
#   Optional string for the configuration file path and name.
#######################################
update_task_config() {
  printf '%s\n' "=========================="
  printf '%s\n' "Update Task configurations in into Firestore/Datastore."
  check_authentication
  quit_if_failed $?
  check_firestore_existence

  local task_config
  if [[ -n $1 ]]; then
    task_config=$1
  else
    local default_config_file='./config_task.json'
    printf '%s' "Please input the configuration file [${default_config_file}]:"
    read -r task_config
    task_config=${task_config:-"${default_config_file}"}
    printf '\n'
  fi
  node -e "require('./index.js').uploadTaskConfig(require(process.argv[1]), \
require(process.argv[2]), '${PROJECT_NAMESPACE}')" "${task_config}" \
  "${CONFIG_FILE}"
}

#######################################
# Start a task directly, not through Cloud Pub/Sub.
# Please note: the task configuration is still expected to be on Cloud. You need
# to update task config first if you modified any.
# Globals:
#   None
# Arguments:
#   task Id, a string
#   a stringified JSON object of parameters, e.g. '{"partitionDay":"20191001"}'
#######################################
start_task_locally() {
  cat <<EOF
==========================
Invoke task based locally. However the task configuration is still expected to \
be on Cloud. You need to update task config first if you modified any.
EOF
  check_authentication
  quit_if_failed $?
  check_firestore_existence
  OAUTH2_TOKEN_JSON="${BASE_DIR}/${OAUTH2_TOKEN_JSON}" \
  API_SERVICE_ACCOUNT="${BASE_DIR}/${SA_KEY_FILE}" \
  node -e "require('./index.js').startTaskFromLocal(process.argv[1], \
  process.argv[2], '${PROJECT_NAMESPACE}')" "$@"
}

#######################################
# Start a task by sending out a message to the target Pub/Sub topic.
# Please note: the task configuration is still expected to be on Cloud. You need
# to update task config first if you modified any.
# Globals:
#   PROJECT_NAMESPACE
# Arguments:
#   Task Id, a string
#   A stringified JSON object of parameters, e.g. '{"partitionDay":"20191001"}'
#   Prefix of the topic name
#######################################
start_task_remotely() {
  cat <<EOF
==========================
Invoke task based remotely by sending out a message to the target Pub/Sub topic.
The task configuration is still expected to be on Cloud. You need to update \
task config first if you modified any.
EOF
  check_authentication
  quit_if_failed $?
  check_firestore_existence
  node -e "require('./index.js').startTaskThroughPubSub(process.argv[1], \
process.argv[2],'${PROJECT_NAMESPACE}')" "$@"
}

#######################################
# Create a Cloud Schedular Job which target Pub/Sub. Current Cloud Console does
# not support attributes. For example:
#   ./deploy.sh create_cron_task test-export
# Globals:
#   PROJECT_NAMESPACE
# Arguments:
#   Task name, a string.
#######################################
create_cron_task() {
  check_authentication
  quit_if_failed $?
  check_firestore_existence
  local job_name=${PROJECT_NAMESPACE}-$1
  create_or_update_cloud_scheduler_for_pubsub \
    $job_name \
    "0 6 * * *" \
    "Australia/Sydney" \
    ${PROJECT_NAMESPACE}-monitor \
    '{"today": "${today}","yesterday":"${yesterday}"}' \
    taskId=$1
}

#######################################
# Copy a local folder (default: 'sql/') to the target Storage bucket.
# Globals:
#   CONFIG_FILE
# Arguments:
#   Folder name, a string.
#######################################
copy_sql_to_gcs() {
  cat <<EOF
==========================
Copy a local folder (default: 'sql/') to the target Storage bucket. Can be used
to copy sql files to Cloud Storage.
EOF
  local folder=$1
  folder="${folder:=sql}"
  local target
  target="gs://$(get_value_from_json_file "${CONFIG_FILE}" "GCS_BUCKET")"
  printf '%s\n' "Copy integration data files to target folder in Cloud \
Storage: ${target}"
  gsutil -m rsync "${folder}" "${target}/${folder}"
}

DEFAULT_INSTALL_TASKS=(
  "print_welcome Sentinel"
  load_config
  check_in_cloud_shell
  prepare_dependencies
  confirm_namespace confirm_project confirm_region
  confirm_external_tasks confirm_auth_method
  check_permissions enable_apis
  create_sink
  confirm_monitor_bucket
  save_config
  do_authentication
  deploy_sentinel
  set_internal_task
  post_installation
  "print_finished Sentinel"
)

if [[ "${BASH_SOURCE[0]}" -ef "$0" ]]; then
  run_default_function "$@"
else
  printf '%s\n' "Sentinel Bash Library is loaded."
fi
