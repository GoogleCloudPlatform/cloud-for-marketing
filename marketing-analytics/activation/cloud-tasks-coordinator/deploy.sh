#!/bin/bash
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
source "${BASE_DIR}/../common-libs/install-script-library/install_functions.sh"

# Default project name is 'sentinel'. It will be used as prefix of Cloud
# Functions, PubSub, etc. You can change it here (only lowercase letters,
# numbers and dashes(-) are suggested).
PROJECT_NAME="${PROJECT_NAME:=sentinel}"

# Project configuration file.
CONFIG_FILE="./config.json"

# This parameter name is used by functions to load/save config.
CONFIG_FOLDER_NAME="INBOUND"
CONFIG_ITEMS=("GCS_BUCKET" "${CONFIG_FOLDER_NAME}" "PS_TOPIC")

# Whether or not this installation will use storage monitor
NEED_STORAGE_MONITOR="false"

#TODO change this
# Common permissions to install Sentinel
# https://cloud.google.com/service-usage/docs/access-control
# https://cloud.google.com/storage/docs/access-control/iam-roles
# https://cloud.google.com/pubsub/docs/access-control
# https://cloud.google.com/iam/docs/understanding-roles#service-accounts-roles
# https://cloud.google.com/functions/docs/reference/iam/roles
# https://cloud.google.com/firestore/docs/security/iam#roles
declare -A GOOGLE_CLOUD_PERMISSIONS
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
)

# The APIs that will be used in Sentinel.
declare -A GOOGLE_CLOUD_APIS
GOOGLE_CLOUD_APIS=(
  ["iam.googleapis.com"]="Identity and Access Management (IAM) API"
  ["cloudresourcemanager.googleapis.com"]="Cloud Refolder Manager API"
  ["firestore.googleapis.com"]="Google Cloud Firestore API"
  ["cloudfunctions"]="Cloud Functions API"
  ["pubsub"]="Cloud Pub/Sub API"
)

#######################################
# Create Log Export to capture BigQuery events and granted the permission to
# publish message to Cloud Pub/Sub.
# Globals:
#   PS_TOPIC
#   GCP_PROJECT
# Arguments:
#   None
# Returns:
#   0 if created, non-zero on error.
#######################################
create_sink() {
  (( STEP += 1 ))
  printf '%s\n' "STEP[${STEP}] Create Logging Export (Sink)..."
  printf '%s\n' "  Rationale: Sentinel leverages Logging Export to monitor \
when tasks are finished and trigger next ones."

  node -e "require('./index.js').installSink(process.argv[1])" "${PS_TOPIC}"
  local service_account
  service_account=$(gcloud logging sinks describe "${PS_TOPIC}"-monitor \
    | grep writerIdentity \
    | cut -d\  -f2)
  printf '%s\n'  "Grant pubsub.publisher access to this Sink's service \
account: ${service_account}"
  gcloud -q projects add-iam-policy-binding ${GCP_PROJECT} --member \
"${service_account}" --role roles/pubsub.publisher
  if [[ $? -gt 0 ]]; then
    printf '%s\n' "[Failed] Failed to create Logging Export."
    return 1
  else
    printf '%s\n' "[OK] Successfully create Logging Export."
    return 0
  fi
}

#######################################
# Confirm that whether enable Sentinel to support Cloud Storage Bucket monitor.
# Globals:
#   NEED_STORAGE_MONITOR
# Arguments:
#   None
#######################################
confirm_monitor_bucket() {
  (( STEP += 1 ))
  printf '%s\n' "STEP[${STEP}] Confirm the usage of Cloud Storage monitor..."
  cat <<EOF
  Rationale: Sentinel can also monitor a Cloud Storage Bucket to fulfil a \
'Load' task which will automatically load incoming files to BigQuery. To \
enable that, you need to create/select a Cloud Storage Bucket and confirm a \
monitor folder.
EOF
  printf '\n%s' "Are you going to enable 'Load' task? [Y/n]:"
  local continue
  read -r continue
  continue=${continue:-"Y"}
  if [[ ${continue} = "Y" || ${continue} = "y" ]]; then
    NEED_STORAGE_MONITOR="true"
    printf '%s\n\n' "[OK] Will install Storage monitor."
  else
    NEED_STORAGE_MONITOR="false"
    printf '%s\n\n' "[Skipped] to use Storage monitor."
  fi
}

#######################################
# Deploy Cloud Functions of Sentinel.
# Globals:
#   REGION
#   CF_RUNTIME
#   PROJECT_NAME
#   PS_TOPIC
#   GCS_BUCKET
#   CONFIG_FOLDER_NAME
# Arguments:
#   None
# Returns:
#   0 if all cloud functions deployed, non-zero on error.
#######################################
deploy_sentinel() {
  (( STEP += 1 ))
  printf '%s\n' "STEP[${STEP}] Start to deploy Sentinel..."
  printf '%s\n' "Sentinel is combined of three Cloud Functions."
  while [[ -z ${REGION} ]]; do
    set_region
  done
  printf '%s\n' "[ok] Will deploy Cloud Functions to ${REGION}."

  local cf_flag=()
  cf_flag+=(--region="${REGION}")
  cf_flag+=(--timeout=540 --memory=2048MB --runtime="${CF_RUNTIME}")
  cf_flag+=(--set-env-vars=SENTINEL_TOPIC_PREFIX="${PS_TOPIC}")

  printf '%s\n' " 1. '${PROJECT_NAME}_bq' based on Cloud Pub/Sub \
topic[${PS_TOPIC}-monitor]."
  gcloud functions deploy "${PROJECT_NAME}_bq" --entry-point monitorBigQuery \
--trigger-topic "${PS_TOPIC}"-monitor "${cf_flag[@]}"
  quit_if_failed $?

  printf '%s\n' " 2. '${PROJECT_NAME}_start' based on Cloud Pub/Sub \
topic[${PS_TOPIC}-start]."
  gcloud functions deploy "${PROJECT_NAME}"_start --entry-point startTask \
--trigger-topic "${PS_TOPIC}"-start "${cf_flag[@]}"
  quit_if_failed $?

  if [[ ${NEED_STORAGE_MONITOR} = 'true' ]]; then
    printf '%s\n' " 3. '${PROJECT_NAME}_gcs' based on Cloud Storage \
  bucket[${GCS_BUCKET}]."
    cf_flag+=(--set-env-vars=SENTINEL_INBOUND="${!CONFIG_FOLDER_NAME}")
    gcloud functions deploy "${PROJECT_NAME}_gcs" --entry-point monitorStorage \
  --trigger-bucket "${GCS_BUCKET}" "${cf_flag[@]}"
    quit_if_failed $?
  fi
}

print_welcome() {
  cat <<EOF
###########################################################
##                                                       ##
##            Start installation of Sentinel             ##
##                                                       ##
###########################################################

EOF
}

post_installation() {
  (( STEP += 1 ))
  printf '%s\n' "STEP[${STEP}] Post installation."
  check_firestore_existence
  printf '%s\n' "[ok] Firestore/Datastore is ready."
  #TODO update the link here
  cat <<EOF
Finally, follow the document (link) to create configuration of the tasks.
Save the configuration to a JSON file, e.g. './config_task.json' and run:
  ./deploy.sh update_task_config
to update the configuration to Firestore/Datastore before Sentinel can use them.

EOF
}

print_finished(){
  cat <<EOF
###########################################################
##           Sentinel has been installed.                ##
###########################################################
EOF
}

#######################################
# Start the automatic process to install Sentinel.
# Globals:
#   None
# Arguments:
#   None
#######################################
install_sentinel() {

  print_welcome
  load_config

  local tasks=(
    check_in_cloud_shell prepare_dependencies
    confirm_project confirm_region
    check_permissions enable_apis
    confirm_topic create_sink
  )
  local task
  for task in "${tasks[@]}"; do
    "${task}"
    quit_if_failed $?
  done

  confirm_monitor_bucket
  if [[ ${NEED_STORAGE_MONITOR} = 'true' ]]; then
    create_bucket
    confirm_folder
  fi
  save_config
  deploy_sentinel
  post_installation
  print_finished
}

#######################################
# Upload tasks configuration in local JSON file to Cloud Firestore or Datastore.
# It will adapt to Firestore or Datastore automatically.
# Globals:
#   None
# Arguments:
#   None
#######################################
update_task_config() {
  printf '%s\n' "=========================="
  printf '%s\n' "Update Task configurations in into Firestore."
  check_authentication
  quit_if_failed $?
  check_firestore_existence

  local default_config_file='./config_task.json'
  printf '%s' "Please input the configuration file [${default_config_file}]:"
  local task_config
  read -r task_config
  task_config=${task_config:-"${default_config_file}"}
  printf '\n'
  node -e "require('./index.js').uploadTaskConfig(require(process.argv[1]))" \
"${task_config}"
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
start_task_directly() {
  cat <<EOF
==========================
Invoke task based locally. However the task configuration is still expected to \
be on Cloud. You need to update task config first if you modified any.
EOF
  check_authentication
  quit_if_failed $?
  check_firestore_existence
  node -e "require('./index.js').testTaskDirectly(process.argv[1],\
process.argv[2])" "$@"
}

#######################################
# Start a task by sending out a message to the target Pub/Sub topic.
# Please note: the task configuration is still expected to be on Cloud. You need
# to update task config first if you modified any.
# Globals:
#   PS_TOPIC
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
  node -e "require('./index.js').testTaskThroughPubSub(process.argv[1], \
process.argv[2],'${PS_TOPIC}')" "$@"
}


#######################################
# Create a Cloud Schedular Job which target Pub/Sub. Current Cloud Console does
# not support attributes. For example:
#   ./deploy.sh create_cron_task test-export
# Globals:
#   PROJECT_NAME
# Arguments:
#   Task name, a string.
#######################################
create_cron_task() {
  check_authentication
  quit_if_failed $?
  check_firestore_existence
  gcloud beta scheduler jobs create pubsub ${PROJECT_NAME}-cronjob \
  --schedule="0 6 * * *" \
  --time-zone=Australia/Sydney \
  --topic=${PROJECT_NAME}-start \
  --message-body='{"partitionDay":"${today}"}' \
  --attributes=taskId=$1
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

if [[ "${BASH_SOURCE[0]}" -ef "$0" ]]; then
  MAIN_FUNCTION="install_sentinel"
  run_default_function "$@"
else
  printf '%s\n' "Sentinel Bash Library is loaded."
fi
