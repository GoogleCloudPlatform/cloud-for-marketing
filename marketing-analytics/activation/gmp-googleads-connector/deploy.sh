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
source "${BASE_DIR}/../common-libs/install-script-library/install_functions.sh"

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
CONFIG_FOLDER_NAME="OUTBOUND"
CONFIG_ITEMS=("PROJECT_NAMESPACE" "GCS_BUCKET" "${CONFIG_FOLDER_NAME}")

# The Google Cloud APIs that will be used in Tentacles.
GOOGLE_CLOUD_APIS["firestore.googleapis.com"]="Cloud Firestore API"
GOOGLE_CLOUD_APIS["cloudfunctions.googleapis.com"]="Cloud Functions API"
GOOGLE_CLOUD_APIS["pubsub.googleapis.com"]="Cloud Pub/Sub API"

# Description of external APIs.
INTEGRATION_APIS_DESCRIPTION=(
  "Google Analytics Measurement Protocol"
  "Google Analytics Data Import"
  "Campaign Manager Conversions Upload"
  "Search Ads 360 Conversions Upload"
  "Google Ads API for Customer Match Upload"
  "Google Ads API for Click Conversions Upload"
  "Google Sheets API for Google Ads Conversions Upload based on Google Sheets"
  "SFTP Upload for Search Ads 360 Business Data Upload"
  "Pub/Sub Messages Send"
)

# All build-in external APIs.
INTEGRATION_APIS=(
  "N/A"
  "analytics.googleapis.com"
  "dfareporting.googleapis.com doubleclicksearch.googleapis.com"
  "doubleclicksearch.googleapis.com"
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
  "GA"
  "CM"
  "SA"
  "ACM"
  "ACLC"
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
#   CONFIG_FOLDER_NAME
# Arguments:
#   None
#######################################
deploy_cloud_functions_initiator(){
  local cf_flag=()
  cf_flag+=(--entry-point=initiate)
  cf_flag+=(--trigger-bucket="${GCS_BUCKET}")
  cf_flag+=(--set-env-vars=TENTACLES_OUTBOUND="${!CONFIG_FOLDER_NAME}")
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
#   GCS_BUCKET
#   CONFIG_FOLDER_NAME
# Arguments:
#   None
#######################################
deploy_cloud_functions_transporter(){
  local cf_flag=()
  cf_flag+=(--entry-point=transport)
  cf_flag+=(--trigger-topic="${PROJECT_NAMESPACE}-trigger")
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
#   GCS_BUCKET
#   CONFIG_FOLDER_NAME
# Arguments:
#   None
#######################################
deploy_cloud_functions_api_requester(){
  local cf_flag=()
  cf_flag+=(--entry-point=requestApi)
  cf_flag+=(--trigger-topic="${PROJECT_NAMESPACE}-push")
  set_authentication_env_for_cloud_functions cf_flag
  set_cloud_functions_default_settings cf_flag
  printf '%s\n' " 3. '${PROJECT_NAMESPACE}_api' is triggered by new messages \
from Pub/Sub topic [${PROJECT_NAMESPACE}-push]."
  gcloud functions deploy "${PROJECT_NAMESPACE}"_api "${cf_flag[@]}"
  quit_if_failed $?
}

#######################################
# Deploy three Cloud Functions for Tentacles.
# Globals:
#   REGION
#   CF_RUNTIME
#   PROJECT_NAMESPACE
#   GCS_BUCKET
#   CONFIG_FOLDER_NAME
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
  ensure_region
  printf '%s\n' "OK. Cloud Functions will be deployed to ${REGION}."

  deploy_cloud_functions_initiator
  deploy_cloud_functions_transporter
  deploy_cloud_functions_api_requester
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
  printf '%s\n' "OK. Firestore/Datastore is ready."
  if [[ ${NEED_AUTHENTICATION} = 'true' ]]; then
    local account="YOUR_OAUTH_EMAIL"
    if [[ ${NEED_SERVICE_ACCOUNT} = 'true' ]]; then
      account=$(get_value_from_json_file "${SA_KEY_FILE}" 'client_email')
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
  printf '%s\n' "=========================="
  printf '%s\n' "Update API configurations in Firestore/Datastore."
  check_authentication
  quit_if_failed $?
  check_firestore_existence

  local api_config
  if [[ -n $1 ]]; then
    api_config=$1
  else
    local default_config_file='./config_api.json'
    printf '%s' "Enter the configuration file [${default_config_file}]: "
    read -r api_config
    api_config=${api_config:-"${default_config_file}"}
    printf '\n'
  fi
  node -e "require('./index.js').uploadApiConfig(require(process.argv[1]), \
'${PROJECT_NAMESPACE}')" "${api_config}"
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
  if [[ -n $1 ]]; then
    api=$1
  else
    printf '%s' "Enter the API to trigger: "
    read -r api
    api=${api}
    printf '\n'
  fi
  node -e "require('./index.js').triggerTransport('${api}', \
'${PROJECT_NAMESPACE}')"
}

#######################################
# Invoke the Cloud Function 'API requester' directly based on a local file.
# Note: The API configuration is still expected to be on Google Cloud. You need
# to update the API configuration first if you made any modifications.
# Globals:
#   SA_KEY_FILE
# Arguments:
#   File to be sent out, a path.
#######################################
run_test_locally(){
  printf '%s\n' "=========================="
  cat <<EOF
Invoke the Cloud Function 'API requester' directly based on a local file.
Note: The API configuration is still expected to be on Google Cloud. You need \
to update the API configuration first if you made any modifications.
EOF
  if [[ -f "${SA_KEY_FILE}" ]]; then
    API_SERVICE_ACCOUNT="$(pwd)/${SA_KEY_FILE}"
    printf '%s\n' "Use environment variable \
API_SERVICE_ACCOUNT=${API_SERVICE_ACCOUNT}"
  fi
  DEBUG=true CODE_LOCATION='' API_SERVICE_ACCOUNT="${API_SERVICE_ACCOUNT}" \
node -e "require('./index.js').localApiRequester(process.argv[1])" "$@"
}

#######################################
# Start process by copying a file to target folder.
# Note: The API configuration is still expected to be on Google Cloud. You need
# to update the API configuration first if you made any modifications.
# Globals:
#   CONFIG_FILE
#   CONFIG_FOLDER_NAME
# Arguments:
#   File to be sent out, a path.
#######################################
copy_file_to_gcs(){
  printf '%s\n' "=========================="
  local bucket
  bucket=$(get_value_from_json_file "${CONFIG_FILE}" "GCS_BUCKET")
  local folder
  folder=$(get_value_from_json_file "${CONFIG_FILE}" "${CONFIG_FOLDER_NAME}")
  local target="gs://${bucket}/${folder}"
  echo "Copy local file to target folder in Cloud Storage to start process."
  printf '%s\n' "  Source: $1"
  printf '%s\n' "  Target: ${target}"
  printf '%s' "Confirm? [Y/n]: "
  local input
  read -n1 -s input
  printf '%s\n' "${input}"
  if [[ -z ${input} || ${input} = 'y' || ${input} = 'Y' ]];then
    # gsutil support wildcard name. Use '*' to replace '[' here.
    local source
    source="$(printf '%s' "$1" | sed -r 's/\[/\*/g' )"
    gsutil cp ''"${source}"'' "${target}"
  else
    printf '%s\n' "User cancelled."
  fi
}

DEFAULT_INSTALL_TASKS=(
  "print_welcome Tentacles"
  load_config
  check_in_cloud_shell
  confirm_namespace confirm_project confirm_region
  confirm_integration_api confirm_auth_method
  check_permissions enable_apis
  create_bucket confirm_folder
  save_config
  create_subscriptions
  do_authentication
  deploy_tentacles
  post_installation
  "print_finished Tentacles"
)

if [[ "${BASH_SOURCE[0]}" -ef "$0" ]]; then
  run_default_function "$@"
else
  printf '%s\n' "Tentacles Bash Library is loaded."
fi
