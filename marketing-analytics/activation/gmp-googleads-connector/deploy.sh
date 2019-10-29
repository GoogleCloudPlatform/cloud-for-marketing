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

# Default project name is 'tentacles'. It will be used as prefix of Cloud
# Functions, PubSub, etc. You can change it here (only lowercase letters,
# numbers and dashes(-) are suggested).
PROJECT_NAME="${PROJECT_NAME:=tentacles}"

# Project configuration file.
CONFIG_FILE="./config.json"

# This parameter name is used by functions to load/save config.
CONFIG_FOLDER_NAME="OUTBOUND"
CONFIG_ITEMS=("GCS_BUCKET" "${CONFIG_FOLDER_NAME}" "PS_TOPIC")

# Service account key file.
SA_KEY_FILE="./keys/service-account.key.json"

# Default service account user name.
DEFAULT_SERVICE_ACCOUNT="${PROJECT_NAME}-api"

# Whether or not service account is required for this installation based on the
# selection of APIs.
NEED_SERVICE_ACCOUNT="false"

# To create topics/subscriptions.
ENABLED_INTEGRATED_APIS=()

# The APIs that will be enabled.
declare -A GOOGLE_CLOUD_APIS
GOOGLE_CLOUD_APIS=(
  ["iam.googleapis.com"]="Identity and Access Management (IAM) API"
  ["cloudresourcemanager.googleapis.com"]="Cloud Resource Manager API"
  ["firestore.googleapis.com"]="Google Cloud Firestore API"
  ["cloudfunctions"]="Cloud Functions API"
  ["pubsub"]="Cloud Pub/Sub API"
)

# Description of external APIs.
INTEGRATION_APIS_DESCRIPTION=(
  "Google Analytics Measurement Protocol"
  "Google Analytics Data Import"
  "Campaign Manager Conversions Upload"
  "SFTP Upload"
  "Google Ads conversions scheduled uploads based on Google Sheets"
  "Search Ads 360 Conversions Upload"
#  "Google Ads"
)

# All build-in external APIs.
INTEGRATION_APIS=(
  "N/A"
  "analytics"
  "dfareporting doubleclicksearch"
  "N/A"
  "sheets.googleapis.com"
  "doubleclicksearch"
  "googleads"
)

# Code of external APIs. Used to create different Cloud Pub/Sub topics and
# subscriptions for different APIs.
INTEGRATION_APIS_CODE=(
  "MP"
  "GA"
  "CM"
  "SFTP"
  "GS"
  "SA"
)

# Common permissions to install Tentacles
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
)

# https://cloud.google.com/iam/docs/understanding-roles#service-accounts-roles
declare -A GOOGLE_SERVICE_ACCOUNT_PERMISSIONS
GOOGLE_SERVICE_ACCOUNT_PERMISSIONS=(
  ["Service Account Admin"]="iam.serviceAccounts.create"
  ["Service Account Key Admin"]="iam.serviceAccounts.create"
)

print_welcome() {
  cat <<EOF
###########################################################
##                                                       ##
##            Start installation of Tentacles            ##
##                                                       ##
###########################################################

EOF
}

#######################################
# Confirm the APIs that this instance will support. Based on the selection of
# APIs, it may involve: 1) enable new APIs in Cloud Project; 2) usage of an
# service account key file. Based on the second one, it will update the
# permissions array to be checked.
# Globals:
#   INTEGRATION_APIS_DESCRIPTION
#   INTEGRATION_APIS
#   INTEGRATION_APIS_CODE
#   ENABLED_INTEGRATED_APIS
#   NEED_SERVICE_ACCOUNT
#   GOOGLE_CLOUD_APIS
#   GOOGLE_CLOUD_PERMISSIONS
#   GOOGLE_SERVICE_ACCOUNT_PERMISSIONS
# Arguments:
#   None
#######################################
confirm_apis() {
  (( STEP += 1 ))
  printf '%s\n' "STEP[${STEP}] Select the APIs that will be integrated:"
  local api
  for api in "${!INTEGRATION_APIS_DESCRIPTION[@]}"; do
    printf "%s) %s\n" "${api}" "${INTEGRATION_APIS_DESCRIPTION[$api]}"
  done
  printf '%s' "Use comma to separate APIs or * for all: [*]"
  local input=()
  IFS=', ' read -r -a input

  if [[ ${#input[@]} = 0 || ${input[0]} = '*' ]]; then
    for ((i=0; i<${#INTEGRATION_APIS_DESCRIPTION[@]}; i+=1)); do
      input["${i}"]="${i}"
    done
  fi
  local selection
  for selection in "${!input[@]}"; do
    local index="${input[$selection]}"
    ENABLED_INTEGRATED_APIS+=("${INTEGRATION_APIS_CODE["${index}"]}")
    if [[ ${INTEGRATION_APIS[${index}]} != "N/A" ]]; then
      NEED_SERVICE_ACCOUNT="true"
      GOOGLE_CLOUD_APIS[${INTEGRATION_APIS["${index}"]}]=\
"${INTEGRATION_APIS_DESCRIPTION["${index}"]}"
      printf '%s\n' "  Add ${INTEGRATION_APIS_DESCRIPTION["${index}"]} to \
enable APIs list."
    fi
  done
  if [[ ${NEED_SERVICE_ACCOUNT} = 'true' ]]; then
    local role
    for role in "${!GOOGLE_SERVICE_ACCOUNT_PERMISSIONS[@]}"; do
      GOOGLE_CLOUD_PERMISSIONS["${role}"]=\
"${GOOGLE_SERVICE_ACCOUNT_PERMISSIONS["${role}"]}"
    done
  fi
}

#######################################
# Create Cloud Pub/Sub topics and subscriptions based on the selected APIs.
# Globals:
#   PS_TOPIC
#   ENABLED_INTEGRATED_APIS
# Arguments:
#   None
# Returns:
#   0 if all topics and subscriptions are created, non-zero on error.
#######################################
create_subscriptions() {
  (( STEP += 1 ))
  cat <<EOF
STEP[${STEP}] Create Topics and Subscriptions for Pub/Sub...
  Rationale: Cloud Pub/sub subscription won't get the message published before \
its creation. So we create topics and subscriptions before real data comes.
EOF

  node -e "require('./index.js').initPubsub(process.argv[1], \
process.argv.slice(2))" "${PS_TOPIC}"  "${ENABLED_INTEGRATED_APIS[@]}"
  if [[ $? -gt 0 ]]; then
    printf '%s\n' "[Failed] Failed to create Topics or Subscriptions."
    return 1
  else
    printf '%s\n' "[OK] Successfully create Topics and Subscriptions."
    return 0
  fi
}

#######################################
# Download a service account key file and save as `$SA_KEY_FILE`.
# Globals:
#   SA_NAME
#   GCP_PROJECT
#   SA_KEY_FILE
# Arguments:
#   None
# Returns:
#   0 if service key files exists or created, non-zero on error.
#######################################
download_service_account_key() {
  (( STEP += 1 ))
  printf '%s\n' "STEP[${STEP}] Download key file of the Service Account ..."
  if [[ -z ${SA_NAME} ]];then
    confirm_service_account
  fi
  local suffix exist
  suffix=$(get_sa_domain_from_gcp_id "${GCP_PROJECT}")
  local email="${SA_NAME}@${suffix}"
  local prompt="Would you like to download the key file for [${email}] and \
save it as ${SA_KEY_FILE}? [Y/n] "
  local default_value="y"
  if [[ -f "${SA_KEY_FILE}" && -s "${SA_KEY_FILE}" ]]; then
    exist=$(get_value_from_json_file ${SA_KEY_FILE} 'client_email' 2>&1)
    if [[ ${exist} =~ .*("@${suffix}") ]]; then
      prompt="There is already a key file for [${exist}] with the id '\
$(get_value_from_json_file ${SA_KEY_FILE} 'private_key_id')'. Would you like to\
 create a new key to overwrite it? [N/y] "
      default_value="n"
    fi
  fi
  printf '%s' "${prompt}"
  local input
  read -r input
  input=${input:-"${default_value}"}
  if [[ ${input} = 'y' || ${input} = 'Y' ]];then
    printf '%s\n' "Start to download a new key file for [${email}]..."
    gcloud iam service-accounts keys create "${SA_KEY_FILE}" --iam-account \
"${email}"
    if [[ $? -gt 0 ]]; then
      printf '%s\n' "[Failed] Failed to download new key files for [${email}]."
      return 1
    else
      printf '%s\n' "[OK] New key file is saved at [${SA_KEY_FILE}]."
      return 0
    fi
  else
    printf '%s\n' "[Skipped] To know more about service account key file, see: \
https://cloud.google.com/iam/docs/creating-managing-service-account-keys";
    return 0
  fi
}

#######################################
# Make sure a service account for integration exists and set the email of the
# service account to the global variable `SA_NAME`.
# Globals:
#   GCP_PROJECT
#   SA_KEY_FILE
#   SA_NAME
#   DEFAULT_SERVICE_ACCOUNT
# Arguments:
#   None
#######################################
confirm_service_account() {
  cat <<EOF
  Rationale: Some external APIs may require authentication based on OAuth or \
JWT(service account). For example, Google Analytics Data Import or Campaign \
Manager. In this step, we'll prepare the service account.
  For more information, see: https://cloud.google.com/iam/docs/creating-managing-service-accounts
EOF

  local suffix
  suffix=$(get_sa_domain_from_gcp_id "${GCP_PROJECT}")
  local email
  if [[ -f "${SA_KEY_FILE}" && -s "${SA_KEY_FILE}" ]]; then
    email=$(get_value_from_json_file "${SA_KEY_FILE}" 'client_email')
    if [[ ${email} =~ .*("@${suffix}") ]]; then
      printf '%s' "There is already a key file with service account[${email}]. \
Would you like to create a new one? [N/y]"
      local input
      read -r input
      if [[ ${input} != 'y' && ${input} != 'Y' ]]; then
        printf '%s\n' "[OK] Will use exist service account [${email}]"
        SA_NAME=$(printf "${email}" | cut -d@ -f1)
        return 0
      fi
    fi
  fi

  SA_NAME="${SA_NAME:-"${DEFAULT_SERVICE_ACCOUNT}"}"
  while :; do
    printf '%s' "Input the name of service account [${SA_NAME}]: "
    local input sa_elements=() sa
    read -r input
    input=${input:-"${SA_NAME}"}
    IFS='@' read -a sa_elements <<< "${input}"
    if [[ ${#sa_elements[@]} = 1 ]]; then
      echo "  Appended default suffix to service account name and get: ${email}"
      sa="${input}"
      email="${sa}@${suffix}"
    else
      if [[ ${sa_elements[1]} != "${suffix}" ]]; then
        printf '%s\n' "  [Error] Service account domain name ${sa_elements[1]} \
doesn't belong to current project. Should be: ${suffix}."
        continue
      fi
      sa="${sa_elements[0]}"
      email="${input}"
    fi

    printf 'Checking the existence of the service account...'
    if ! result=$(gcloud iam service-accounts describe "${email}" 2>&1); then
      printf '  not exist. Try to create...\n'
      gcloud iam service-accounts create "${sa}" --display-name \
"Tentacles API requester"
      if [[ $? -gt 0 ]]; then
        printf 'Failed. Please try again...\n'
      else
        printf 'Create successfully.\n'
        SA_NAME=${sa}
        break
      fi
    else
      printf ' found.\n'
      SA_NAME=${sa}
      break
    fi
  done
  printf '%s\n' "[OK] Service Account [${SA_NAME}] is ready."
}

#######################################
# Deploy 3 Cloud Functions of Tentacles.
# Globals:
#   REGION
#   CF_RUNTIME
#   PROJECT_NAME
#   GCS_BUCKET
#   CONFIG_FOLDER_NAME
#   PS_TOPIC
#   SA_KEY_FILE
# Arguments:
#   None
# Returns:
#   0 if all cloud functions deployed, non-zero on error.
#######################################
deploy_tentacles() {
  (( STEP += 1 ))
  printf '%s\n' "STEP[${STEP}] Start to deploy Tentacles..."
  printf '%s\n' "Tentacles is combined of three Cloud Functions."
  while [[ -z ${REGION} ]]; do
    set_region
  done
  printf '%s\n' "[ok] Will deploy Cloud Functions to ${REGION}."

  local cf_flag=()
  cf_flag+=(--region="${REGION}")
  cf_flag+=(--timeout=540 --memory=2048MB --runtime="${CF_RUNTIME}")
  cf_flag+=(--set-env-vars=TENTACLES_TOPIC_PREFIX="${PS_TOPIC}")

  printf '%s\n' " 1. '${PROJECT_NAME}_init' based on Cloud Storage \
bucket[${GCS_BUCKET}]."
  gcloud functions deploy "${PROJECT_NAME}"_init --entry-point initiate \
--trigger-bucket "${GCS_BUCKET}" "${cf_flag[@]}" \
--set-env-vars=TENTACLES_OUTBOUND="${!CONFIG_FOLDER_NAME}"
  quit_if_failed $?

  printf '%s\n' " 2. '${PROJECT_NAME}_tran' based on Pub/Sub \
topic[${PS_TOPIC}-trigger]."
  gcloud functions deploy "${PROJECT_NAME}"_tran --entry-point transport \
--trigger-topic "${PS_TOPIC}"-trigger "${cf_flag[@]}"
  quit_if_failed $?

  if [[ -f "${SA_KEY_FILE}" ]]; then
    cf_flag+=(--set-env-vars=API_SERVICE_ACCOUNT="${SA_KEY_FILE}")
  fi
  printf '%s\n' " 3. '${PROJECT_NAME}_api' based on Pub/Sub \
topic[${PS_TOPIC}-push]."
  gcloud functions deploy "${PROJECT_NAME}"_api --entry-point requestApi \
--trigger-topic "${PS_TOPIC}"-push "${cf_flag[@]}"
  quit_if_failed $?
}

#######################################
# Check Firestore status and print next steps information after installation.
# Globals:
#   NEED_SERVICE_ACCOUNT
#   SA_KEY_FILE
#   PROJECT_NAME
#   GCS_BUCKET
#   CONFIG_FOLDER_NAME
#   PS_TOPIC
#   SA_KEY_FILE
# Arguments:
#   None
#######################################
post_installation() {
  (( STEP += 1 ))
  printf '%s\n' "STEP[${STEP}] Post installation."
  check_firestore_existence
  printf '%s\n' "[ok] Firestore/Datastore is ready."
  if [[ ${NEED_SERVICE_ACCOUNT} = 'true' ]]; then
    local exist
    exist=$(get_value_from_json_file "${SA_KEY_FILE}" 'client_email')
    cat <<EOF
Some enabled APIs require service account. This requires extra steps to grant \
access to the service account's email in external systems, e.g. Google \
Analytics or Campaign Manager.
This should be done before use Tentacles to send out data to them.
  1. For Google Analytics Data Import
   * Set up Data set for Data Import, see: \
https://support.google.com/analytics/answer/3191417?hl=en
   * Grant the 'Edit' access to [${exist}]
  2. For Campaign Manager
   * DCM/DFA Reporting and Trafficking API's Conversions service, see: \
https://developers.google.com/doubleclick-advertisers/guides/conversions_overview
   * Create User Profile for [${exist}] and grant the access to 'Insert \
offline conversions'
  3. For Google Ads conversions scheduled uploads based on Google Sheets
   * Import conversions from ad clicks into Google Ads, see: \
https://support.google.com/google-ads/answer/7014069
   * Add [${exist}] as an Editor of the Google Spreadsheet that Google Ads \
will take conversions from.
EOF
  fi
  cat <<EOF

Finally, follow the document (https://github.com/GoogleCloudPlatform/cloud-for-marketing/blob/master/marketing-analytics/activation/gmp-googleads-connector/README.md#4-api-details)\
 to create configuration of the integration.
Save the configuration to a JSON file, e.g. './config_api.json' and run:
  ./deploy.sh update_api_config
to update the configuration to Firestore/Datastore before Tentacles can use \
them.
EOF
}

print_finished(){
  cat <<EOF
###########################################################
##          Tentacles has been installed.                ##
###########################################################
EOF
}

#######################################
# Start the automatic process to install Tentalces.
# Globals:
#   NEED_SERVICE_ACCOUNT
# Arguments:
#   None
#######################################
install_tentacles() {

  print_welcome
  load_config

  local tasks=(
    check_in_cloud_shell prepare_dependencies
    confirm_project confirm_region confirm_apis
    check_permissions enable_apis create_bucket
    confirm_folder confirm_topic save_config
    create_subscriptions
  )
  local task
  for task in "${tasks[@]}"; do
    "${task}"
    quit_if_failed $?
  done
  # Confirmed during the tasks.
  if [[ ${NEED_SERVICE_ACCOUNT} = 'true' ]]; then
    download_service_account_key
    quit_if_failed $?
  fi

  deploy_tentacles
  post_installation
  print_finished
}

#######################################
# Print email of service account.
# Globals:
#   SA_KEY_FILE
# Arguments:
#   None
#######################################
print_service_account(){
  printf '%s\n' "=========================="
  local email
  email=$(get_value_from_json_file "${SA_KEY_FILE}" 'client_email')
  printf '%s\n' "The email of current service account is: ${email}"
}

#######################################
# Upload API configuration in local JSON file to Cloud Firestore or Datastore.
# It will adapt to Firestore or Datastore automatically.
# Globals:
#   None
# Arguments:
#   None
#######################################
update_api_config(){
  printf '%s\n' "=========================="
  printf '%s\n' "Update API configurations in into Firestore/Datastore."
  check_authentication
  quit_if_failed $?
  check_firestore_existence

  local default_config_file='./config_api.json'
  printf '%s' "Please input the configuration file [${default_config_file}]:"
  local api_config
  read -r api_config
  api_config=${api_config:-"${default_config_file}"}
  printf '\n'
  node -e "require('./index.js').uploadApiConfig(require(process.argv[1]))" \
"${api_config}"
}

#######################################
# Invoke API requester cloud function directly based on a local file.
# Please note: the API configuration is still expected to be on Cloud. You need
# to update API config first if you modified any.
# Globals:
#   SA_KEY_FILE
# Arguments:
#   File to be sent out, a path.
#######################################
run_test_locally(){
  printf '%s\n' "=========================="
  cat <<EOF
Invoke API requester cloud function directly based on a local file.
Please note: the API configuration is still expected to be on Cloud. You need \
to update API config first if you modified any.

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
# Please note: the API configuration is still expected to be on Cloud. You need
# to update API config first if you modified any.
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
  printf '%s' "Confirm? [Y/n]"
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

if [[ "${BASH_SOURCE[0]}" -ef "$0" ]]; then
  MAIN_FUNCTION="install_tentacles"
  run_default_function "$@"
else
  printf '%s\n' "Tentacles Bash Library is loaded."
fi
