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

# Cloud Functions Runtime Environment.
CF_RUNTIME="${CF_RUNTIME:=nodejs10}"

# Counter for steps.
STEP=0
# Name of the default function when run this script without any arguments.
MAIN_FUNCTION="default_install"
# Array of tasks that the default function will execute.
DEFAULT_INSTALL_TASKS=()

# Service account key file.
SA_KEY_FILE="./keys/service-account.key.json"
# OAuth token file.
OAUTH2_TOKEN_JSON="./keys/oauth2.token.json"

# Authentication is required for the external API(s).
NEED_AUTHENTICATION="false"
# OAuth is required for the selected API(s).
NEED_OAUTH="false"
# Service account is required for selected API(s).
NEED_SERVICE_ACCOUNT="false"

# DEBUG is a deployed env variable for Cloud Functions. It is used to control
# the logging level and it will only be set when it is not "false".
DEBUG="false"
# IN_GCP is a deployed env variable for Cloud Functions.
# StackDriver Logging will be used to replace Console.log when this is true.
IN_GCP="true"

# The APIs that will be used in this solution which means need to be enabled.
declare -A GOOGLE_CLOUD_APIS
GOOGLE_CLOUD_APIS=(
  ["appengine.googleapis.com"]="App Engine Admin API"
  ["cloudbuild.googleapis.com"]="Google Build API"
  ["cloudresourcemanager.googleapis.com"]="Resource Manager API"
  ["iam.googleapis.com"]="Cloud Identity and Access Management API"
)

# Permissions that are required to carry out this installation.
declare -A GOOGLE_CLOUD_PERMISSIONS

# External APIs that may be used in the solutions.
# Selected APIs will be enabled in the GCP.
declare EXTERNAL_APIS=(
  "analytics.googleapis.com" #GA
  "dfareporting.googleapis.com" #CM360
  "doubleclickbidmanager.googleapis.com" #DV360 reports / DBM reports
  "doubleclicksearch.googleapis.com" #SA360
  "sheets.googleapis.com"
  "googleads.googleapis.com"
  "adsdatahub.googleapis.com"
)

# Some APIs only support OAuth which needs an process to generate refresh token.
# If none of these APIs are enabled, then the use can choose to use JWT auth.
declare EXTERNAL_APIS_OAUTH_ONLY=(
  "googleads.googleapis.com"
  "doubleclickbidmanager.googleapis.com"
)

# API scopes for OAuth authentication.
declare -A EXTERNAL_API_SCOPES
EXTERNAL_API_SCOPES=(
  ["analytics.googleapis.com"]="https://www.googleapis.com/auth/analytics"
  ["dfareporting.googleapis.com"]=\
"https://www.googleapis.com/auth/ddmconversions \
    https://www.googleapis.com/auth/dfareporting \
    https://www.googleapis.com/auth/dfatrafficking"
  # Currently only report for DV360 API
  ["doubleclickbidmanager.googleapis.com"]=\
"https://www.googleapis.com/auth/doubleclickbidmanager"
  ["doubleclicksearch.googleapis.com"]=\
"https://www.googleapis.com/auth/doubleclicksearch"
  ["sheets.googleapis.com"]="https://www.googleapis.com/auth/spreadsheets"
  ["googleads.googleapis.com"]="https://www.googleapis.com/auth/adwords"
  ["adsdatahub.googleapis.com"]="https://www.googleapis.com/auth/adsdatahub"
)

# Enabled APIs' OAuth scopes.
ENABLED_OAUTH_SCOPES=()

# https://cloud.google.com/iam/docs/understanding-roles#service-accounts-roles
declare -A GOOGLE_SERVICE_ACCOUNT_PERMISSIONS
GOOGLE_SERVICE_ACCOUNT_PERMISSIONS=(
  ["Service Account Admin"]="iam.serviceAccounts.create"
  ["Service Account Key Admin"]="iam.serviceAccounts.create"
)

# Storage regions.
STORAGE_REGIONS=(
  "Multi-regional"
  "North America"
  "South America"
  "Europe"
  "Asia"
  "Australia"
)

# Region location list.
STORAGE_REGIONS_PARAMETER=(
  "MULTI_REGIONAL"
  "NORTH_AMERICA"
  "SOUTH_AMERICA"
  "EUROPE"
  "ASIA"
  "AUSTRALIA"
)
MULTI_REGIONAL=("ASIA" "EU" "US")
NORTH_AMERICA=(
  "northamerica-northeast1"
  "us-central1"
  "us-east1"
  "us-east4"
  "us-west1"
  "us-west2"
)
SOUTH_AMERICA=("southamerica-east1")
EUROPE=(
  "europe-north1"
  "europe-west1"
  "europe-west2"
  "europe-west3"
  "europe-west4"
  "europe-west6"
)
ASIA=(
  "asia-east1"
  "asia-east2"
  "asia-northeast1"
  "asia-northeast2"
  "asia-south1"
  "asia-southeast1"
)
AUSTRALIA=("australia-southeast1")

# Preparation functions.
#######################################
# Mimic a Cloud Shell environment to enable local running.
# Globals:
#   CLOUD_SHELL
#   GCP_PROJECT
#   CONFIG_FILE
# Arguments:
#   None
#######################################
emulate_cloud_shell() {
  if [[ ${CLOUD_SHELL} != 'true' ]];then
    echo "To emulate Cloud Shell, set env CLOUD_SHELL, GCP_PROJECT and check \
ADC credentials."
    CLOUD_SHELL='true'
    echo "  CLOUD_SHELL = ${CLOUD_SHELL}"

    local current_project
    current_project=$(gcloud config get-value project 2>/dev/null)
    local target_project
    while [[ -z ${GCP_PROJECT} ]]; do
      if [[ -f "${CONFIG_FILE}" ]]; then
        target_project=$(get_value_from_json_file "${CONFIG_FILE}" 'PROJECT_ID')
      else
        printf '%s' "Can't find ${CONFIG_FILE}. Enter your Google Cloud \
project ID: [${current_project}]"
        local project
        read -r project
        project=${project:-"${current_project}"}
        target_project=${project}
      fi
      if [[ -n ${current_project} && \
${current_project} != "${target_project}" ]];then
        printf '%s' "Setting the Google Cloud project to [${target_project}]..."
        gcloud config set project "${target_project}"
        if [[ $? -gt 0 ]]; then
          continue
        fi
      fi
      export GCP_PROJECT=$(gcloud config get-value project 2>/dev/null)
    done
    echo "  PROJECT_ID = ${GCP_PROJECT}"

    gcloud auth application-default print-access-token >/dev/null 2>&1
    if [[ $? -eq 0 ]]; then
      echo "  GOOGLE_APPLICATION_CREDENTIALS is set. OK to go."
    else
      echo -e "\e[31m  GOOGLE_APPLICATION_CREDENTIALS isn't set. Some \
functions may not work properly. Run 'gcloud auth application-default login' \
to login ADC.\e[0m"
    fi

    echo ""
  fi
}

#######################################
# Check whether current auth in local environment can carry out the tasks.
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   0 if passed, non-zero on error.
#######################################
check_authentication() {
  local permissions=("datastore.databases.get" "datastore.entities.create")
  local missed
  missed=$(get_number_of_missed_permissions "${permissions[@]}")
  if [[ $missed -gt 0 ]]; then
    echo "  As the current user, you don't have enough permissions to update \
Firestore. After you're granted the 'Cloud Datastore User' role, try again."
    return 1
  fi
  return 0
}

# Process functions

#######################################
## Load settings of previous installation and set them as global parameters.
## Globals:
##   CONFIG_FILE
##   CONFIG_ITEMS
##   GCP_PROJECT
## Arguments:
##   None
########################################
load_config() {
  if [[ -f "${CONFIG_FILE}" ]]; then
    printf '%s\n' "Found configuration file '${CONFIG_FILE}'. Loading the \
file..."
    local i value key
    for i in "${!CONFIG_ITEMS[@]}"; do
      value=$(get_value_from_json_file "${CONFIG_FILE}" "${CONFIG_ITEMS[$i]}")
      key=${CONFIG_ITEMS[$i]}
      declare -g "${key}=${value}"
      printf '%s\n' "  ${key} = ${!key}"
    done
    printf '%s\n\n' "  (Configuration values can be changed during the \
installation process.)"
  else
    printf '%s\n\n' "There is no configuration file '${CONFIG_FILE}'. \
Configuration values can be set and saved during the installation process."
  fi
	export GCP_PROJECT=$(gcloud config get-value project)
}

#######################################
# Check whether the environment is in Cloud Shell.
# Globals:
#   CLOUD_SHELL
# Arguments:
#   None
# Returns:
#   0 in a Cloud Shell, non-zero not.
#######################################
check_in_cloud_shell() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Checking to see if you're using Cloud Shell..."
  cat <<EOF
  We require Cloud Shell for this installation because many Google Cloud \
processes are part of this installation. Cloud Shell leverages current tools \
and might avoid authorization issues.
EOF
  if [[ ${CLOUD_SHELL} != 'true' ]];then
    printf '%s\n' "Check failed. Use Cloud Shell for this installation. See \
https://cloud.google.com/shell/ for more information."
    return 1
  else
    printf '%s\n' "OK. You're using Cloud Shell."
    return 0
  fi
}

#######################################
# Prepare dependency libraries.
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   0 succeeded, non-zero on error.
#######################################
prepare_dependencies() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Preparing dependency libraries..."
  npm install -s
  local flag=$?
  if [[ ${flag} -gt 0 ]];then
    echo "Failed to install dependencies. Try to run the script again. If you \
still get errors, report the issue to \
https://github.com/GoogleCloudPlatform/cloud-for-marketing/issues."
    return 1
  else
    printf '%s\n' "OK. All dependencies are ready."
    return 0
  fi
}

#######################################
# Confirm the project namespace.
# Globals:
#   PROJECT_NAMESPACE
# Arguments:
#   None
#######################################
confirm_namespace() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Confirming project namespace..."
  cat <<EOF
  To avoid potential conflicts with other applications that use Cloud Functions \
or Pub/Sub topics, a unified prefix (namespace) will be used for those names.
EOF
  local namespace
  printf '%s' "Enter the project namespace [${PROJECT_NAMESPACE}]: "
  read -r namespace
  namespace=${namespace:-"${PROJECT_NAMESPACE}"}
  while [[ ! $namespace =~ ^[a-z][a-z0-9]{0,19}$ ]]; do
    printf '%s\n' "Namespace only contains lower letters and digits, starts \
with a letter and is not longer than 20 characters."
    printf '%s' "Enter the project namespace again [${namespace}]: "
    read -r namespace
  done
  PROJECT_NAMESPACE=${namespace:-"${PROJECT_NAMESPACE}"}
  printf '%s\n' "OK. Continue with project namespace [${PROJECT_NAMESPACE}]."
}

#######################################
# Confirm the Google Cloud project.
# Globals:
#   GCP_PROJECT
# Arguments:
#   None
# Returns:
#   0 succeeded, non-zero on error.
#######################################
confirm_project() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Confirming the Google Cloud project..."
  while :; do
    printf '%s' "Enter the ID for the Google Cloud project where you want this \
solution installed: [${GCP_PROJECT}]"
    local input result
    read -r input
    input=${input:-"${GCP_PROJECT}"}
    result=$(gcloud config set project "${input}" --user-output-enabled=false \
2>&1)
    if [[ -z ${result} ]]; then
      printf '%s\n' "OK. Continuing installation in [${input}]..."
      GCP_PROJECT="${input}"
			export GCP_PROJECT=$(gcloud config get-value project)
      return 0
    else
      printf '%s\n' "  Installation failed. Reason: ${result}"
    fi
  done
}

#######################################
# Confirm and set env variable for the region for Cloud Functions.
# Globals:
#   SOLUTION_NAME
#   REGION
# Arguments:
#   None
#######################################
confirm_region() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Select the region to use for deploying \
${SOLUTION_NAME}."
  cat <<EOF
This solution uses Cloud Functions and Cloud Storage. Based on your \
requirements, select the region to use for deployment. Because of latency and \
pricing considerations, we recommend you use the same region for Cloud \
Functions and Cloud Storage. See \
https://cloud.google.com/functions/docs/locations/ for more information.
EOF
  set_region
  printf '%s\n' "OK. ${REGION} will be used for deployment."
}

#######################################
# Set the env variable 'REGION' as the deploy target for Cloud Functions.
# Globals:
#   PROJECT_NAMESPACE
#   REGION
# Arguments:
#   None
#######################################
set_region() {
  local locations_response
  locations_response=$(gcloud functions regions list)
  local locations
  locations=($(printf "${locations_response}"|grep "projects"|sed \
's/projects\/.*\/locations\///'))
  local region
  while :; do
    local exist_functions
    exist_functions=($(gcloud functions list --filter=\
"name~${PROJECT_NAMESPACE}" --format="csv[no-heading](name,REGION)"))
    if [[ ${#exist_functions[@]} -gt 0 ]]; then
      local exist_region
      exist_region=$(printf "${exist_functions[0]}" | cut -d, -f2 | uniq)
      printf '%s\n' "Related Cloud Functions are already installed in region: \
${exist_region}."
      local i
      for i in "${!exist_functions[@]}"; do
        printf '  %s\n' "${exist_functions[$i]}"
      done
      printf '%s' "Would you like to continue updating related Cloud Functions \
in the region '${exist_region}'? [Y/n]: "
      local continue
      read -r continue
      continue=${continue:-"Y"}
      if [[ ${continue} = "Y" || ${continue} = "y" ]]; then
        region=${exist_region}
        break
      else
        printf '%s' "Deploying to other regions deletes existing Cloud \
Functions for that region. Do you want to continue? [N/y]: "
        local confirm_delete
        read -r confirm_delete
        if [[ ${confirm_delete} = "Y" || ${confirm_delete} = "y" ]]; then
          for i in "${!exist_functions[@]}"; do
            local exist_function
            exist_function=$(printf "${exist_functions[$i]}" | cut -d, -f1)
            local function_region
            function_region=$(printf "${exist_functions[$i]}" | cut -d, -f2)
            gcloud functions delete --region="${function_region}" \
"${exist_function}"
          done
        else
          continue
        fi
      fi
    else
      printf '%s\n' "Select from the following regions for deploying Cloud \
Functions:"
      select region in "${locations[@]}"; do
        if [[ " ${locations[@]} " =~ " ${region} " ]]; then
          break 2
        fi
      done
    fi
  done
  REGION="${region}"
}

#######################################
# Make sure the global $REGION has a value. This is used before deploy Cloud
# Functions.
# Globals:
#   REGION
#######################################
ensure_region() {
  while [[ -z ${REGION} ]]; do
    set_region
  done
}

#######################################
# Confirm the APIs that this instance supports. Based on the APIs selected, you
# might need to do the following: 1) Enable new APIs in your Google Cloud
# project; 2) use a service account key file. If the second case, the
# permissions array will be updated to be checked.
# Globals:
#   INTEGRATION_APIS_DESCRIPTION
#   INTEGRATION_APIS
#   GOOGLE_CLOUD_APIS
#   NEED_AUTHENTICATION
#   NEED_OAUTH
# Arguments:
#   Optional appended function for selected APIs.
#######################################
confirm_apis() {
  printf '%s\n' "Select the APIs that will be integrated: "
  local apiIndex
  for apiIndex in "${!INTEGRATION_APIS_DESCRIPTION[@]}"; do
    printf "%s) %s\n" "${apiIndex}" "${INTEGRATION_APIS_DESCRIPTION[$apiIndex]}"
  done
  printf '%s' "Use a comma to separate APIs or enter * for all: [*]"
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
    local apiGroup="${INTEGRATION_APIS[${index}]}"
    # Extra operation for selected APIs.
    if [[ -n $1 ]]; then
      $1 "${index}"
    fi
    if [[ ${apiGroup} != "N/A" ]]; then
      local apis=(${apiGroup}) #Need to convert APIs to array.
      local singleApi
      for singleApi in "${!apis[@]}"; do
        local api="${apis["${singleApi}"]}"
        ENABLED_OAUTH_SCOPES+=($(join_string_array "%20" \
          "${EXTERNAL_API_SCOPES["${api}"]}"))
        if [[ " ${EXTERNAL_APIS_OAUTH_ONLY[@]} " =~ " ${api} " ]]; then
           NEED_OAUTH="true"
        fi
      done
      NEED_AUTHENTICATION="true"
      GOOGLE_CLOUD_APIS["${apiGroup}"]="${INTEGRATION_APIS_DESCRIPTION["${index}"]}"
      printf '%s\n' "  Add ${INTEGRATION_APIS_DESCRIPTION["${index}"]} to \
enable API list."
    else
      printf '%s\n' "  ${INTEGRATION_APIS_DESCRIPTION["${index}"]} doesn't \
need to be enabled."
    fi
  done
  printf '\n'
}

#######################################
# Confirm the authentication method that this instance will use. Based on the
# APIs selected, you might: 1) do not need to authenticate; 2) have to use OAuth
# 2.0; 3) use service account or OAuth 2.0. In the last situation, you can
# select the authentication method.
# Globals:
#   NEED_AUTHENTICATION
#   NEED_OAUTH
#   NEED_SERVICE_ACCOUNT
#   GOOGLE_CLOUD_PERMISSIONS
#   GOOGLE_SERVICE_ACCOUNT_PERMISSIONS
# Arguments:
#   None
#######################################
confirm_auth_method() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Checking authentication method for selected \
API(s):"
  if [[ ${NEED_AUTHENTICATION} != 'true' ]]; then
    printf '%s\n\n'  "OK. No authentication is required."
    return 0
  fi
  if [[ ${NEED_OAUTH} = 'true' ]]; then
    printf '%s\n\n'  "OK. OAuth is required by selected API(s)."
    return 0
  fi
  printf '%s\n' "Selected API(s) require authentication. Please select \
authentication method:"
  local auths=("Service Account (recommended)" "OAuth")
  select auth in "${auths[@]}"; do
    if [[ " ${auths[@]} " =~ " ${auth} " ]]; then
      break
    fi
  done
  if [[ ${auth} = 'OAuth' ]]; then
    NEED_OAUTH="true"
    printf '%s\n' "... OAuth is selected."
  else
    NEED_SERVICE_ACCOUNT="true"
    local role
    for role in "${!GOOGLE_SERVICE_ACCOUNT_PERMISSIONS[@]}"; do
      GOOGLE_CLOUD_PERMISSIONS["${role}"]=\
"${GOOGLE_SERVICE_ACCOUNT_PERMISSIONS["${role}"]}"
    done
    printf '%s\n' "... Service Account is selected."
  fi
  printf '\n'
}

#######################################
# Check whether the current operator has enough permissions to complete the
# installation.
# Globals:
#   GOOGLE_CLOUD_PERMISSIONS
# Arguments:
#   None
# Returns:
#   0 if all permissions are granted, 1 if not.
#######################################
check_permissions() {
    (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Checking current user's access..."
  local role error
  for role in "${!GOOGLE_CLOUD_PERMISSIONS[@]}"; do
    printf '%s'  "  Checking permissions for ${role}... "
    local permissions
    permissions=(${GOOGLE_CLOUD_PERMISSIONS[${role}]})
    local missed
    missed=$(get_number_of_missed_permissions "${permissions[@]}")
    if [[ $missed -gt 0 ]]; then
      message="missed ${missed}, failed"
      error=1
    else
      message='successfully'
    fi
    printf '%s\n' " ${message}."
  done
  if [[ ${error} -gt 0 ]]; then
    printf '%s\n' "Permissions check failed."
    return 1
  else
    echo "OK. Permissions check passed for Google Cloud project \
[${GCP_PROJECT}]."
    return 0
  fi
}

#######################################
# Check whether the current operator has enough permissions to complete the
# installation. This is native version based on 'gcloud' and 'curl', so it does
# not rely on Nodejs library.
# Globals:
#   GOOGLE_CLOUD_PERMISSIONS
# Arguments:
#   None
# Returns:
#   0 if all permissions are granted, 1 if not.
#######################################
check_permissions_native() {
  check_permissions
}

#######################################
# Check whether current user has the given permissions. This is based on
# 'gcloud' and 'curl', so it does not rely on Nodejs library.
# Globals:
#   GCP_PROJECT
# Arguments:
#   Array of string, permissions
# Returns:
#   Number of missed permissions
#######################################
get_number_of_missed_permissions() {
  local accessToken
  accessToken=$(gcloud auth print-access-token)
  local permissions
  permissions=("$@")
  local expectedPermissionNumber
  expectedPermissionNumber="${#permissions[@]}"
  local permissionsStr
  permissionsStr=$(node -e \
"console.log(process.argv.slice(1).join('\\\",\\\"'))" "${permissions[@]}") \
1>/dev/null
  local request=(
    -s
    "https://cloudresourcemanager.googleapis.com/v1/projects/${GCP_PROJECT}:testIamPermissions"
    -H "Accept: application/json"
    -H "Content-Type: application/json"
    -H "Authorization: Bearer ${accessToken}"
    -d "{\"permissions\":[\"${permissionsStr}\"]}"
  )
  local response
  response=$(curl "${request[@]}")
  local returnedPermissionNumber
  returnedPermissionNumber=$(node -e \
"const permissions=JSON.parse(process.argv[1]).permissions; \
console.log(permissions ? permissions.length : 0)" "${response}") 1>/dev/null
  local missed
  (( missed=$expectedPermissionNumber-$returnedPermissionNumber ))
  echo $missed
}

#######################################
# Enable APIs in Google Cloud project.
# Globals:
#   GOOGLE_CLOUD_APIS
# Arguments:
#   None
# Returns:
#   0 if all Apis enabled, 1 on not.
#######################################
enable_apis() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Enabling necessary components..."
  local error=0 api single_api
  for api in "${!GOOGLE_CLOUD_APIS[@]}"; do
    printf '%s' "  Enabling ${GOOGLE_CLOUD_APIS[${api}]}... "
    for single_api in ${api}; do
      gcloud services enable "${single_api}" 2>/dev/null
      if [[ $? -gt 0 ]]; then
        message='failed'
        error=1
      else
        message='successfully'
      fi
      printf '%s\n' "[${single_api}] ${message}."
    done
  done
  if [[ ${error} -gt 0 ]]; then
    printf '%s\n' "Enabling some APIs failed."
    return 1
  else
    printf '%s\n' "OK. APIs are enabled for Google Cloud project \
[${GCP_PROJECT}]."
    return 0
  fi
}

#######################################
# Confirm or create a Cloud Storage bucket for this project.
# Globals:
#   SOLUTION_NAME
#   GCS_BUCKET
#   GCP_PROJECT
#   REGION
#   STORAGE_REGIONS
#   STORAGE_REGIONS_PARAMETER
# Arguments:
#   None
#######################################
create_bucket() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Entering a Cloud Storage Bucket name..."
  cat <<EOF
  This solution checks for new files added to this bucket. Event-driven \
architecture like this reduces the interval between scheduled checks and \
simplifies the solution. See \
https://cloud.google.com/functions/docs/calling/storage for more information.

You can use an existing bucket or create a new one here.
EOF
#  generate default Bucket name
  local default_bucket_name
  default_bucket_name=$(get_default_bucket_name "${GCP_PROJECT}")
  GCS_BUCKET=${GCS_BUCKET:-$default_bucket_name}
# available buckets in current project
  local all_buckets
  all_buckets="$(gsutil ls)";
  while :; do
    printf '%s' "Enter the name of your existing Cloud Storage bucket \
[${GCS_BUCKET}]: "
    local bucket
    read -r bucket
    bucket=${bucket:-$GCS_BUCKET}
    local bucket_str="gs://${bucket}/"
    if [[ ${all_buckets} == *"${bucket_str}"* ]]; then
      printf '%s\n' "OK. The bucket [${bucket}] exists in your current \
project, [${GCP_PROJECT}]."
      GCS_BUCKET=${bucket}
      break
    fi
    printf '%s\n' "  Checking the existence of Cloud Storage bucket \
[$bucket]..."
    local result
    result="$(gsutil ls -p "${GCP_PROJECT}" "${bucket_str}" 2>&1)"
    if [[ ${result} =~ .*(BucketNotFoundException: 404 ).* ]]; then
      printf '%s\n' "  [$bucket] doesn't exist. Continuing to create the \
bucket..."
      if [[ -n ${REGION} ]]; then
        printf '%s' "Would you like to create the Cloud Storage bucket \
[$bucket] at '${REGION}'? [Y/n]: "
        local user_region
        read -r user_region
        user_region=${user_region:-"Y"}
        if [[ ${user_region} = "Y" || ${user_region} = "y" ]]; then
          printf '%s\n' "  Attempting to create the bucket [${bucket}] at \
${REGION}..."
          gsutil mb -c REGIONAL -l "${REGION}" "${bucket_str}"
          if [[ $? -gt 0 ]]; then
            printf '%s\n' "Failed to create the bucket [${bucket}]. Try again."
            continue
          else
            GCS_BUCKET=${bucket}
            break
          fi
        fi
      fi
      printf '%s\n' "Select the region for Cloud Storage:"
      local region class
      select region in "${STORAGE_REGIONS[@]}"; do
        if [[ -n "${region}" ]]; then
          if [[ $REPLY = 1 ]]; then
            class="MULTI_REGIONAL"
          else
            class="REGIONAL"
          fi
          cat <<EOF
Selected region [${region}] class is ${class}. See \
https://cloud.google.com/storage/docs/storage-classes for more information \
about the storage classes that Cloud Storage offers.

Continue to select the location for the Cloud Storage bucket. See \
https://cloud.google.com/storage/docs/locations for more information about \
bucket locations. Enter your selection, or enter 0 to return to region \
selection:
EOF
          declare -n options="${STORAGE_REGIONS_PARAMETER["$((REPLY-1))"]}"
          local location
          select location in "${options[@]}"; do
            if [[ -n "${location}" ]]; then
              printf '%s\n' "  Attempting to create the bucket [${bucket}] at \
${location}..."
              gsutil mb -c "${class}" -l "${location}" "${bucket_str}"
              if [[ $? -gt 0 ]]; then
                printf '%s\n' "Failed to create bucket [${bucket}]. Try again."
                break 2
              else
                GCS_BUCKET=${bucket}
                break 3
              fi
            elif [[ ${REPLY} = 0 ]]; then
              break
            else
              printf '%s\n' "Select the location or enter 0 to return to \
region selection:"
            fi
          done
        fi
        printf '%s\n' "Select the Cloud Storage region or press Enter to \
refresh the list:"
      done
    elif [[ ${result} =~ .*(AccessDeniedException: 403 ).* ]]; then
      printf '%s\n' "  The bucket [${bucket}] exists, and the current account \
cannot access it."
      GCS_BUCKET="${default_bucket_name}"
    else
      printf '%s\n' "  The bucket [${bucket}] exists in another project, so it \
can't be the trigger source here."
      GCS_BUCKET="${default_bucket_name}"
    fi
    printf '%s\n' "Please try another bucket name."
  done
  printf '%s\n\n' "OK. ${SOLUTION_NAME} will monitor the bucket [${GCS_BUCKET}]."
}

#######################################
# Confirm the monitored folder.
# Globals:
#   CONFIG_FOLDER_NAME
# Arguments:
#   None
#######################################
confirm_folder() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Confirming ${CONFIG_FOLDER_NAME} folder..."
  local loaded_value="${!CONFIG_FOLDER_NAME}"
  local default_value
  default_value=$(printf '%s' "${CONFIG_FOLDER_NAME}" | \
tr '[:upper:]' '[:lower:]')

  local folder=${loaded_value:-${default_value}}
  cat <<EOF
  Cloud Storage events are bound to a bucket. To prevent a Cloud Function from \
occupying a bucket exclusively, you must provide a folder name. This solution \
only takes the files under that folder. After it takes the files, Cloud \
Functions moves the files to the folder 'processed/'.
EOF
  printf '%s' "Enter the ${CONFIG_FOLDER_NAME} folder name [${folder}]: "
  local input
  read -r input
  folder=${input:-"${folder}"}
  if [[ ! ${folder} =~ ^.*/$ ]]; then
    folder="${folder}/"
  fi
  declare -g "${CONFIG_FOLDER_NAME}=${folder}"
  printf '%s\n\n' "OK. Continue with monitored folder [${folder}]."
}

#######################################
# Save the configuration to a local file.
# Globals:
#   GCP_PROJECT
#   CONFIG_ITEMS
#   CONFIG_FILE
# Arguments:
#   None
#######################################
save_config() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Saving the configuration..."

  local config_items_str="  \"PROJECT_ID\": \"${GCP_PROJECT}\""
  local i
  for i in "${!CONFIG_ITEMS[@]}"; do
    local key=${CONFIG_ITEMS[$i]}
    config_items_str="${config_items_str},"$'\n'"  \"${key}\": \"${!key}\""
  done
  local json_str=$'{\n'"${config_items_str}"$'\n}'$'\n'
  printf '%s\n%s' "  Will save" "${json_str}"
  printf '%s' "  to '${CONFIG_FILE}'. Confirm? [Y/n]: "
  local input
  read -r input
  if [[ -z ${input} || ${input} = 'y' || ${input} = 'Y' ]];then
    printf '%s' "${json_str}" > "${CONFIG_FILE}"
    printf '%s\n\n' "OK. Saved to ${CONFIG_FILE}."
    return 0
  else
    printf '%s\n' "User cancelled.";
    return 1
  fi
}

#######################################
# Based on the authentication method, guide user complete authentication
# process. For service account, confirm to use/create a service account and
# download key file; for OAuth 2.0, guide user to complete OAuth authentication
# and save the refresh token.
# Globals:
#   NEED_SERVICE_ACCOUNT
#   NEED_OAUTH
# Arguments:
#   None
#######################################
do_authentication(){
  if [[ ${NEED_SERVICE_ACCOUNT} = 'true' ]]; then
    download_service_account_key
  fi
  if [[ ${NEED_OAUTH} = 'true' ]]; then
    do_oauth
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
  printf '%s\n' "Step ${STEP}: Downloading the key file for the service \
account..."
  if [[ -z ${SA_NAME} ]];then
    confirm_service_account
  fi
  local suffix exist
  suffix=$(get_sa_domain_from_gcp_id "${GCP_PROJECT}")
  local email="${SA_NAME}@${suffix}"
  local prompt="Would you like to download the key file for [${email}] and \
save it as ${SA_KEY_FILE}? [Y/n]: "
  local default_value="y"
  if [[ -f "${SA_KEY_FILE}" && -s "${SA_KEY_FILE}" ]]; then
    exist=$(get_value_from_json_file ${SA_KEY_FILE} 'client_email' 2>&1)
    if [[ ${exist} =~ .*("@${suffix}") ]]; then
      prompt="A key file for [${exist}] with the key ID '\
$(get_value_from_json_file ${SA_KEY_FILE} 'private_key_id') already exists'. \
Would you like to create a new key to overwrite it? [N/y]: "
      default_value="n"
    fi
  fi
  printf '%s' "${prompt}"
  local input
  read -r input
  input=${input:-"${default_value}"}
  if [[ ${input} = 'y' || ${input} = 'Y' ]];then
    printf '%s\n' "Downloading a new key file for [${email}]..."
    gcloud iam service-accounts keys create "${SA_KEY_FILE}" --iam-account \
"${email}"
    if [[ $? -gt 0 ]]; then
      printf '%s\n' "Failed to download new key files for [${email}]."
      return 1
    else
      printf '%s\n' "OK. New key file is saved at [${SA_KEY_FILE}]."
      return 0
    fi
  else
    printf '%s\n' "Skipped downloading new key file. See \
https://cloud.google.com/iam/docs/creating-managing-service-account-keys \
to learn more about service account key files."
    return 0
  fi
}

#######################################
# Make sure a service account for this integration exists and set the email of
# the service account to the global variable `SA_NAME`.
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
  Some external APIs might require authentication based on OAuth or \
JWT(service account), for example, Google Analytics or Campaign Manager. \
In this step, you prepare the service account. For more information, see \
https://cloud.google.com/iam/docs/creating-managing-service-accounts
EOF

  local suffix
  suffix=$(get_sa_domain_from_gcp_id "${GCP_PROJECT}")
  local email
  if [[ -f "${SA_KEY_FILE}" && -s "${SA_KEY_FILE}" ]]; then
    email=$(get_value_from_json_file "${SA_KEY_FILE}" 'client_email')
    if [[ ${email} =~ .*("@${suffix}") ]]; then
      printf '%s' "A key file for service account [${email}] already exists. \
Would you like to create a new service account? [N/y]: "
      local input
      read -r input
      if [[ ${input} != 'y' && ${input} != 'Y' ]]; then
        printf '%s\n' "OK. Will use existing service account [${email}]."
        SA_NAME=$(printf "${email}" | cut -d@ -f1)
        return 0
      fi
    fi
  fi

  SA_NAME="${SA_NAME:-"${PROJECT_NAMESPACE}-api"}"
  while :; do
    printf '%s' "Enter the name of service account [${SA_NAME}]: "
    local input sa_elements=() sa
    read -r input
    input=${input:-"${SA_NAME}"}
    IFS='@' read -a sa_elements <<< "${input}"
    if [[ ${#sa_elements[@]} = 1 ]]; then
      echo "  Append default suffix to service account name and get: ${email}"
      sa="${input}"
      email="${sa}@${suffix}"
    else
      if [[ ${sa_elements[1]} != "${suffix}" ]]; then
        printf '%s\n' "  Error: Service account domain name ${sa_elements[1]} \
doesn't belong to the current project. The service account domain name for the \
current project should be: ${suffix}."
        continue
      fi
      sa="${sa_elements[0]}"
      email="${input}"
    fi

    printf '%s\n' "Checking the existence of the service account [${email}]..."
    if ! result=$(gcloud iam service-accounts describe "${email}" 2>&1); then
      printf '%s\n' "  Service account [${email}] does not exist. Trying to \
create..."
      gcloud iam service-accounts create "${sa}" --display-name \
"Tentacles API requester"
      if [[ $? -gt 0 ]]; then
        printf '%s\n' "Creating the service account [${email}] failed. Please \
try again..."
      else
        printf '%s\n' "The service account [${email}] was successfully created."
        SA_NAME=${sa}
        break
      fi
    else
      printf ' found.\n'
      SA_NAME=${sa}
      break
    fi
  done
  printf '%s\n' "OK. Service account [${SA_NAME}] is ready."
}

#######################################
# Guide an OAuth process and save the token to file.
# The whole process will be:
# 1. Check whether the OAuth scopes is ready, if not, go to confirm the oauth \
# scopes.
# 2. Prompt user to enter the OAuth Client ID.
# 3. Prompt user to enter the OAuth Client secret.
# 4. Print the OAuth authentication URL which users should open in a browser \
# and complete the process.
# 5. Copy the authentication code from the browser and paste here.
# 6. Use the authentication code to redeem an OAuth token and save it.
# Globals:
#   ENABLED_OAUTH_SCOPES
#   OAUTH2_TOKEN_JSON
# Arguments:
#   None
#######################################
do_oauth(){
  # Base url of Google OAuth service.
  OAUTH_BASE_URL="https://accounts.google.com/o/oauth2/"
  # Defaulat parameters of OAuth requests.
  OAUTH_PARAMETERS=(
    "access_type=offline"
    "redirect_uri=urn:ietf:wg:oauth:2.0:oob"
    "response_type=code"
  )
  while [ ${#ENABLED_OAUTH_SCOPES[@]} -eq 0 ]; do
    confirm_apis
  done
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Starting OAuth process..."
  local client_id client_secret scope default_value
  if [[ -f "${OAUTH2_TOKEN_JSON}" && -s "${OAUTH2_TOKEN_JSON}" ]]; then
    client_id=$(get_value_from_json_file ${OAUTH2_TOKEN_JSON} 'client_id' 2>&1)
    client_secret=$(get_value_from_json_file ${OAUTH2_TOKEN_JSON} 'client_secret' 2>&1)
    scopes=($(get_value_from_json_file ${OAUTH2_TOKEN_JSON} 'token.scope' 2>&1))
    printf '%s\n' "An OAuth token file for client_id[${client_id}] exists, \
with scopes:"
    for scope in "${!scopes[@]}"; do
      printf '%s\n' "  ${scopes[${scope}]}"
    done
    printf '%s' "Would you like to create a new token? [N/y]: "
    default_value="n"
    local input
    read -r input
    input=${input:-"${default_value}"}
    if [[ ${input} = 'n' || ${input} = 'N' ]];then
      printf '%s\n' "OK. Confirmed to use current OAuth token."
      return 0
    fi
  fi
  cat <<EOF

A native application OAuth 2.0 Client ID is required here, for example, OAuth \
Client ID of a "Desktop app". Do not use Web application.
Click the following link to create a OAuth client ID: \
https://console.developers.google.com/apis/credentials/oauthclient

For more information, see: \
https://developers.google.com/identity/protocols/oauth2/native-app#creatingcred

EOF

  local client_id
  local client_secret
  local auth_code
  local auth_response
  local auth_error
  local input
  while :; do
    printf '%s' "1. Enter the OAuth client ID[${client_id}]: "
    read -r input
    client_id=${input:-"${client_id}"}
    if [[ -z ${client_id} ]]; then
      continue
    fi
    printf '%s' "2. Enter the OAuth client secret[${client_secret}]: "
    read -r input
    client_secret=${input:-"${client_secret}"}
    if [[ -z ${client_secret} ]]; then
      continue
    fi
    local scope
    scope=$(join_string_array "%20" "${ENABLED_OAUTH_SCOPES[@]}")
    local parameters
    parameters=$(join_string_array "&" "${OAUTH_PARAMETERS[@]}" \
"client_id=${client_id}" "scope=${scope}")
    local auth_url
    auth_url="${OAUTH_BASE_URL}auth?${parameters}"
    printf '%s\n' "3. Open the link in browser and finish authentication: \
${auth_url}"
    cat <<EOF
  Note: if the OAuth client is not for a native application, there will be an \
"Error 400: redirect_uri_mismatch" shown up on the page. In this case, press \
"Enter" to start again with a native application OAuth client ID.
EOF
    printf '%s' "4. Copy the authentication code from browser and paste here: "
    read -r auth_code
    if [[ -z ${auth_code} ]]; then
      printf '%s\n\n' "No authentication code. Starting from beginning again..."
      continue
    fi
    auth_response=$(curl -s -d "code=${auth_code}" -d "client_id=${client_id}" \
-d "grant_type=authorization_code" -d "redirect_uri=urn:ietf:wg:oauth:2.0:oob" \
-d "client_secret=${client_secret}" "${OAUTH_BASE_URL}token")
    auth_error=$(node -e "console.log(!!JSON.parse(process.argv[1]).error)" \
"${auth_response}")
    if [[ ${auth_error} = "true" ]]; then
      printf '%s\n' "Error happened in redeem the authentication code: \
${auth_response}"
      continue
    fi
    printf '\n%s\n' "OAuth token was fetched successfully. Saving to file..."
    mkdir -p "${OAUTH2_TOKEN_JSON%/*}"
    cat > ${OAUTH2_TOKEN_JSON} <<EOF
{
  "client_id": "${client_id}",
  "client_secret": "${client_secret}",
  "token": ${auth_response}
}
EOF
    printf '\n%s\n' "OK. OAuth token has been saved."
    break
  done
}

#######################################
# Set default configuration for a Cloud Functions.
# Globals:
#   REGION
#   CF_RUNTIME
#   PROJECT_NAMESPACE
# Arguments:
#   Array of flags to deploy the Cloud Functions.
#######################################
set_cloud_functions_default_settings() {
  ensure_region
  local -n default_cf_flag=$1
  default_cf_flag+=(--region="${REGION}")
  default_cf_flag+=(--no-allow-unauthenticated)
  default_cf_flag+=(--timeout=540 --memory=2048MB --runtime="${CF_RUNTIME}")
  default_cf_flag+=(--set-env-vars=GCP_PROJECT="${GCP_PROJECT}")
  default_cf_flag+=(--set-env-vars=PROJECT_NAMESPACE="${PROJECT_NAMESPACE}")
  default_cf_flag+=(--set-env-vars=DEBUG="${DEBUG}")
  default_cf_flag+=(--set-env-vars=IN_GCP="${IN_GCP}")
}

#######################################
# Set default authentication environment variables for a Cloud Functions.
# Globals:
#   SA_KEY_FILE
#   OAUTH2_TOKEN_JSON
# Arguments:
#   Array of flags to deploy the Cloud Functions.
#######################################
set_authentication_env_for_cloud_functions() {
  local -n default_cf_flag=$1
  if [[ -f "${SA_KEY_FILE}" ]]; then
    cf_flag+=(--set-env-vars=API_SERVICE_ACCOUNT="${SA_KEY_FILE}")
  fi
  if [[ -f "${OAUTH2_TOKEN_JSON}" ]]; then
    cf_flag+=(--set-env-vars=OAUTH2_TOKEN_JSON="${OAUTH2_TOKEN_JSON}")
  fi
}

#######################################
# Create or update a Cloud Schdduleer
# Globals:
#   None
# Arguments:
#   Scheduler name
#   Frequency
#   Time Zone
#   Topic
#   Data
#   Attributes
#######################################
create_or_update_cloud_scheduler_for_pubsub(){
  check_authentication
  quit_if_failed $?
  local scheduler_flag=()
  scheduler_flag+=(--schedule="$2")
  scheduler_flag+=(--time-zone="$3")
  scheduler_flag+=(--topic="$4")
  scheduler_flag+=(--message-body="$5")
  local exist_job
  exist_job=($(gcloud scheduler jobs list --filter="name~${1}" \
--format="csv[no-heading](name,REGION)"))
  local action
  if [[ ${#exist_job[@]} -gt 0 ]]; then
    action="update"
    scheduler_flag+=(--update-attributes=$6)
  else
    action="create"
    scheduler_flag+=(--attributes=$6)
  fi
  gcloud scheduler jobs ${action} pubsub $1 "${scheduler_flag[@]}"
}

#######################################
# Print information before installation.
# Globals:
#   None
# Arguments:
#   Solution name
#######################################
print_welcome() {
  printf '\n'
  printf '%s\n' "###########################################################"
  printf '%s\n' "##                                                       ##"
  printf '%s%-20s%s\n' "##            Start installation of " "$1" " ##"
  printf '%s\n' "##                                                       ##"
  printf '%s\n' "###########################################################"
  printf '\n'
}

#######################################
# Print information after installation.
# Globals:
#   None
# Arguments:
#   Solution name
#######################################
print_finished(){
  printf '\n'
  printf '%s\n' "###########################################################"
  printf '%s%19s%s\n' "##" "$1" " has been installed.                ##"
  printf '%s\n' "###########################################################"
  printf '\n'
}

#######################################
# Print the email address of the service account.
# Globals:
#   SA_KEY_FILE
# Arguments:
#   None
#######################################
print_service_account(){
  printf '%s\n' "=========================="
  local email
  email=$(get_value_from_json_file "${SA_KEY_FILE}" 'client_email')
  printf '%s\n' "The email address of the current service account is ${email}."
}

#######################################
# Default function to start the automatic installation process.
# Globals:
#   DEFAULT_INSTALL_TASKS
# Arguments:
#   None
#######################################
default_install() {
  local task
  for task in "${DEFAULT_INSTALL_TASKS[@]}"; do
    ${task}
    quit_if_failed $?
  done
}

#######################################
# Run the default Function if there are no parameters; otherwise, run the
# specific function.
# Globals:
#   MAIN_FUNCTION
# Arguments:
#   None
#######################################
run_default_function() {
  if [[ -z "$1" ]]; then
    "${MAIN_FUNCTION}"
  else
    printf '%s\n\n' "Running the single task [$*], setting variables..."
    emulate_cloud_shell
    load_config
    "$@"
  fi
}

# Utilities functions

#######################################
# Load a value from a given JSON file.
# Globals:
#   None
# Arguments:
#   JSON file path, 'dot' is the delimiter of embedded property names.
#   Property name.
# Returns:
#   The value.
#######################################
get_value_from_json_file() {
  if [[ -s $1 ]];then
    local script
    read -d '' script << EOF
const properties = process.argv[2].split('.');
const result = properties.reduce((previous, currentProperty) => {
  return previous[currentProperty];
}, require(process.argv[1]));
let output;
if (typeof result !== 'object'){
  output = result;
} else{
  output = JSON.stringify(result);
}
console.log(output);
EOF
    node -e "${script}" "$@"
  fi
}

#######################################
# Generate a default Cloud Storage name based on the given string. If there is
# a colon in the string, use only the part after the colon.
# Globals:
#   PROJECT_NAMESPACE
# Arguments:
#   Any string.
# Returns:
#   The default bucket name.
#######################################
get_default_bucket_name() {
  printf '%s' "${PROJECT_NAMESPACE}-$(printf '%s' "$1" | cut -d : -f 2)"
}

#######################################
# Generate a domain name for the email of the current project's service account.
# Globals:
#   None
# Arguments:
#   Project ID
# Returns:
#   The domain name for the email of current project's service account.
#######################################
get_sa_domain_from_gcp_id() {
  if [[ $1 =~ .*(:).* ]]; then
    printf '%s' "$(printf '%s' "$1" | \
sed  -r 's/^([^:]*):(.*)$/\2.\1/').iam.gserviceaccount.com"
  else
    printf '%s' "$1.iam.gserviceaccount.com"
  fi
}

#######################################
# Returns the default service account of the given Cloud Functions.
# Use cases:
# 1. When BigQuery query Google Sheet based external tables, this service
#    account needs to be added to the Google Sheet as a viewer.
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   The default service account of the given Cloud Functions.
#######################################
get_cloud_functions_service_account() {
  local region=$(gcloud functions list --format="csv[no-heading](name,REGION)" \
| grep "${1}" | cut -d, -f2 | uniq)
  if [[ -z ${region} ]]; then
    printf '%s\n' "Cloud Functions [$1] doesn't exist."
  else
    local service_account=$(gcloud functions describe "$1" \
--region="${region}" --format="get(serviceAccountEmail)")
    printf '%s' "$service_account"
  fi
}

#######################################
# Make sure Firestore or Datastore is in the current project.
# To create the Firestore, the operator need to have following permissions:
# appengine.applications.create - role: Owner
# datastore.locations.list - sample role: Cloud Datastore Owner
# servicemanagement.services.bind - sample role: Editor
# Globals:
#   GCP_PROJECT
# Arguments:
#   None.
#######################################
check_firestore_existence() {
  local firestore_status
  firestore_status=$(gcloud firestore operations list 2>&1)
  while [[ ${firestore_status} =~ .*NOT_FOUND.* ]]; do
    cat <<EOF
Cannot find Firestore or Datastore in current project. Please visit \
https://console.cloud.google.com/firestore?project=${GCP_PROJECT} to create a \
database before continue.

Press any key to continue after you create the database...
EOF
    local any
    read -n1 -s any
    printf '\n'
    firestore_status=$(gcloud firestore operations list 2>&1)
  done
}

#######################################
# Check the last execution status. Quit the current process if there's an error.
# Globals:
#   None
# Arguments:
#   Last execution's exist status.
#######################################
quit_if_failed() {
  printf '\n'
  if [[ $1 -gt 0 ]];then
    printf '%s\n' "[Error] Quit."
    exit 1
  fi
}

#######################################
# Join the array to a string with the given separator string.
# Globals:
#   None
# Arguments:
#   Separator.
#   Array of string.
#######################################
join_string_array() {
  local separator=$1;
  shift
  local first=$1;
  shift
  printf %s "$first" "${@/#/$separator}"
}
