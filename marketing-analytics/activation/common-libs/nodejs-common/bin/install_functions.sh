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
CF_RUNTIME="${CF_RUNTIME:=nodejs14}"
CF_MEMORY="${CF_MEMORY:=2048MB}"

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

# Bucket Locations. https://cloud.google.com/storage/docs/locations
# The list has the same locations as Dataset locations except grouping way.
# https://cloud.google.com/bigquery/docs/locations
# So put it here for reuse in selection location for bucket and dataset.
NORTH_AMERICA=(
  "Montréal (northamerica-northeast1)"
  "Toronto (northamerica-northeast2)"
  "Iowa (us-central1)"
  "South Carolina (us-east1)"
  "Northern Virginia (us-east4)"
  "Oregon (us-west1)"
  "Los Angeles (us-west2)"
  "Salt Lake City (us-west3)"
  "Las Vegas (us-west4)"
)
SOUTH_AMERICA=(
  "São Paulo (southamerica-east1)"
)
EUROPE=(
  "Warsaw (europe-central2)"
  "Finland (europe-north1)"
  "Belgium (europe-west1)"
  "London (europe-west2)"
  "Frankfurt (europe-west3)"
  "Netherlands (europe-west4)"
  "Zürich (europe-west6)"
)
ASIA=(
  "Taiwan (asia-east1)"
  "Hong Kong (asia-east2)"
  "Tokyo (asia-northeast1)"
  "Osaka (asia-northeast2)"
  "Seoul (asia-northeast3)"
  "Mumbai (asia-south1)"
  "Delhi (asia-south2)"
  "Singapore (asia-southeast1)"
  "Jakarta (asia-southeast2)"
)
AUSTRALIA=(
  "Sydney (australia-southeast1)"
  "Melbourne (australia-southeast2)"
)

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
  "youtube.googleapis.com"
)

# Some APIs only support OAuth which needs an process to generate refresh token.
# If none of these APIs are enabled, then the use can choose to use JWT auth.
declare EXTERNAL_APIS_OAUTH_ONLY=(
  "googleads.googleapis.com"
  "doubleclickbidmanager.googleapis.com"
  "youtube.googleapis.com"
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
  ["youtube.googleapis.com"]=\
"https://www.googleapis.com/auth/youtube.force-ssl"
)

# Enabled APIs' OAuth scopes.
ENABLED_OAUTH_SCOPES=()

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
      if [[ -z ${current_project} || \
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
# Confirm the timezone that this instance will use. Usually this will be used
# to create the Cloud Schedule jobs.
# Globals:
#   TIMEZONE
# Arguments:
#   None
#######################################
confirm_timezone() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Confirming the timezone..."
  local timezoneVariableName defaultValue
  timezoneVariableName="${1:-"TIMEZONE"}"
  defaultValue="${!timezoneVariableName}"
  while :; do
    printf '%s' "Enter the project timezone [${defaultValue}]: "
    read -r timezone
    timezone=${timezone:-$defaultValue}
    local result=$(node -e "try{
      Intl.DateTimeFormat(undefined, {timeZone: '${timezone}'});
      console.log(0);
    } catch(e){
       console.log(1);
    }")
    if [[ ${result} -eq 0 ]]; then
      declare -g "${timezoneVariableName}=${timezone}"
      break
    else
      printf '%s\n' "[${timezone}] is not a valid timezone. Check the column \
'TZ database name' in the List of \
https://en.wikipedia.org/wiki/List_of_tz_database_time_zones for a valid one."
    fi
  done
  printf '%s\n' "OK. This solution will use the timezone [${timezone}]."
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
solution installed [${GCP_PROJECT}]: "
    local input result
    read -r input
    input=${input:-"${GCP_PROJECT}"}
    printf '%s' "Checking billing status for [${input}]..."
    result=$(gcloud beta billing projects describe "${input}" \
--format="csv[no-heading](billingEnabled)")
    if [[ "${result}" != "True"  && "${result}" != "true"  ]]; then
      printf '%s\n' " there is no billing account."
      return 1
    else
      printf '%s\n' "succeeded."
    fi
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
# Confirm and set env variable for the region for Cloud Functions, Cloud Storage
# or BigQuery.
# Globals:
#   SOLUTION_NAME
#   REGION
# Arguments:
#   Region variable name, default value 'REGION'
#######################################
confirm_region() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Select the region to use for deploying \
${SOLUTION_NAME}."
  cat <<EOF
This solution uses Cloud Functions, Cloud Storage or BigQuery. Based on your \
requirements, select the region to use for deployment. Because of latency and \
pricing considerations, we recommend you use the same region for Cloud \
Functions and Cloud Storage. See \
https://cloud.google.com/functions/docs/locations/ for more information.
EOF
  select_functions_location $*
  printf '%s\n' "OK. ${REGION} will be used for deployment."
}

#######################################
# Set the env variable (default 'REGION') as the deploy target for Cloud
# Functions. It could be used for Cloud Storage or BigQuery as well.
# Globals:
#   PROJECT_NAMESPACE
#   REGION
# Arguments:
#   Region variable name, default value 'REGION'
#######################################
select_functions_location() {
  local regionVariableName defaultValue locations
  regionVariableName="${1:-"REGION"}"
  defaultValue="${!regionVariableName}"
  locations=($(gcloud functions regions list --format="csv[no-heading](name)"| \
    sed 's/projects\/.*\/locations\///'))
  local region
  while :; do
    local exist_functions
    exist_functions=($(gcloud functions list --filter=\
"name~${PROJECT_NAMESPACE}" --format="csv[no-heading](name,REGION)"))
    if [[ ${#exist_functions[@]} -gt 0 ]]; then
      local exist_region
      exist_region=$(printf "${exist_functions[0]}" | cut -d\, -f2 | uniq)
      printf '%s\n' "Current application has already been installed in region: \
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
      if [[ ${continue} == "Y" || ${continue} == "y" ]]; then
        region=${exist_region}
        break
      else
        printf '%s' "Deploying to other regions deletes existing Cloud \
Functions for that region. Do you want to continue? [N/y]: "
        local confirm_delete
        read -r confirm_delete
        if [[ ${confirm_delete} == "Y" || ${confirm_delete} == "y" ]]; then
          for i in "${!exist_functions[@]}"; do
            local exist_function
            exist_function=$(printf "${exist_functions[$i]}" | cut -d\, -f1)
            local function_region
            function_region=$(printf "${exist_functions[$i]}" | cut -d\, -f2)
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
  declare -g "${regionVariableName}=${region}"
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

  if [[ ${#input[@]} == 0 || ${input[0]} == '*' ]]; then
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
# Arguments:
#   None
#######################################
confirm_auth_method() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Checking authentication method for selected \
API(s):"
  if [[ ${NEED_AUTHENTICATION} != 'true' ]]; then
    printf '%s\n'  "OK. No authentication is required."
    return 0
  fi
  if [[ ${NEED_OAUTH} == 'true' ]]; then
    printf '%s\n'  "OK. OAuth is required by selected API(s)."
    return 0
  fi
  printf '%s\n' "Selected API(s) require authentication. Choose the \
authentication method:"
  local auths=("Service Account (recommended)" "OAuth")
  select auth in "${auths[@]}"; do
    if [[ " ${auths[@]} " =~ " ${auth} " ]]; then
      break
    fi
  done
  if [[ ${auth} == 'OAuth' ]]; then
    NEED_OAUTH="true"
    printf '%s\n' "... OAuth is selected."
  else
    NEED_SERVICE_ACCOUNT="true"
    printf '%s\n' "... Service Account is selected."
  fi
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
  while :; do
    error=0
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
      printf '%s\n' "Would you like to login with another account? [Y/n]: "
      local reLogin
      read -r reLogin
      reLogin=${reLogin:-"Y"}
      if [[ ${reLogin} == "Y" || ${reLogin} == "y" ]]; then
        gcloud auth login
        continue
      else
        return 1
      fi
    else
      echo "OK. Permissions check passed for Google Cloud project \
  [${GCP_PROJECT}]."
      return 0
    fi
  done
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
# Set the env variable as the location for BigQuery.
# Globals:
#   None
# Arguments:
#   Region variable name.
#######################################
select_dataset_location() {
  # Dataset locations definition.
  # See https://cloud.google.com/bigquery/docs/locations
  local DATASET_REGIONS=(
    "Multi-regional"
    "Americas"
    "Europe"
    "Asia Pacific"
  )
  # Region location list.
  local DATASET_REGIONS_PARAMETER=(
    "MULTI_REGIONAL"
    "AMERICAS"
    "EUROPE"
    "ASIA_PACIFIC"
  )
  local MULTI_REGIONAL=(
    "Data centers within member states of the European Union (eu)"
    "Data centers in the United States (us)"
  )
  local AMERICAS=(
    "${NORTH_AMERICA[@]}"
    "${SOUTH_AMERICA[@]}"
  )
  local ASIA_PACIFIC=(
    "${ASIA[@]}"
    "${AUSTRALIA[@]}"
  )
  if [[ -z "${1}" ]]; then
    printf '%s\n' "Error. Need a variable name for dataset location."
    return 1
  fi
  local regionVariableName
  regionVariableName="${1}"
  printf '%s\n' "Select the region of the dataset location:"
  local location locationType
  select location in "${DATASET_REGIONS[@]}"; do
    if [[ -n "${location}" ]]; then
      if [[ "${REPLY}" == 1 ]]; then
        locationType="MULTI_REGIONAL"
      else
        locationType="REGIONAL"
      fi
      cat <<EOF
Selected location[${location}]'s type is ${locationType}. See \
https://cloud.google.com/bigquery/docs/locations#locations_or_region_types for \
more information about the locations or region types.

Continue to select the location for the BigQuery dataset. See
https://cloud.google.com/bigquery/docs/locations#regional-locations for more
information about locations. Enter your selection, or enter 0 to return to \
location type selection:
EOF
      declare -n options="${DATASET_REGIONS_PARAMETER["$((REPLY-1))"]}"
      local selectedLocation
      select selectedLocation in "${options[@]}"; do
        if [[ -n "${selectedLocation}" ]]; then
          local location
          location=$(echo "${selectedLocation}" | cut -d\( -f2 | cut -d\) -f1)
          echo "Select region: ${location}"
          break 2
        elif [[ "${REPLY}" == 0 ]]; then
          break
        else
          printf '%s\n' "Select the location or enter 0 to return to \
location type selection:"
        fi
      done
    fi
    printf '%s\n' "Select the BigQuery Dataset region or press Enter to
refresh the list:"
  done
  declare -g "${regionVariableName}=${location}"
}

#######################################
# Confirm or create a BigQuery dataset with a given location variable.
# If the location variable has value, then it will create the dataset with this
# location; otherwise it will let the user to select the location and set to
# the variable, then continue to create the dataset.
# Globals:
#   DATASET
#   REGION
# Arguments:
#   Dataset variable name, default value 'DATASET'
#   Location variable name, default value 'REGION'
#   If the second location var is unset, use this var as default value
#######################################
confirm_located_dataset(){
  local datasetName defaultValue locationName location
  datasetName="${1:-"DATASET"}"
  locationName="${2:-"REGION"}"
  defaultValue="${!datasetName}"
  # If the location variable has no value, use 'select_x_location' to select it.
  location=$(printf "${!locationName}" | tr [:upper:] [:lower:])
  if [[ -z "${location}" && -n "${3}" ]]; then
    local defaultValueFrom
    defaultValueFrom="${3}"
    location="${!defaultValueFrom}"
  fi

  (( STEP += 1 ))
  if [[ -z "${location}" ]]; then
    printf '%s\n' "Step ${STEP}: Checking or creating a BigQuery dataset..."
    if [[ "${locationName}" == "REGION" ]]; then
      select_functions_location ${locationName}
    else
      select_dataset_location ${locationName}
    fi
    location="${!locationName}"
  else
    printf '%s\n' "Step ${STEP}: Checking or creating a BigQuery dataset in \
location ["${location}"] ..."
  fi
  declare -g "${locationName}=${location}"
  while :; do
    printf '%s' "Enter the name of your dataset [${defaultValue}]: "
    local dataset
    read -r dataset
    dataset="${dataset:-$defaultValue}"
    if [[ -z "${dataset}" ]]; then
      continue
    fi
    local datasetMetadata
    datasetMetadata="$(bq --format json --dataset_id "${dataset}" show 2>&1)"
    # Dataset exists.
    if [[ "${datasetMetadata}" != *"BigQuery error in show operation"* ]]; then
      local currentLocation
      currentLocation="$(get_value_from_json_string "${datasetMetadata}" \
        "location" | tr [:upper:] [:lower:])"
      if [[ "${currentLocation}" == "${location}" ]]; then
        printf '%s\n' "OK. The dataset [${dataset}] exists in the location \
[${location}] ."
        break
      fi
      printf '%s' "  The dataset [${dataset}] exists in the location \
[${currentLocation}] . "
      # If the location var is 'REGION', then no other location is allowed.
      if [[ "${locationName}" == "REGION" ]]; then
        printf '%s\n' "Continuing to enter another dataset..."
        continue
      fi
      printf '%s' "Do you want to continue with it? [N/y]: "
      local confirmContinue
      read -r confirmContinue
      if [[ ${confirmContinue} == "Y" || ${confirmContinue} == "y" ]]; then
        # Update the env var if confirmed.
        declare -g "${locationName}=${currentLocation}"
        break
      else
        printf '%s\n' "Continuing to enter another dataset..."
        continue
      fi
    else # Dataset doesn't exist.
      bq --location="${location}" mk "${dataset}"
      if [[ $? -gt 0 ]]; then
        printf '%s\n' "Failed to create the dataset [${dataset}]. Try again."
        continue
      else
        printf '%s\n' "OK. The dataset [${dataset}] has been created in the \
location [${location}] ."
        break
      fi
    fi
  done
  declare -g "${datasetName}=${dataset}"
}

#######################################
# Deprecated. Use 'confirm_located_dataset' instead.
# Kept here for compatibility.
# Confirm or create a BigQuery dataset with a given location.
# Globals:
#   DATASET
# Arguments:
#   Dataset env name
#   Location value
#######################################
confirm_dataset_with_location() {
  local datasetName locationName
  datasetName="${1:-"DATASET"}"
  locationName="${datasetName}_LOCATION"
  declare -g "${locationName}=${2}"
  confirm_located_dataset ${datasetName} ${locationName}
}

#######################################
# Get the metadata of the given Storage bucket. It will return one of the
# following information in a JSON string:
# 1. error.code 404 Not existent
# 2. error.code 403 No access for current user.
# 3. metadata, including 'projectNumber' and 'location',
# Globals:
#   None
# Arguments:
#   Bucket name
# Returns:
#   Bucket location
#######################################
get_bucket_metadata() {
  local accessToken request bucketMetadata
  accessToken=$(gcloud auth print-access-token)
  request=(
    -s
    "https://storage.googleapis.com/storage/v1/b/$1"
    -H "Accept: application/json"
    -H "Authorization: Bearer ${accessToken}"
  )
  bucketMetadata=$(curl "${request[@]}")
  printf '%s' "${bucketMetadata}"
}

#######################################
# Set the env variable as the location for Cloud Storage bucket.
# Globals:
#   None
# Arguments:
#   Region variable name.
#######################################
select_bucket_location() {
  # Storage regions.
  local STORAGE_REGIONS=(
    "Multi-regional"
    "Dual-regional"
    "North America"
    "South America"
    "Europe"
    "Asia"
    "Australia"
  )
  # Region location list.
  local STORAGE_REGIONS_PARAMETER=(
    "MULTI_REGIONAL"
    "DUAL_REGIONAL"
    "NORTH_AMERICA"
    "SOUTH_AMERICA"
    "EUROPE"
    "ASIA"
    "AUSTRALIA"
  )
  local MULTI_REGIONAL=(
    "Data centers in Asia (ASIA)"
    "Data centers within member states of the European Union (EU)"
    "Data centers in the United States (US)"
  )
  local DUAL_REGIONAL=(
    "ASIA-NORTHEAST1 and ASIA-NORTHEAST2 (ASIA1)"
    "EUROPE-NORTH1 and EUROPE-WEST4 (EUR4)"
    "US-CENTRAL1 and US-EAST1 (NAM4)"
  )
  if [[ -z "${1}" ]]; then
    printf '%s\n' "Error. Need a variable name for bucket location."
    return 1
  fi
  local regionVariableName
  regionVariableName="${1}"
  printf '%s\n' "Select the region for Cloud Storage:"
  local region class
  select region in "${STORAGE_REGIONS[@]}"; do
    if [[ -n "${region}" ]]; then
      if [[ "${REPLY}" == 1 ]]; then
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
      local selectedLocation
      select selectedLocation in "${options[@]}"; do
        if [[ -n "${selectedLocation}" ]]; then
          local location
          location=$(echo "${selectedLocation}" | cut -d\( -f2 | cut -d\) -f1)
          echo "Select region: ${location}"
          break 3
        elif [[ "${REPLY}" == 0 ]]; then
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
  declare -g "${regionVariableName}=${location}"
}

#######################################
# Confirm or create a Cloud Storage bucket with a given location.
# If the location variable has value, then it will create the bucket with this
# location; otherwise it will let the user to select the location and set to
# the variable, then continue to create the bucket.
# Globals:
#   GCP_PROJECT
# Arguments:
#   Bucket name var with optional usage, e.g. 'GCS_BUCKET_REPORT:reports'.
#     The default value is 'GCS_BUCKET'.
#   Location var name, default value 'REGION'
#   If the second location var is unset, use this var as default value
#######################################
confirm_located_bucket() {
  local gcsName usage defaultValue defaultBucketName locationName location
  if [[ -z "${1}" ]]; then
    gcsName="GCS_BUCKET"
  elif [[ "${1}" == *":"* ]]; then
    gcsName=$(echo "${1}" | cut -d\: -f1)
    usage=$(echo "${1}" | cut -d\: -f2)
    if [[ -n "${usage}" ]]; then
      usage=" for ${usage}"
    fi
  else
    gcsName="${1}"
  fi
  locationName="${2:-"REGION"}"
  defaultValue="${!gcsName}"
  defaultBucketName=$(get_default_bucket_name "${GCP_PROJECT}")
  defaultValue="${defaultValue:-${defaultBucketName}}"
  # If the location variable has no value, use 'set_region' to select it.
  location=$(printf "${!locationName}" | tr [:upper:] [:lower:])
  if [[ -z "${location}" && -n "${3}" ]]; then
    local defaultValueFrom
    defaultValueFrom="${3}"
    location="${!defaultValueFrom}"
  fi

  (( STEP += 1 ))
  if [[ -z "${location}" ]]; then
    printf '%s\n' "Step ${STEP}: Checking or creating a Cloud Storage \
Bucket${usage}..."
    if [[ "${locationName}" == "REGION" ]]; then
      select_functions_location ${locationName}
    else
      select_bucket_location ${locationName}
    fi
    location="${!locationName}"
  else
    printf '%s\n' "Step ${STEP}: Checking or creating a Cloud Storage \
Bucket${usage} in location [${location}] ..."
  fi
  declare -g "${locationName}=${location}"
  while :; do
    printf '%s' "Enter the name of your Cloud Storage bucket [${defaultValue}]: "
    local bucket
    read -r bucket
    bucket="${bucket:-$defaultValue}"
    if [[ -z "${bucket}" ]]; then
      continue
    fi
    local bucketMetadata projectNumber
    bucketMetadata="$(get_bucket_metadata "${bucket}")"
    projectNumber="$(get_value_from_json_string "${bucketMetadata}" "projectNumber")"
    # No project number means it doesn't exist or no access to it.
    if [[ -z "${projectNumber}" ]]; then
      local errorCode
      errorCode=$(get_value_from_json_string "${bucketMetadata}" "error.code")
      if [[ ${errorCode}  == '404' ]]; then
        gsutil mb -l "${location}" "gs://${bucket}/"
        if [[ $? -gt 0 ]]; then
          printf '%s\n' "Failed to create the bucket [${bucket}]. Try again."
          continue
        else
          printf '%s\n' "OK. The bucket [${bucket}] has been created in the \
location [${location}] ."
          break
        fi
      fi
      local errorMessage
      errorMessage="$(get_value_from_json_string "${bucketMetadata}" "error.message")"
      printf '%s\n' "  ${errorMessage} Continuing to enter another bucket..."
      continue
    else
      local currentLocation currentProjectNumber
      currentLocation="$(get_value_from_json_string "${bucketMetadata}" \
        "location" | tr [:upper:] [:lower:])"
      currentProjectNumber="$(get_project_number)"
      if [[ "${projectNumber}" != "${currentProjectNumber}" ]]; then
        printf '%s\n' "  The bucket [${bucket}] belongs to another Cloud \
Project. Continuing to enter another bucket..."
        continue
      fi
      if [[ "${currentLocation}" == "${location}" ]]; then
        printf '%s\n' "OK. The bucket [${bucket}] exists in the location \
[${location}] ."
        break
      fi
      printf '%s' "  The bucket [${bucket}] exists in the location \
[${currentLocation}] . "
      if [[ "${locationName}" == "REGION" ]]; then
        printf '%s\n' "Continuing to enter another bucket..."
        continue
      fi
      printf '%s' "Do you want to continue with it? [N/y]: "
      local confirmContinue
      read -r confirmContinue
      if [[ ${confirmContinue} == "Y" || ${confirmContinue} == "y" ]]; then
        # Update the env var if confirmed.
        declare -g "${locationName}=${currentLocation}"
        break
      else
        printf '%s\n' "Continuing to enter another bucket..."
        continue
      fi
    fi
  done
  declare -g "${gcsName}=${bucket}"
  confirm_bucket_lifecycle "${bucket}"
}

#######################################
# Deprecated. Use 'confirm_located_bucket' instead.
# Kept here for compatibility.
# Confirm or create a Cloud Storage bucket with a given location.
# Globals:
#   GCS_BUCKET
# Arguments:
#   Bucket env name
#   Location value
#######################################
confirm_bucket_with_location() {
  local gcsName locationName
  gcsName="${1:-"GCS_BUCKET"}"
  locationName="${gcsName}_LOCATION"
  declare -g "${locationName}=${2}"
  confirm_located_bucket ${gcsName} ${locationName}
}

#######################################
# Manage the lifecycle of a GCS bucket: setting or removing the GCS lifecycle
# rule of 'age'.
# See: https://cloud.google.com/storage/docs/lifecycle#age
# Arguments:
#   Bucket name
#######################################
confirm_bucket_lifecycle() {
  local bucket bucketMetadata lifecycle
  bucket="${1}"
  bucketMetadata="$(get_bucket_metadata "${bucket}")"
  lifecycle="$(get_value_from_json_string "${bucketMetadata}" "lifecycle")"
  if [[ -n "${lifecycle}" ]]; then
    printf '%s\n' "There are lifecycle rules in this bucket: ${lifecycle}."
    printf '%s' "Would you like to overwrite it? [N/y]:"
    local confirmContinue
    read -r confirmContinue
    confirmContinue=${confirmContinue:-"N"}
    if [[ ${confirmContinue} == "N" || ${confirmContinue} == "n" ]]; then
      return 0
    fi
  else
    printf '%s\n' "There is no lifecycle rules in this bucket."
    printf '%s' "Would you like to create it? [Y/n]:"
    local confirmContinue
    read -r confirmContinue
    confirmContinue=${confirmContinue:-"Y"}
    if [[ ${confirmContinue} == "N" || ${confirmContinue} == "n" ]]; then
      return 0
    fi
  fi
  while :; do
    printf '%s' "Enter the number of days that a file will be kept before it \
is automatically removed in the bucket[${bucket}]. (enter 0 to remove all \
existing lifecycle rules): "
    local days
    read -r days
    days=${days}
    if [[ "${days}" =~ ^[0-9]+$ ]]; then
      local lifecycle
      if [[ "${days}" == "0" ]]; then
        lifecycle="{}"
      else
        lifecycle='{"rule":[{"action":{"type":"Delete"},"condition":{"age":'\
"${days}}}]}"
      fi
      gsutil lifecycle set /dev/stdin gs://${bucket} <<< ${lifecycle}
      return $?
    fi
  done
}

#######################################
# Confirm the monitored folder.
# Globals:
#   CONFIG_FOLDER_NAME
# Arguments:
#   Folder name, default value ${CONFIG_FOLDER_NAME}
#######################################
confirm_folder() {
  (( STEP += 1 ))
  local folderName=${1-"${CONFIG_FOLDER_NAME}"}
  printf '%s\n' "Step ${STEP}: Confirming ${folderName} folder..."
  local loaded_value="${!folderName}"
  local default_value
  default_value=$(printf '%s' "${folderName}" | \
tr '[:upper:]' '[:lower:]')

  local folder=${loaded_value:-${default_value}}
  cat <<EOF
  Cloud Storage events are bound to a bucket. To prevent a Cloud Function from \
occupying a bucket exclusively, you must provide a folder name. This solution \
only takes the files under that folder. After it takes the files, Cloud \
Functions moves the files to the folder 'processed/'.
EOF
  printf '%s' "Enter the ${folderName} folder name [${folder}]: "
  local input
  read -r input
  folder=${input:-"${folder}"}
  if [[ ! ${folder} =~ ^.*/$ ]]; then
    folder="${folder}/"
  fi
  declare -g "${folderName}=${folder}"
  printf '%s\n' "OK. Continue with monitored folder [${folder}]."
}

#######################################
# Create or update a Log router sink.
# Globals:
#   None
# Arguments:
#   Name of the sink
#   Filter conditions
#   Sink destination
#######################################
create_or_update_sink() {
  local sinkName=${1}
  local logFilter=${2}
  local sinkDestAndFlags=(${3})
  local existingFilter
  existingFilter=$(gcloud logging sinks list --filter="name:${sinkName}" \
--format="value(filter)")
  local action
  if [[ -z "${existingFilter}" ]]; then
    action="create"
    printf '%s\n' "  Logging Export [${sinkName}] doesn't exist. Creating..."
  else
    action="update"
    printf '%s\n' "  Logging Export [${sinkName}] exists with a different \
filter. Updating..."
  fi
  gcloud -q logging sinks ${action} "${sinkName}" "${sinkDestAndFlags[@]}" \
--log-filter="${logFilter}"
  if [[ $? -gt 0 ]];then
    printf '%s\n' "Failed to create or update Logs router sink."
    return 1
  fi
}

#######################################
# Confirm the service account of the given sink has proper permission to dump
# the logs.
# Globals:
#   None
# Arguments:
#   Name of the sink
#   Bindings role, e.g. "pubsub.publisher" or "bigquery.dataEditor"
#   Role name, e.g. "Pub/Sub Publisher" or "BigQuery Data Editor"
#######################################
confirm_sink_service_account_permission() {
  local sinkName=${1}
  local bindingsRole=${2}
  local roleName=${3}
  local serviceAccount existingRole
  serviceAccount=$(gcloud logging sinks describe "${sinkName}" \
--format="get(writerIdentity)")
  while :;do
    printf '%s\n' "  Checking the role of the sink's service account \
[${serviceAccount}]..."
    existingRole=$(gcloud projects get-iam-policy "${GCP_PROJECT}" \
--flatten=bindings --filter="bindings.members:${serviceAccount} AND \
bindings.role:roles/${bindingsRole}" --format="get(bindings.members)")
    if [[ -z "${existingRole}" ]];then
      printf '%s\n'  "  Granting Role '${roleName}' to the service \
account..."
      gcloud -q projects add-iam-policy-binding "${GCP_PROJECT}" --member \
"${serviceAccount}" --role roles/${bindingsRole}
      if [[ $? -gt 0 ]];then
        printf '%s\n' "Failed to grant the role. Use this link \
https://console.cloud.google.com/iam-admin/iam?project=${GCP_PROJECT} to \
manually grant Role '${roleName}' to ${serviceAccount}."
        printf '%s' "Press any key to continue after you grant the access..."
        local any
        read -n1 -s any
        continue
      fi
    else
      printf '%s\n'  "  The role has already been granted."
      return 0
    fi
  done
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
  if [[ -z ${input} || ${input} == 'y' || ${input} == 'Y' ]];then
    printf '%s' "${json_str}" > "${CONFIG_FILE}"
    printf '%s\n' "OK. Saved to ${CONFIG_FILE}."
    return 0
  else
    printf '%s\n' "User cancelled.";
    return 1
  fi
}

#######################################
# Based on the authentication method, guide user complete authentication
# process.
# For service account, given that Cloud Functions can extend the authorized API
# scopes now, it will use the default service account rather than an explicit
# service account with the key file downloaded. By doing so, we can reduce the
# risk of leaking service account key. If there is a service key in the current
# folder from previous installation, the code will continue using it, otherwise
# it will use the Cloud Functions' default service account.
# For OAuth 2.0, guide user to complete OAuth authentication and save the
# refresh token.
# Globals:
#   NEED_SERVICE_ACCOUNT
#   NEED_OAUTH
# Arguments:
#   None
#######################################
do_authentication(){
  if [[ ${NEED_OAUTH} == "true" ]]; then
    do_oauth
  fi
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
# 5. Copy the authorization code from the browser and paste here.
# 6. Use the authorization code to redeem an OAuth token and save it.
# Globals:
#   ENABLED_OAUTH_SCOPES
#   OAUTH2_TOKEN_JSON
# Arguments:
#   None
#######################################
do_oauth(){
  # Base url of Google OAuth service.
  OAUTH_BASE_URL="https://accounts.google.com/o/oauth2/"
  # Redirect uri.
  # Must be the same for requests of authorization code and refresh token.
  REDIRECT_URI="http%3A//127.0.0.1%3A8887"
  # Defaulat parameters of OAuth requests.
  OAUTH_PARAMETERS=(
    "access_type=offline"
    "redirect_uri=${REDIRECT_URI}"
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
    if [[ ${input} == 'n' || ${input} == 'N' ]];then
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
    printf '%s\n' "3. Open the link in your browser and finish authentication. \
    Do not close the redirected page: ${auth_url}"
    cat <<EOF
  Note:
    The succeeded OAuth flow will land the browser on an error page - \
"This site can't be reached". This is expected behavior. Copy the whole URL and continue.
    If the OAuth client is not for a native application, there will be an \
"Error 400: redirect_uri_mismatch" shown up on the page. In this case, press \
"Enter" to start again with a native application OAuth client ID.

EOF
    printf '%s' "4. Copy the complete URL from your browser and paste here: "
    read -r auth_code
    if [[ -z ${auth_code} ]]; then
      printf '%s\n\n' "No authorization code. Starting from beginning again..."
      continue
    fi
    auth_code=$(printf "%s" "${auth_code}" | sed 's/^.*code=//;s/&.*//')
    printf '%s\n' "Got authorization code: ${auth_code}"
    auth_response=$(curl -s -d "code=${auth_code}" -d "client_id=${client_id}" \
-d "grant_type=authorization_code" -d "redirect_uri=${REDIRECT_URI}" \
-d "client_secret=${client_secret}" "${OAUTH_BASE_URL}token")
    auth_error=$(node -e "console.log(!!JSON.parse(process.argv[1]).error)" \
"${auth_response}")
    if [[ ${auth_error} == "true" ]]; then
      printf '%s\n' "Error happened in redeem the authorization code: \
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
  local -n default_cf_flag=$1
  default_cf_flag+=(--region="${REGION}")
  default_cf_flag+=(--no-allow-unauthenticated)
  default_cf_flag+=(--timeout=540 --memory="${CF_MEMORY}" --runtime="${CF_RUNTIME}")
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
# Create or update a Cloud Scheduler
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
  local location_flag="--location=${REGION}"
  local scheduler_flag=()
  scheduler_flag+=(--schedule="$2")
  scheduler_flag+=(--time-zone="$3")
  scheduler_flag+=(--topic="$4")
  scheduler_flag+=(--message-body="$5")
  local exist_job
  exist_job=($(gcloud scheduler jobs list --filter="name~${1}" \
--format="value(state)" "${location_flag}"))
  local action needPause
  if [[ ${#exist_job[@]} -gt 0 ]]; then
    action="update"
    scheduler_flag+=(--update-attributes=$6)
    if [[ "${exist_job[0]}" == "PAUSED" ]]; then
      gcloud scheduler jobs resume "${1}" "${location_flag}"
      if [[ $? -gt 0 ]]; then
        printf '%s\n' "Failed to resume paused Cloud Scheduler job [${1}]."
        return 1
      fi
      needPause="true"
    fi
  else
    action="create"
    scheduler_flag+=(--attributes=$6)
  fi
  gcloud scheduler jobs ${action} pubsub "$1" "${scheduler_flag[@]}" "${location_flag}"
  if [[ "${needPause}" == "true" ]]; then
      gcloud scheduler jobs pause "${1}" "${location_flag}"
  fi
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
  email=$(get_service_account)
  printf '%s\n' "The email address of the current service account is: ${email}."
}

#######################################
# Returns the email address of the service account. If there is a key file of
# service account, it returns the email of that service account; otherwise it
# it will get the default service account of Cloud Functions.
# Globals:
#   SA_KEY_FILE
# Arguments:
#   None
#######################################
get_service_account(){
  local email
  if [[ -f "${SA_KEY_FILE}" ]]; then
    email=$(get_value_from_json_file "${SA_KEY_FILE}" 'client_email')
  else
    email=$(get_cloud_functions_service_account)
  fi
  printf '%s' "${email}"
}

#######################################
# Customized function to start the automatic installation process.
# Globals:
#   None
# Arguments:
#   An array of tasks
#######################################
customized_install() {
  local tasks=("$@")
  local task
  for task in "${tasks[@]}"; do
    local cmd
    eval "cmd=(${task})"
    "${cmd[@]}"
    quit_if_failed $?
  done
}

#######################################
# Default function to start the automatic installation process.
# Globals:
#   DEFAULT_INSTALL_TASKS
# Arguments:
#   None
#######################################
default_install() {
  customized_install "${DEFAULT_INSTALL_TASKS[@]}"
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
# Get the access token based on OAuth key file.
# Globals:
#   OAUTH2_TOKEN_JSON
# Arguments:
#   OAuth key file, default value ${OAUTH2_TOKEN_JSON}
#######################################
get_oauth_access_token() {
  local oauthFile clientId clientSecret refreshToken
  oauthFile="${1:-${OAUTH2_TOKEN_JSON}}"
  if [[ ! -s "${oauthFile}" ]]; then
    printf '%s\n' "Fail to find OAuth key file: ${oauthFile}" >&2
    return
  fi
  clientId=$(get_value_from_json_file "${oauthFile}" 'client_id')
  clientSecret=$(get_value_from_json_file "${oauthFile}" 'client_secret')
  refreshToken=$(get_value_from_json_file "${oauthFile}" 'token.refresh_token')
  local request response accessToken
  request=(
    -d "client_id=${clientId}"
    -d "client_secret=${clientSecret}"
    -d "grant_type=refresh_token"
    -d "refresh_token=${refreshToken}"
    -s "https://accounts.google.com/o/oauth2/token"
  )
  response=$(curl "${request[@]}")
  accessToken=$(get_value_from_json_string "${response}" "access_token")
  if [[ -z "${accessToken}" ]]; then
    printf '%s\n' "Fail to refresh access token: ${response}" >&2
    return
  fi
  printf '%s' "${accessToken}"
}

#######################################
# Copy a local file or synchronize a local folder to the target Storage bucket.
# Globals:
#   CONFIG_FILE
# Arguments:
#   File or folder name, a string.
#   Cloud Storage link, default value is 'gs://${GCS_BUCKET}'
#######################################
copy_to_gcs() {
  local source bucket target
  source="${1}"
  bucket="$(get_value_from_json_file "${CONFIG_FILE}" "GCS_BUCKET")"
  target="${2-"gs://${bucket}"}"
  if [[ -d "${source}" ]]; then
    printf '%s\n' "  Synchronizing local folder [${source}] to target \
[${target}]..."
    gsutil -m rsync "${source}" "${target}/${source}"
  else
    printf '%s\n' "  Copying local file [${source}] to target [${target}]..."
    gsutil cp "${source}" "${target}"
  fi
}

#######################################
# Get a value from a given JSON string.
# Globals:
#   None
# Arguments:
#   JSON string, 'dot' is the delimiter of embedded property names.
#   Property name.
# Returns:
#   The value.
#######################################
get_value_from_json_string() {
  local script
  read -d '' script << EOF
const properties = process.argv[2].split('.');
const currentLocation = properties.reduce((previous, currentProperty) => {
  return previous ? previous[currentProperty] : undefined;
}, JSON.parse(process.argv[1]));
let output;
if (typeof currentLocation !== 'object'){
  output = currentLocation;
} else{
  output = JSON.stringify(currentLocation);
}
console.log(output ? output : '');
EOF
  node -e "${script}" "$@"
}

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
  if [[ -s "${1}" ]];then
    local json
    json="$(cat "${1}")"
    printf "$(get_value_from_json_string "${json}" "${2}")"
  else
    printf '%s\n' "Failed to find file ${1} ."
    return 1
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
# Get the project number of the given project.
# Globals:
#   GCP_PROJECT
# Arguments:
#   Project Id, default environment GCP_PROJECT
# Returns:
#   The project number.
#######################################
get_project_number() {
  local projectId
  projectId=${1-"${GCP_PROJECT}"}
  printf '%s' "$(gcloud projects describe "${projectId}" \
    --format='value(projectNumber)')"
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
# Globals:
#   None
# Arguments:
#   The name of Cloud Functions, default value is a random one starts with the
#     namespace.
# Returns:
#   The default service account of the given Cloud Functions.
#######################################
get_cloud_functions_service_account() {
  local cf=($(gcloud functions list --format="csv[no-heading,separator=\
' '](name,REGION)" | grep "${1:-"${PROJECT_NAMESPACE}"}" | head -1))
  if [[ ${#cf[@]} -lt 1 ]]; then
    printf '%s\n' "Cloud Functions [${1}] doesn't exist."
  else
    local name="${cf[0]}"
    local region="${cf[1]}"
    local service_account=$(gcloud functions describe "${name}" \
--region="${region}" --format="get(serviceAccountEmail)")
    printf '%s' "${service_account}"
  fi
}

#######################################
# Make sure the Firestore database is in the current project. If there is no
# Firestore datastore, it will help to create one.
# To create the Firestore, the operator need to be the Owner.
# Globals:
#   GCP_PROJECT
# Arguments:
#   Firestore mode, 'native' or 'datastore'.
#   Firestore region, it's not the same list as Cloud Functions regions and it
#   will be bonded to this Cloud project after created.
#######################################
check_firestore_existence() {
  local firestore mode appRegion
  mode="${1}"
  appRegion="${2}"
  firestore=$(gcloud app describe --format="csv[no-heading](databaseType)")
  if [[ -z "${firestore}" ]]; then
    printf '%s\n' "Firestore is not ready. Creating a new Firestore database\
is an irreversible operation, so read carefully before continue:"
    printf '%s\n' "  1. You need to be the owner of ${GCP_PROJECT} to continue."
    printf '%s\n' "  2. Once you select the region and mode, you cannot change it."
    printf '%s\n' "Press any key to continue..."
    local any
    read -n1 -s any
    if [[ -z "${mode}" ]]; then
      printf '%s\n' "  For more information about mode, see \
https://cloud.google.com/firestore/docs/firestore-or-datastore#choosing_a_database_mode"
    fi
    while [[ -z "${mode}" ]]; do
      printf '%s' "  Enter the mode of your dataset [Native]: "
      local selectMode
      read -r selectMode
      selectMode=$(printf '%s' "${selectMode:-"Native"}" | \
        tr '[:upper:]' '[:lower:]')
      if [[ ${selectMode} == 'native' || ${selectMode} == 'datastore' ]]; then
        mode=${selectMode}
      fi
    done
    printf '%s\n' "Creating Firestore database in ${mode} mode..."
    gcloud app create --region=${appRegion}
    if [[ $? -eq 0 ]]; then
      if [[ "${mode}" == "native" ]]; then
        appRegion=$(gcloud app describe --format="csv[no-heading](locationId)")
        gcloud firestore databases create --region="${appRegion}"
      fi
    else
      return 1
    fi
  else
    printf '%s\n' "OK. Firestore is ready in mode ${firestore}."
  fi
}

#######################################
# Installation step for confirming Firestore is ready.
# See function check_firestore_existence.
#######################################
confirm_firestore() {
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Checking the status of Firestore..."
  check_firestore_existence "$@"
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
  if [[ ${1} -gt 0 ]];then
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
  local separator=\\$1;
  shift
  local first=$1;
  shift
  printf %s "$first" "${@/#/$separator}" | sed -e "s/\\$separator/$separator/g"
}

# Import other bash files.
_SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${_SELF}/google_ads.sh"
source "${_SELF}/bigquery.sh"
source "${_SELF}/apps_scripts.sh"

printf '%s\n' "Common Bash Library is loaded."
