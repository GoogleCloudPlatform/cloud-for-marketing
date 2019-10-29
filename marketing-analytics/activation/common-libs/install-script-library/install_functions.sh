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

# Cloud Functions Runtime Environment.
CF_RUNTIME="${CF_RUNTIME:=nodejs8}"

# Counter for steps.
STEP=0

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
#   GOOGLE_APPLICATION_CREDENTIALS
# Arguments:
#   None
#######################################
emulate_cloud_shell() {
  if [[ ${CLOUD_SHELL} != 'true' ]];then
    echo "To emulate Cloud Shell, set env GCP_PROJECT and ADC credentials."
    CLOUD_SHELL='true'

    local current_project
    current_project=$(gcloud config get-value project 2>/dev/null)
    local target_project
    while [[ -z ${GCP_PROJECT} ]]; do
      if [[ -f "${CONFIG_FILE}" ]]; then
        target_project=$(get_value_from_json_file "${CONFIG_FILE}" 'PROJECT_ID')
      else
        printf '%s' "Can't find ${CONFIG_FILE}, please input Cloud Project ID: \
[${current_project}] "
        local project
        read -r project
        project=${project:-"${current_project}"}
        target_project=${project}
      fi
      if [[ -n ${current_project} && \
${current_project} != "${target_project}" ]];then
        printf '%s' "Set the GCP to [${target_project}].... "
        gcloud config set project "${target_project}"
        if [[ $? -gt 0 ]]; then
          continue
        fi
      fi
      export GCP_PROJECT=$(gcloud config get-value project 2>/dev/null)
    done

    cat <<EOF
  CLOUD_SHELL = ${CLOUD_SHELL}
  PROJECT_ID = ${GCP_PROJECT}
  GOOGLE_APPLICATION_CREDENTIALS = ${GOOGLE_APPLICATION_CREDENTIALS}

EOF
  fi
}

#######################################
# Check whether current auth in local environment can carry out the tasks.
# Globals:
#   GCP_PROJECT
#   GOOGLE_APPLICATION_CREDENTIALS
# Arguments:
#   None
# Returns:
#   0 if passed, non-zero on error.
#######################################
check_authentication() {
  local permissions=("datastore.databases.get" "datastore.entities.create")
  node -e "require('./index.js').checkPermissions(process.argv.slice(1))" \
"${permissions[@]}" 1>/dev/null
  if [[ $? -gt 0 ]]; then
    printf '%s\n' "  Current auth doesn't have enough permissions."
    if [[ -f "./tests/keys/${GCP_PROJECT}.key.json" ]];then
      printf '%s' "  Find default ADC key file and try again..."
      export GOOGLE_APPLICATION_CREDENTIALS=\
"./tests/keys/${GCP_PROJECT}.key.json"
      node -e "require('./index.js').checkPermissions(process.argv.slice(1))" \
"${permissions[@]}" 1>/dev/null
      if [[ $? -eq 0 ]]; then
        printf '%s\n' "succeeded!"
        return 0
      else
        printf '%s\n' "failed."
      fi
    fi
    cat <<EOF
Current shell is not in a Cloud Shell and there is no available key file.
  Note: In local mode, 'GOOGLE_APPLICATION_CREDENTIALS' is expected in \
environment variables. Also, make sure the service account related has enough \
permissions.

Quit now.
EOF
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
  printf '%s\n' "Project name suffix '${PROJECT_NAME}'"
  if [[ -f "${CONFIG_FILE}" ]]; then
    printf '%s\n' "Find configuration file '${CONFIG_FILE}', loading ..."
    local i value key
    for i in "${!CONFIG_ITEMS[@]}"; do
      value=$(get_value_from_json_file "${CONFIG_FILE}" "${CONFIG_ITEMS[$i]}")
      key=${CONFIG_ITEMS[$i]}
      declare -g "${key}=${value}"
      printf '%s\n' "  ${key} = ${!key}"
    done
    printf '%s\n\n' "Note: values can be changed during installation process."
  fi
	export GCP_PROJECT=$(gcloud config get-value project)
}

#######################################
# Check the environment is in Cloud Shell environment or not.
# Globals:
#   CLOUD_SHELL
# Arguments:
#   None
# Returns:
#   0 in a Cloud Shell, non-zero not.
#######################################
check_in_cloud_shell() {
  (( STEP += 1 ))
  printf '%s\n' "STEP[${STEP}] Check the environment..."
  cat <<EOF
  Rational: The process will require lots of operation on Google Cloud Project.\
Doing this in a Cloud Shell is easier by leveraging the ready tools and \
avoiding authentication issues.
EOF
  if [[ ${CLOUD_SHELL} != 'true' ]];then
    printf '%s\n' "[Failed] It looks like we are not in a Cloud Shell. For \
more information, see: https://cloud.google.com/shell/"
    return 1
  else
    printf '%s\n' "[OK] Great. It looks like we are in a Cloud Shell."
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
  printf '%s\n' "STEP[${STEP}] Prepare dependency libraries..."
  mkdir -p libs
  npm run prepare -s
  local flag=$?
  npm install -s nodejs-common
  flag=$(( $?+flag ))
  npm install -s
  flag=$(( $?+flag ))
  if [[ ${flag} -gt 0 ]];then
    printf '%s\n' "[Failed] Fail to install dependencies."
    return 1
  else
    printf '%s\n' "[OK] Great. All dependencies are ready."
    return 0
  fi
}

#######################################
# Confirm the Cloud Project
# Globals:
#   GCP_PROJECT
# Arguments:
#   None
# Returns:
#   0 succeeded, non-zero on error.
#######################################
confirm_project() {
  (( STEP += 1 ))
  printf '%s\n' "STEP[${STEP}] Confirm Google Cloud Project (GCP) ..."
  while :; do
    printf '%s' "Input the Project ID of GCP that is going to install \
[${GCP_PROJECT}]:"
    local input result
    read -r input
    input=${input:-"${GCP_PROJECT}"}
    result=$(gcloud config set project "${input}" --user-output-enabled=false \
2>&1)
    if [[ -z ${result} ]]; then
      printf '%s\n' "[OK] Continue installation in [${input}]."
      GCP_PROJECT="${input}"
			export GCP_PROJECT=$(gcloud config get-value project)
      return 0
    else
      printf '%s\n' "  [Failed] ${result}"
    fi
  done
}

#######################################
# Confirm and set env variable for the Region for Cloud Functions.
# Globals:
#   PROJECT_NAME
#   REGION
# Arguments:
#   None
#######################################
confirm_region() {
  (( STEP += 1 ))
  printf '%s\n' "STEP[${STEP}] Select the region to deploy ${PROJECT_NAME}..."
  cat <<EOF
This solution is based on Cloud Functions and Cloud Storage. Please select the \
region to deploy based on your requirement. Same region is suggested here for \
Cloud Functions and Cloud Storage for the interests of latency and pricing.
For more information, see: https://cloud.google.com/functions/docs/locations
EOF
  set_region
  printf '%s\n' "[ok] Will deploy to ${REGION}."
}

#######################################
# Set the env variable 'REGION' as the Cloud Functions deploy target.
# Globals:
#   PROJECT_NAME
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
    exist_functions=($(gcloud functions list --filter="name~${PROJECT_NAME}" \
--format="value(REGION[])"))
    if [[ ${#exist_functions[@]} -gt 0 ]]; then
      local exist_region
      exist_region=$(printf "${exist_functions[0]}" | cut -d/ -f4 | uniq)
      printf '%s\n' "Related Cloud Functions are already installed in region: \
${exist_region}."
      local i
      for i in "${!exist_functions[@]}"; do
        printf '  %s\n' "${exist_functions[$i]}"
      done
      printf '%s' "Would you like to continue update those Cloud Functions in \
region '${exist_region}'? [Y/n]:"
      local continue
      read -r continue
      continue=${continue:-"Y"}
      if [[ ${continue} = "Y" || ${continue} = "y" ]]; then
        region=${exist_region}
        break
      else
        printf '%s' "If you want to deploy to other regions, current Cloud \
Functions will be deleted, Confirm? [N/y]:"
        local confirm_delete
        read -r confirm_delete
        if [[ ${confirm_delete} = "Y" || ${confirm_delete} = "y" ]]; then
          for i in "${!exist_functions[@]}"; do
            local exist_function
            exist_function=$(printf "${exist_functions[$i]}" | cut -d/ -f6)
            local function_region
            function_region=$(printf "${exist_functions[$i]}" | cut -d/ -f4)
            gcloud functions delete --region="${function_region}" \
"${exist_function}"
          done
        else
          continue
        fi
      fi
    else
      printf '%s\n' "Following are available regions for Cloud Functions. \
Please select the region to deploy:"
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
# Check whether current operator has enough permissions to install.
# Globals:
#   GOOGLE_CLOUD_PERMISSIONS
# Arguments:
#   None
# Returns:
#   0 if all permissions are granted, 1 on not.
#######################################
check_permissions() {
  (( STEP += 1 ))
  printf '%s\n' "STEP[${STEP}] Check current operator's access..."
  local role error
  for role in "${!GOOGLE_CLOUD_PERMISSIONS[@]}"; do
    printf '%s'  "  Check permissions for ${role}... "
    local permissions
    permissions=(${GOOGLE_CLOUD_PERMISSIONS[${role}]})
    node -e "require('./index.js').checkPermissions(process.argv.slice(1))" \
"${permissions[@]}" 1>/dev/null
    if [[ $? -gt 0 ]]; then
        message='failed'
        error=1
      else
        message='successfully'
      fi
      printf '%s\n' " ${message}."
  done
  if [[ ${error} -gt 0 ]]; then
    printf '%s\n' "[Failed] Failed to pass permission check."
    return 1
  else
    printf '%s\n' "[OK] Passed permissions check for Project [${GCP_PROJECT}]."
    return 0
  fi
}

#######################################
# Enable Apis in Cloud project.
# Globals:
#   GOOGLE_CLOUD_APIS
# Arguments:
#   None
# Returns:
#   0 if all Apis enabled, 1 on not.
#######################################
enable_apis() {
  (( STEP += 1 ))
  printf '%s\n' "STEP[${STEP}] Enable necessary components..."
  local error=0 api single_api
  for api in "${!GOOGLE_CLOUD_APIS[@]}"; do
    printf '%s' "Enable ${GOOGLE_CLOUD_APIS[${api}]}... "
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
    printf '%s\n' "[Failed] Failed to enable some APIs."
    return 1
  else
    printf '%s\n' "[OK] APIs are enabled for Project [${GCP_PROJECT}]."
    return 0
  fi
}

#######################################
# Confirm or create a Storage bucket for this project.
# Globals:
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
  printf '%s\n' "STEP[${STEP}] Input a Storage Bucket name..."
  cat <<EOF
  Rationale: This solution will monitor the new files incoming to this Bucket. \
This kind of event driven architecture can reduce the interval between \
scheduled checks as well as simplify the solution.
  For more, see https://cloud.google.com/functions/docs/calling/storage
  You can use existent bucket or create a new one here.
EOF
#  generate default Bucket name
  local default_bucket_name
  default_bucket_name=$(get_default_bucket_name "${GCP_PROJECT}")
  GCS_BUCKET=${GCS_BUCKET:-$default_bucket_name}
# available buckets in current project
  local all_buckets
  all_buckets="$(gsutil ls)";
  while :; do
    printf '%s' "Please input the Cloud Storage Bucket Name[${GCS_BUCKET}]:"
    local bucket
    read -r bucket
    bucket=${bucket:-$GCS_BUCKET}
    local bucket_str="gs://${bucket}/"
    if [[ ${all_buckets} == *"${bucket_str}"* ]]; then
      printf '%s\n' "Great. The Bucket [${bucket}] exists in current \
project[${GCP_PROJECT}]."
      GCS_BUCKET=${bucket}
      break
    fi
    printf '%s' "  Check the existence of Bucket [$bucket]..."
    local result
    result="$(gsutil ls -p "${GCP_PROJECT}" "${bucket_str}" 2>&1)"
    if [[ ${result} =~ .*(BucketNotFoundException: 404 ).* ]]; then
      printf '%s\n' " not existent. Continue to create Bucket."
      if [[ -n ${REGION} ]]; then
        printf '%s' "Would you like to create the Storage Bucket at \
'${REGION}'? [Y/n]:"
        local user_region
        read -r user_region
        user_region=${user_region:-"Y"}
        if [[ ${user_region} = "Y" || ${user_region} = "y" ]]; then
          printf '%s\n' "  Try to create the Bucket [${bucket}] at ${REGION}..."
          gsutil mb -c REGIONAL -l "${REGION}" "${bucket_str}"
          if [[ $? -gt 0 ]]; then
            printf '%s\n' "Fail to create Bucket named ${bucket}. Please try \
again."
            continue
          else
            GCS_BUCKET=${bucket}
            break
          fi
        fi
      fi
      printf '%s\n' "Please select the Storage region:"
      local region class
      select region in "${STORAGE_REGIONS[@]}"; do
        if [[ -n "${region}" ]]; then
          if [[ $REPLY = 1 ]]; then
            class="MULTI_REGIONAL"
          else
            class="REGIONAL"
          fi
          cat <<EOF
Selected region[${region}] class is ${class}. For more information of \
Storage class, see https://cloud.google.com/storage/docs/storage-classes

Continue to select the location for the Storage bucket (input 0 to return to \
region selection):
  For more information of Bucket locations, see \
https://cloud.google.com/storage/docs/locations
EOF
          declare -n options="${STORAGE_REGIONS_PARAMETER["$((REPLY-1))"]}"
          local location
          select location in "${options[@]}"; do
            if [[ -n "${location}" ]]; then
              printf '%s\n' "  Try to create the Bucket [${bucket}] at \
${location}..."
              gsutil mb -c "${class}" -l "${location}" "${bucket_str}"
              if [[ $? -gt 0 ]]; then
                printf '%s\n' "Fail to create Bucket named ${bucket}. Please \
try again."
                break 2
              else
                GCS_BUCKET=${bucket}
                break 3
              fi
            elif [[ ${REPLY} = 0 ]]; then
              break
            else
              printf '%s\n' "Please select the location (input 0 to return to \
region selection):"
            fi
          done
        fi
        printf '%s\n' "Please select the Storage region (press ENTER to \
refresh the list):"
      done
    elif [[ ${result} =~ .*(AccessDeniedException: 403 ).* ]]; then
      printf '%s\n' "  The Bucket [${bucket}] exists and current account \
doesn't have the access."
      GCS_BUCKET="${default_bucket_name}"
    else
      printf '%s\n' "  The Bucket [${bucket}] exists in other project, so it \
can't be the trigger source here."
      GCS_BUCKET="${default_bucket_name}"
    fi
    printf '%s\n' "Please try another one."
  done
  printf '%s\n\n' "[OK] ${PROJECT_NAME} will monitor the Bucket named \
[${GCS_BUCKET}]."
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
  printf '%s\n' "STEP[${STEP}] Confirm ${CONFIG_FOLDER_NAME} folder..."
  local loaded_value="${!CONFIG_FOLDER_NAME}"
  local default_value
  default_value=$(printf '%s' "${CONFIG_FOLDER_NAME}" | \
tr '[:upper:]' '[:lower:]')

  local folder=${loaded_value:-${default_value}}
  cat <<EOF
  Rationale: Since Cloud Storage events are bound to Bucket, in order to \
prevent this Cloud Functions occupying the bucket exclusively, an explicit \
folder is required here. This solution will only take the files that under \
that folder.
  Note: Cloud Functions will move files to folder 'processed/' after takes the \
files.
EOF
  printf '%s' "Input the ${CONFIG_FOLDER_NAME} folder name [${folder}]: "
  local input
  read -r input
  folder=${input:-"${folder}"}
  if [[ ! ${folder} =~ ^.*/$ ]]; then
    folder="${folder}/"
  fi
  declare -g "${CONFIG_FOLDER_NAME}=${folder}"
  printf '%s\n\n' "[OK] Continue with monitored folder as [${folder}]."
}

#######################################
# Confirm the prefix of Cloud Pub/Sub topics and subscriptions.
# Globals:
#   PS_TOPIC
#   PROJECT_NAME
# Arguments:
#   None
#######################################
confirm_topic() {
  (( STEP += 1 ))
  printf '%s\n' "STEP[${STEP}] Confirm Topic prefix for Pub/Sub..."
  PS_TOPIC=${PS_TOPIC:-"${PROJECT_NAME}"}
  cat <<EOF
  Rationale: To avoid potential conflicts with other application for using \
Pub/Sub topics, a unified prefix for Topics and Subscriptions will be used.
EOF
  printf '%s' "Input the prefix for Topics [${PS_TOPIC}]: "
  local topic
  read -r topic
  PS_TOPIC=${topic:-"${PS_TOPIC}"}
  printf '%s\n' "[OK] Continue with Topic prefix as [${PS_TOPIC}]"
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
  printf '%s\n' "STEP[${STEP}] Save the configuration..."

  local config_items_str="  \"PROJECT_ID\": \"${GCP_PROJECT}\""
  local i
  for i in "${!CONFIG_ITEMS[@]}"; do
    local key=${CONFIG_ITEMS[$i]}
    config_items_str="${config_items_str},"$'\n'"  \"${key}\": \"${!key}\""
  done
  local json_str=$'{\n'"${config_items_str}"$'\n}'$'\n'
  printf '%s\n%s' "  Will save" "${json_str}"
  printf '%s' "  to '${CONFIG_FILE}'. Confirm? [Y/n]"
  local input
  read -r input
  if [[ -z ${input} || ${input} = 'y' || ${input} = 'Y' ]];then
    printf '%s' "${json_str}" > "${CONFIG_FILE}"
    printf '%s\n' "[OK] Save to ${CONFIG_FILE}"
    return 0
  else
    printf '%s\n' "[User cancelled]";
    return 1
  fi
}

#######################################
# Run default function if there is no parameters; otherwise run the specific
# function.
# Globals:
#   MAIN_FUNCTION
# Arguments:
#   None
#######################################
run_default_function() {
  if [[ -z "$1" ]]; then
    "${MAIN_FUNCTION}"
  else
    printf '%s\n\n' "Running single task [$*], need to set variables..."
    emulate_cloud_shell
    load_config
    check_authentication
    "$@"
  fi
}

# Utilities functions

#######################################
# Load a value from a given JSON file. Note, only support JSON file with the
# type as: {{string, string}}.
# Globals:
#   None
# Arguments:
#   JSON file path.
#   Property name.
# Returns:
#   The value.
#######################################
get_value_from_json_file() {
  if [[ -s $1 ]];then
    node -e "console.log(require(process.argv[1])[process.argv[2]]||'')" "$@"
  fi
}

#######################################
# Generate a default bucket name based on the given string. If there is a colon
# in the string, will just use the part before colon.
# Globals:
#   None
# Arguments:
#   Any string.
# Returns:
#   The default bucket name.
#######################################
get_default_bucket_name() {
  printf '%s' "${PROJECT_NAME}-$(printf '%s' "$1" | \
sed  -r 's/^([^:]*:)?(.*)$/\2/')"
}

#######################################
# Generate a domain name for the email of current project's service account.
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
# Make sure there is a Firestore or Datastore in current project.
# Globals:
#   GCP_PROJECT
# Arguments:
#   None.
#######################################
check_firestore_existence() {
  local firestore_status
  firestore_status=$(gcloud beta firestore operations list 2>&1)
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
    firestore_status=$(gcloud beta firestore operations list 2>&1)
  done
}

#######################################
# Check the last execution status. Quit current process if there is an error.
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
