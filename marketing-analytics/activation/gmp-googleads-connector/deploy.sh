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

# Cloud Functions Runtime Environment
if [[ -z ${CF_RUNTIME} ]]; then
  CF_RUNTIME=nodejs8
fi

# Default project name is 'tentacles'. It will be used as prefix of Cloud Functions, PubSub, etc.
# You can change it here (only lowercase letters, numbers and dashes(-) are suggested).
if [[ -z ${PROJECT_NAME} ]]; then
  PROJECT_NAME="tentacles"
fi

# Project configuration file
CONFIG_FILE="./config.json"

# Service account key file
SA_KEY_FILE="./keys/service-account.key.json"

DEFAULT_SERVICE_ACCOUNT="${PROJECT_NAME}-api"

# Whether or not service account is required for this installation based on the selection of APIs.
NEED_SERVICE_ACCOUNT="false"

# To create topics/subscriptions
declare -a ENABLED_INTEGRATED_APIS

# The APIs that will be used in Tentacles.
declare -A GOOGLE_CLOUD_APIS
GOOGLE_CLOUD_APIS=(
  ["iam.googleapis.com"]="Identity and Access Management (IAM) API"
  ["cloudresourcemanager.googleapis.com"]="Cloud Resource Manager API"
  ["firestore.googleapis.com"]="Google Cloud Firestore API"
  ["cloudfunctions"]="Cloud Functions API"
  ["pubsub"]="Cloud Pub/Sub API"
)

# Description of external APIs.
declare -a INTEGRATION_APIS_DESCRIPTION=(
  "Google Analytics Measurement Protocol"
  "Google Analytics Data Import"
  "Campaign Manager Conversions Upload"
  "SFTP Upload"
  "Google Ads conversions scheduled uploads based on Google Sheets"
  "Search Ads 360 Conversions Upload"
#  "Google Ads"
)

# All build-in external APIs.
declare -a INTEGRATION_APIS=(
  "N/A"
  "analytics"
  "dfareporting doubleclicksearch"
  "N/A"
  "sheets.googleapis.com"
  "doubleclicksearch"
  "googleads"
)

# Code of external APIs.
declare -a INTEGRATION_APIS_CODE=(
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
  ["Service Usage Admin"]="servicemanagement.services.bind serviceusage.services.enable"
  ["Storage Admin"]="storage.buckets.create storage.buckets.list"
  ["Pub/Sub Editor"]="pubsub.subscriptions.create pubsub.topics.create"
  ["Service Account User"]="iam.serviceAccounts.actAs"
  ["Cloud Functions Developer"]="cloudfunctions.functions.create"
  ["Cloud Datastore User"]="cloudfunctions.functions.create"
)

# https://cloud.google.com/iam/docs/understanding-roles#service-accounts-roles
declare -A GOOGLE_SERVICE_ACCOUNT_PERMISSIONS
GOOGLE_SERVICE_ACCOUNT_PERMISSIONS=(
  ["Service Account Admin"]="iam.serviceAccounts.create"
  ["Service Account Key Admin"]="iam.serviceAccounts.create"
)

# Storage regions
declare -a STORAGE_REGIONS=(
  "Multi-regional"
  "North America"
  "South America"
  "Europe"
  "Asia"
  "Australia"
)

# Region location list
declare -a STORAGE_REGIONS_PARAMETER=(
  "MULTI_REGIONAL"
  "NORTH_AMERICA"
  "SOUTH_AMERICA"
  "EUROPE"
  "ASIA"
  "AUSTRALIA"
)
declare -a MULTI_REGIONAL=("ASIA" "EU" "US")
declare -a NORTH_AMERICA=("northamerica-northeast1" "us-central1" "us-east1" "us-east4" "us-west1" "us-west2")
declare -a SOUTH_AMERICA=("southamerica-east1")
declare -a EUROPE=("europe-north1" "europe-west1" "europe-west2" "europe-west3" "europe-west4" "europe-west6")
declare -a ASIA=("asia-east1" "asia-east2" "asia-northeast1" "asia-northeast2" "asia-south1" "asia-southeast1")
declare -a AUSTRALIA=("australia-southeast1")

# Counter for steps
STEP=0

emulate_cloud_shell(){
  if [[ ${CLOUD_SHELL} != 'true' ]];then
    printf '%s\n' "To emulate Cloud Shell, set variables and ADC credentials."
    CLOUD_SHELL='true'
    if [[ -f "${CONFIG_FILE}" ]]; then
      GOOGLE_CLOUD_PROJECT=$(get_value_from_json_file "${CONFIG_FILE}" 'PROJECT_ID')
    else
      printf '%s' "Can't find ${CONFIG_FILE}, please input Cloud Project ID here: "
      read -r project
      project=${project:-"oc-bridge"}
      GOOGLE_CLOUD_PROJECT=${project}
    fi
#    DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
    if [[ -f "./tests/keys/${GOOGLE_CLOUD_PROJECT}.key.json" ]];then
      export GOOGLE_APPLICATION_CREDENTIALS="./tests/keys/${GOOGLE_CLOUD_PROJECT}.key.json"
    else
      export GOOGLE_APPLICATION_CREDENTIALS=""
    fi
    printf '%s\n' "  CLOUD_SHELL = ${CLOUD_SHELL}"
    printf '%s\n' "  GOOGLE_CLOUD_PROJECT = ${GOOGLE_CLOUD_PROJECT}"
    printf '%s\n' "  GOOGLE_APPLICATION_CREDENTIALS = ${GOOGLE_APPLICATION_CREDENTIALS}"
    if [[ $(gcloud config get-value project) != "${GOOGLE_CLOUD_PROJECT}" ]];then
      printf '%s' "Set the GCP to [${GOOGLE_CLOUD_PROJECT}].... "
      gcloud config set project "${GOOGLE_CLOUD_PROJECT}"
    fi
    printf '\n'
  fi
}

get_value_from_json_file(){
  if [[ -s $1 ]];then
    node -e "console.log(require(process.argv[1])[process.argv[2]])" "$@"
  fi
}

get_default_bucket_name(){
  printf '%s' "${PROJECT_NAME}-$(printf '%s' "$1" | sed  -r 's/^([^:]*:)?(.*)$/\2/')"
}

get_sa_domain_from_gcp_id(){
  if [[ $1 =~ .*(:).* ]]; then
    printf '%s' "$(printf '%s' "$1" | sed  -r 's/^([^:]*):(.*)$/\2.\1/').iam.gserviceaccount.com"
  else
    printf '%s' "$1.iam.gserviceaccount.com"
  fi
}

check_firestore_existence(){
  firestore=$(gcloud beta firestore operations list 2>&1)
  while [[ ${firestore} =~ .*NOT_FOUND.* ]]; do
    printf '%s\n' "There is no Firestore/Datastore database. Please visit https://console.cloud.google.com/firestore?project=${GOOGLE_CLOUD_PROJECT} to create a database before continue."
    printf '%s\n' "Press any key to continue after you create the database..."
    read -n1 -s any
    printf '\n'
    firestore=$(gcloud beta firestore operations list 2>&1)
  done
}

load_config(){
  printf '%s\n' "Project name suffix '${PROJECT_NAME}'"
  if [[ -f "${CONFIG_FILE}" ]]; then
    printf '%s\n' "Find configuration file '${CONFIG_FILE}', loading ..."
    OUTBOUND=$(get_value_from_json_file "${CONFIG_FILE}" 'OUTBOUND')
    INBOUND=$(get_value_from_json_file "${CONFIG_FILE}" 'INBOUND')
    GCS_BUCKET=$(get_value_from_json_file "${CONFIG_FILE}" 'GCS_BUCKET')
    PS_TOPIC=$(get_value_from_json_file "${CONFIG_FILE}" 'PS_TOPIC')
    printf '%s\n' "  GCS_BUCKET = ${GCS_BUCKET}"
    printf '%s\n' "  OUTBOUND = ${OUTBOUND}"
    printf '%s\n' "  INBOUND = ${INBOUND}"
    printf '%s\n' "  PS_TOPIC = ${PS_TOPIC}"
    printf '%s\n\n' "Note: values can be changed during installation process."
  fi
}

quit_if_failed(){
  printf '\n'
  if [[ $1 -gt 0 ]];then
    printf '%s\n' "[Error] Quit."
    exit 1
  fi
}

print_welcome(){
  printf '%s\n' "###########################################################"
  printf '%s\n' "##                                                       ##"
  printf '%s\n' "##          Welcome to install Tentacles                 ##"
  printf '%s\n' "##                                                       ##"
  printf '%s\n\n' "###########################################################"
  load_config
}

prepare_dependencies(){
  STEP=$((STEP+1))
  printf '%s\n' "STEP[${STEP}] Prepare dependency libraries..."
  mkdir -p libs
  npm run prepare -s
  npm install -s nodejs-common
  npm install -s
  flag=$?
  if [[ ${flag} -gt 0 ]];then
    printf '%s\n' "[Failed] Fail to install dependencies."
    return 1
  else
    printf '%s\n' "[OK] Great. All dependencies are ready."
    return 0
  fi
}


check_in_cloud_shell(){
  STEP=$((STEP+1))
  printf '%s\n' "STEP[${STEP}] Check the environment..."
  printf '%s\n' "  Rational: The process will require lots of operation on Google Cloud Project. Doing this in a Cloud"
  printf '%s\n' "  Shell is easier by leveraging the ready tools and avoiding authentication issues."
  if [[ ${CLOUD_SHELL} != 'true' ]];then
    printf '%s\n' "[Failed] It looks like we are not in a Cloud Shell. For more information, see: https://cloud.google.com/shell/"
    return 1
  else
    printf '%s\n' "[OK] Great. It looks like we are in a Cloud Shell."
    return 0
  fi
}

confirm_project(){
  STEP=$((STEP+1))
  printf '%s\n' "STEP[${STEP}] Confirm Google Cloud Project (GCP) ..."
  while :; do
    printf '%s' "Input the ID of GCP that Tentacles is going to be installed[${GOOGLE_CLOUD_PROJECT}]:"
    read -r input
    INPUT_GCP=${input:-"${GOOGLE_CLOUD_PROJECT}"}
    result=$(gcloud config set project "${INPUT_GCP}" --user-output-enabled=false 2>&1)
    if [[ -z ${result} ]]; then
      printf '%s\n' "[OK] Continue installation in [${INPUT_GCP}]."
      GOOGLE_CLOUD_PROJECT="${INPUT_GCP}"
      return 0
    else
      printf '%s\n' "  [Failed] ${result}"
    fi
  done
}

set_region(){
  declare -a locations
  locationsRaw=$(gcloud functions regions list)
  locations=($(printf "${locationsRaw}"|grep "projects"|sed 's/projects\/.*\/locations\///'))
  while :; do
    declare -a existFunctions=($(gcloud functions list --filter="name~${PROJECT_NAME}" --format="value(REGION[])"))
    if [[ ${#existFunctions[@]} -gt 0 ]]; then
      existRegion=$(printf "${existFunctions[1]}" | cut -d/ -f4 | uniq)
      printf '%s\n' "Tentacles' Cloud Functions are already installed in region: ${existRegion}."
      for i in "${!existFunctions[@]}"; do
        existFunction="${existFunctions[$i]}"
        printf '  %s\n' "${existFunction}"
      done
      printf '%s' "Would you like to continue update those Cloud Functions in region '${existRegion}'? [Y/n]:"
      read -r continue
      CONTINUE=${continue:-"Y"}
      if [[ ${CONTINUE} = "Y" || ${CONTINUE} = "y" ]]; then
        region=${existRegion}
        break
      else
        printf '%s' "If you want to deploy to other regions, current Cloud Functions will be deleted, Confirm? [N/y]:"
        read -r confirmDelete
        if [[ ${confirmDelete} = "Y" || ${confirmDelete} = "y" ]]; then
          for i in "${!existFunctions[@]}"; do
            existFunction=$(printf "${existFunctions[$i]}" | cut -d/ -f6)
            currentRegion=$(printf "${existFunctions[$i]}" | cut -d/ -f4)
            gcloud functions delete --region="${currentRegion}" "${existFunction}"
          done
        else
          continue
        fi
      fi
    else
      printf '%s\n' "Following are available regions for Cloud Functions. Please select the region to deploy:"
      select region in "${locations[@]}"; do
        if [[ " ${locations[@]} " =~ " ${region} " ]]; then
          break 2
        fi
      done
    fi
  done
  REGION="${region}"
}

confirm_region(){
  STEP=$((STEP+1))
  printf '%s\n' "STEP[${STEP}] Select the region to deploy Tentacles..."
  printf '%s\n' "Tentacles is based on Cloud Functions and Cloud Storage. Please select the region to deploy based on your requirement."
  set_region
  printf '%s\n' "[ok] Will deploy Tentacles to ${REGION}."
}


confirm_apis(){
  STEP=$((STEP+1))
  printf '%s\n' "STEP[${STEP}] Select the APIs that will be integrated:"
  for i in "${!INTEGRATION_APIS_DESCRIPTION[@]}"; do
    printf "%s) %s\n" "${i}" "${INTEGRATION_APIS_DESCRIPTION[$i]}"
  done
  printf '%s' "Use comma to separate APIs or * for all: [*]"
  IFS=', ' read -r -a input

  if [[ ${#input[@]} = 0 || ${input[0]} = '*' ]]; then
    for ((i=0; i<${#INTEGRATION_APIS_DESCRIPTION[@]}; i+=1)); do
      input["${i}"]="${i}"
    done
  fi
  for i in "${!input[@]}"; do
    index="${input[$i]}"
    ENABLED_INTEGRATED_APIS+=("${INTEGRATION_APIS_CODE["${index}"]}")
    if [[ ${INTEGRATION_APIS[${index}]} != "N/A" ]]; then
      NEED_SERVICE_ACCOUNT="true"
      GOOGLE_CLOUD_APIS[${INTEGRATION_APIS["${index}"]}]="${INTEGRATION_APIS_DESCRIPTION["${index}"]}"
      printf '%s\n' "  Add ${INTEGRATION_APIS_DESCRIPTION["${index}"]} to enable APIs list."
    fi
  done
}

check_permissions(){
  STEP=$((STEP+1))
  printf '%s\n' "STEP[${STEP}] Check current operator's access..."
  if [[ ${NEED_SERVICE_ACCOUNT} = 'true' ]]; then
    for role in "${!GOOGLE_SERVICE_ACCOUNT_PERMISSIONS[@]}"; do
      GOOGLE_CLOUD_PERMISSIONS["${role}"]="${GOOGLE_SERVICE_ACCOUNT_PERMISSIONS["${role}"]}"
    done
  fi
  for role in "${!GOOGLE_CLOUD_PERMISSIONS[@]}"; do
    printf '%s'  "  Check permissions for ${role}... "
    declare -a permissions=(${GOOGLE_CLOUD_PERMISSIONS[${role}]})
    GOOGLE_CLOUD_PROJECT="${GOOGLE_CLOUD_PROJECT}" node -e "require('./index.js').checkPermissions(process.argv.slice(1))" "${permissions[@]}" 1>/dev/null
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
    printf '%s\n' "[OK] Passed permissions check for Project [${GOOGLE_CLOUD_PROJECT}]."
    return 0
  fi
}

enable_apis(){
  STEP=$((STEP+1))
  printf '%s\n' "STEP[${STEP}] Enable necessary components..."
  local error=0
  for api in "${!GOOGLE_CLOUD_APIS[@]}"; do
    printf '%s' "Enable ${GOOGLE_CLOUD_APIS[${api}]}... "
    for singleApi in ${api}; do
      gcloud services enable "${singleApi}" 2>/dev/null
      if [[ $? -gt 0 ]]; then
        message='failed'
        error=1
      else
        message='successfully'
      fi
      printf '%s\n' "[${singleApi}] ${message}."
    done
  done
  if [[ ${error} -gt 0 ]]; then
    printf '%s\n' "[Failed] Failed to enable some APIs."
    return 1
  else
    printf '%s\n' "[OK] APIs are enabled for Project [${GOOGLE_CLOUD_PROJECT}]."
    return 0
  fi
}

create_bucket(){
  STEP=$((STEP+1))
  printf '%s\n' "STEP[${STEP}] Input a Storage Bucket name..."
  printf '%s\n' "  Rationale: Tentacles will monitor the new files to this Bucket. Event driven architecture "
  printf '%s\n' "  reduce the interval between scheduled checks as well as simply the solution."
  printf '%s\n' "  For more, see https://cloud.google.com/functions/docs/calling/storage"
  printf '%s\n' "  You can use existent bucket or create a new one here."
#  generate default Bucket name
  GCS_BUCKET=${GCS_BUCKET:-"$(get_default_bucket_name "${GOOGLE_CLOUD_PROJECT}")"}
# available buckets in current project
  local all_buckets="$(gsutil ls)";
  while :; do
    printf '%s' "Please input the Cloud Storage Bucket Name[${GCS_BUCKET}]:"
    read -r bucket
    bucket=${bucket:-$GCS_BUCKET}
    bucket_str="gs://${bucket}/"
    if [[ ${all_buckets} == *"${bucket_str}"* ]]; then
      printf '%s\n' "Great. The Bucket [${bucket}] exists in current project[${GOOGLE_CLOUD_PROJECT}]."
      GCS_BUCKET=${bucket}
      break
    fi
    printf '%s' "  Try to check the existence of Bucket[$bucket]..."
    result="$(gsutil ls -p "${GOOGLE_CLOUD_PROJECT}" "${bucket_str}" 2>&1)"
    if [[ ${result} =~ .*(BucketNotFoundException: 404 ).* ]]; then
      printf '%s\n' " not existent. Continue to create Bucket."
      if [[ -n ${REGION} ]]; then
        printf '%s' "Would you like to create the Storage Bucket at '${REGION}'? [Y/n]:"
        read -r useRegion
        noSelectionRegion=${useRegion:-"Y"}
        if [[ ${noSelectionRegion} = "Y" || ${noSelectionRegion} = "y" ]]; then
          location=${REGION}
          printf '%s\n' "  Try to create the Bucket [${bucket}] at ${location}..."
          gsutil mb -c REGIONAL -l "${location}" "${bucket_str}"
          if [[ $? -gt 0 ]]; then
            printf '%s\n' "Fail to create Bucket named ${bucket}. Please try again."
            continue
          else
            GCS_BUCKET=${bucket}
            break
          fi
        fi
      fi
      printf '%s\n' "Please select the Storage region:"
      select region in "${STORAGE_REGIONS[@]}"; do
        if [[ -n "${region}" ]]; then
          if [[ $REPLY = 1 ]]; then
            class="MULTI_REGIONAL"
          else
            class="REGIONAL"
          fi
          printf '%s\n' "Select region[${region}]'s class is ${class}."
          printf '  %s\n' "For more information of Storage class, see https://cloud.google.com/storage/docs/storage-classes"
          declare -n options="${STORAGE_REGIONS_PARAMETER["$((REPLY-1))"]}"
          printf '%s\n' "Continue to select the location for the Storage bucket (input 0 to return to region selection):"
          printf '%s\n' "For more information of Bucket locations, see https://cloud.google.com/storage/docs/locations"
          select location in "${options[@]}"; do
            if [[ -n "${location}" ]]; then
              printf '%s\n' "  Try to create the Bucket [${bucket}] at ${location}..."
              gsutil mb -c "${class}" -l "${location}" "${bucket_str}"
              if [[ $? -gt 0 ]]; then
                printf '%s\n' "Fail to create Bucket named ${bucket}. Please try again."
                break 2
              else
                GCS_BUCKET=${bucket}
                break 3
              fi
            elif [[ ${REPLY} = 0 ]]; then
              break
            else
              printf '%s\n' "Please select the location (input 0 to return to region selection):"
            fi
          done
        fi
        printf '%s\n' "Please select the Storage region (press ENTER to refresh the list):"
      done
    elif [[ ${result} =~ .*(AccessDeniedException: 403 ).* ]]; then
      printf '%s\n' "  The Bucket [${bucket}] exists and current account doesn't have the access."
      GCS_BUCKET="YOUR_BUCKET_NAME"
    else
      printf '%s\n' "  The Bucket [${bucket}] exists in other project, so it can't be the trigger source here."
      GCS_BUCKET="YOUR_BUCKET_NAME"
    fi
    printf '%s\n' "Please try another one."
  done
  printf '%s\n' "[OK] Tentacles will monitor the Bucket named [${GCS_BUCKET}]."
  return 0
}

confirm_folder(){
  STEP=$((STEP+1))
  printf '%s\n' "STEP[${STEP}] Confirm OUTBOUND folder..."
  OUTBOUND=${OUTBOUND:-"outbound"}
  printf '%s\n' "  Rationale: Since Cloud Storage events are bound to Bucket, in order to prevent Tentacles occupying "
  printf '%s\n' "  the bucket exclusively, an explicit folder is required here. Tentacles will only take the files "
  printf '%s\n' "  that under that folder."
  printf '%s\n' "  Note: Tentacles will move files to folder 'processed/' after takes the files."
  printf '%s' "Input the OUTBOUND folder name [${OUTBOUND}]: "
  read -r outbound
  OUTBOUND=${outbound:-"${OUTBOUND}"}
  if [[ ! ${OUTBOUND} =~ ^.*/$ ]]; then
    OUTBOUND="${OUTBOUND}/"
  fi
  printf '%s\n' "[OK] Continue with OUTBOUND as [${OUTBOUND}]"
  return 0
}

confirm_topic(){
  STEP=$((STEP+1))
  printf '%s\n' "STEP[${STEP}] Confirm Topic prefix for Pub/Sub..."
  PS_TOPIC=${PS_TOPIC:-"${PROJECT_NAME}"}
  printf '%s\n' "  Rationale: Tentacles uses Pub/Sub to manage the data flow. To avoid potential conflicts"
  printf '%s\n' "  with other applications' Topics, a unified prefix for Topics and Subscriptions will be used."
  printf '%s' "Input the prefix for Topics [${PS_TOPIC}]: "
  read -r topic
  PS_TOPIC=${topic:-"${PS_TOPIC}"}
  printf '%s\n' "[OK] Continue with Topic prefix as [${PS_TOPIC}]"
  return 0
}

save_config(){
  STEP=$((STEP+1))
  printf '%s\n' "STEP[${STEP}] Save the configuration..."
  JSON=$(cat <<-END
{
  "PROJECT_ID": "${GOOGLE_CLOUD_PROJECT}",
  "GCS_BUCKET": "${GCS_BUCKET}",
  "OUTBOUND": "${OUTBOUND}",
  "PS_TOPIC": "${PS_TOPIC}"
}
END
)
  JSON="$JSON"$'\n'
  printf '%s\n%s' "  Will save" " ${JSON}"
  printf '%s' "  to '${CONFIG_FILE}'. Confirm? [Y/n]"
  read -r input
  if [[ -z ${input} || ${input} = 'y' || ${input} = 'Y' ]];then
    printf '%s' "${JSON}" > "${CONFIG_FILE}"
    printf '%s\n' "[OK] Save to ${CONFIG_FILE}"
    return 0
  else
    printf '%s\n' "[User cancelled]";
    return 1
  fi
}

create_subscriptions(){
  STEP=$((STEP+1))
  printf '%s\n' "STEP[${STEP}] Create Topics and Subscriptions for Pub/Sub..."
  printf '%s\n' "  Rationale: Pub/sub subscription won't get the message before its creation. So we need to "
  printf '%s\n' "  create topics and subscriptions (placeholder) before real data comes."

  node -e "require('./index.js').initPubsub(process.argv[1], process.argv.slice(2))" "${PS_TOPIC}"  "${ENABLED_INTEGRATED_APIS[@]}"

  if [[ $? -gt 0 ]]; then
    printf '%s\n' "[Failed] Failed to create Topics or Subscriptions."
    return 1
  else
    printf '%s\n' "[OK] Successfully create Topics and Subscriptions."
    return 0
  fi
}

create_service_account(){
#  STEP=$((STEP+1))
#  printf '%s\n' "STEP[${STEP}] Create Service Account..."
  printf '%s\n' "  Rationale: Some external APIs may require authentication based on OAuth or JWT(service account)."
  printf '%s\n' "  For example, Google Analytics Data Import or Campaign Manager."
  printf '%s\n' "  In this step, we'll prepare the service account. For more information, see https://cloud.google.com/iam/docs/creating-managing-service-accounts"
  local suffix=$(get_sa_domain_from_gcp_id "${GOOGLE_CLOUD_PROJECT}")

  local email
  if [[ -f "${SA_KEY_FILE}" && -s "${SA_KEY_FILE}" ]]; then
    email=$(get_value_from_json_file "${SA_KEY_FILE}" 'client_email')
    if [[ ${email} =~ .*("@${suffix}") ]]; then
      printf '%s' "There is already a key file with service account[${email}]. Would you like to continue creating a new one? [N/y]"
      read -r input
      if [[ ${input} != 'y' && ${input} != 'Y' ]]; then
        printf '%s\n' "[OK] Will use existent service account [${email}]"
        SA_NAME=$(printf "${email}" | cut -d@ -f1)
        return 0
      fi
    fi
  fi

  SA_NAME="${SA_NAME:-"${DEFAULT_SERVICE_ACCOUNT}"}"
  while :; do
    printf '%s' "Input the name of service account [${SA_NAME}]: "
    read -r sa
    sa=${sa:-"${SA_NAME}"}
    if [[ ${sa} != *"@"* ]]; then
      email="${sa}@${suffix}"
      printf '%s\n' "  Appended default suffix to service account name and get: ${email}"
    else
      email=${sa}
    fi
    printf '%s' "Check the existence of the service account..."
    result=$(gcloud iam service-accounts describe "${email}" 2>&1)
    if [[ $? -gt 0 ]]; then
      printf '%s\n' "  not exist. Try to create..."
      gcloud iam service-accounts create "${sa}" --display-name "Tentacles API requester"
      if [[ $? -gt 0 ]]; then
        printf '%s\n' "Failed. Please try again..."
      else
        printf '%s\n' "Create successfully."
        SA_NAME=${sa}
        break
      fi
    else
      printf '%s\n' " found."
      SA_NAME=${sa}
      break
    fi
  done
  printf '%s\n' "[OK] Service Account [${SA_NAME}] is ready."
  return 0
}

download_service_account_key(){
  STEP=$((STEP+1))
  printf '%s\n' "STEP[${STEP}] Download key file of the Service Account ..."
  if [[ -z ${SA_NAME} ]];then
    create_service_account
  fi
  local prompt defaultValue email existentEmail
  local suffix=$(get_sa_domain_from_gcp_id "${GOOGLE_CLOUD_PROJECT}")
#  if [[ -z ${SA_NAME} ]];then
#    SA_NAME="${SA_NAME:-"${DEFAULT_SERVICE_ACCOUNT}"}"
#    printf '%s' "Input the name of service account [${SA_NAME}]: "
#    read -r sa
#    SA_NAME=${sa:-"${SA_NAME}"}
#  fi
  email="${SA_NAME}@${suffix}"
  if [[ -f "${SA_KEY_FILE}" && -s "${SA_KEY_FILE}" ]]; then
    existentEmail=$(get_value_from_json_file ${SA_KEY_FILE} 'client_email' 2>&1)
    if [[ ${existentEmail} =~ .*("@${suffix}") ]]; then
      printf '%s\n' "  There is already a service account key file for [${existentEmail}]"
      printf '%s\n' "  The key id is $(get_value_from_json_file ${SA_KEY_FILE} 'private_key_id')"
      prompt="Would you like to create a new one for [${email}] to overwrite it? [N/y] "
      defaultValue="n"
    else
      prompt="Would you like to download the key file for [${email}] and save it as ${SA_KEY_FILE}? [Y/n] "
      defaultValue="y"
    fi
  else
    prompt="Would you like to download the key file for [${email}] and save it as ${SA_KEY_FILE}? [Y/n] "
    defaultValue="y"
  fi
  printf '%s' "${prompt}"
  read -r input
  input=${input:-"${defaultValue}"}
  if [[ ${input} = 'y' || ${input} = 'Y' ]];then
    printf '%s\n' "Start to download a new key file for [${email}]..."
    gcloud iam service-accounts keys create "${SA_KEY_FILE}" --iam-account "${email}"
    if [[ $? -gt 0 ]]; then
      printf '%s\n' "[Failed] Failed to download new key files for [${email}]."
      return 1
    else
      printf '%s\n' "[OK] New key file is saved at [${SA_KEY_FILE}]."
      return 0
    fi
  else
    printf '%s\n' "[Skipped] To know more about service account key file, see: https://cloud.google.com/iam/docs/creating-managing-service-account-keys";
    return 0
  fi
}

deploy_tentacles(){
  STEP=$((STEP+1))
  printf '%s\n' "STEP[${STEP}] Start to deploy Tentacles..."
  printf '%s\n' "Tentacles is combined of three Cloud Functions."
  while [[ -z ${REGION} ]]; do
    set_region
  done
  printf '%s\n' "[ok] Will deploy Cloud Functions to ${REGION}."

  declare -a CF_FLAGS=()
  CF_FLAGS+=(--region="${REGION}")
  CF_FLAGS+=(--timeout=540 --memory=2048MB --runtime="${CF_RUNTIME}")
  CF_FLAGS+=(--set-env-vars=TENTACLES_TOPIC_PREFIX="${PS_TOPIC}",TENTACLES_OUTBOUND="${OUTBOUND}")

  printf '%s\n' " 1. '${PROJECT_NAME}_init' based on Cloud Storage bucket[${GCS_BUCKET}]."
  gcloud functions deploy "${PROJECT_NAME}"_init --entry-point initiate --trigger-bucket "${GCS_BUCKET}" "${CF_FLAGS[@]}"
  quit_if_failed $?

  printf '%s\n' " 2. '${PROJECT_NAME}_tran' based on Pub/Sub topic[${PS_TOPIC}-trigger]."
  gcloud functions deploy "${PROJECT_NAME}"_tran --entry-point transport --trigger-topic "${PS_TOPIC}"-trigger "${CF_FLAGS[@]}"
  quit_if_failed $?

  if [[ -f "${SA_KEY_FILE}" ]]; then
    CF_FLAGS+=(--set-env-vars=API_SERVICE_ACCOUNT="${SA_KEY_FILE}")
  fi
  printf '%s\n' " 3. '${PROJECT_NAME}_api' based on Pub/Sub topic[${PS_TOPIC}-push]."
  gcloud functions deploy "${PROJECT_NAME}"_api --entry-point requestApi --trigger-topic "${PS_TOPIC}"-push "${CF_FLAGS[@]}"
  quit_if_failed $?
}

post_installation(){
  STEP=$((STEP+1))
  printf '%s\n' "STEP[${STEP}] Post installation."
  check_firestore_existence
  printf '%s\n' "[ok] Firestore/Datastore is ready."
  if [[ ${NEED_SERVICE_ACCOUNT} = 'true' ]]; then
    existentEmail=$(get_value_from_json_file "${SA_KEY_FILE}" 'client_email')
    printf '%s\n' "Some enabled APIs require service account. This require extra step to grant access to the service account's email in external systems, e.g. Google Analytics or Campaign Manager."
    printf '%s\n' "This should be done before use Tentacles to send out data to them."
    printf '%s\n' "  1. For Google Analytics Data Import"
    printf '%s\n' "   * Set up Data set for Data Import, see: https://support.google.com/analytics/answer/3191417?hl=en"
    printf '%s\n' "   * Grant the 'Edit' access to [${existentEmail}]"
    printf '%s\n' "  2. For Campaign Manager"
    printf '%s\n' "   * DCM/DFA Reporting and Trafficking API's Conversions service, see: https://developers.google.com/doubleclick-advertisers/guides/conversions_overview"
    printf '%s\n' "   * Create User Profile for [${existentEmail}] and grant the access to 'Insert offline conversions'"
    printf '%s\n' "  3. For Google Ads conversions scheduled uploads based on Google Sheets"
    printf '%s\n' "   * Import conversions from ad clicks into Google Ads, see: https://support.google.com/google-ads/answer/7014069"
    printf '%s\n' "   * Add [${existentEmail}] as an Editor of the Google Spreadsheet that Google Ads will take conversions from."
  fi
  printf '\n'
  printf '%s\n' "Finally, follow the document (https://github.com/GoogleCloudPlatform/cloud-for-marketing/blob/master/marketing-analytics/activation/gmp-googleads-connector/README.md#4-api-details) to create configuration of the integration."
  printf '%s\n' "Save the configuration to a JSON file, e.g. './config_api.json' and run './deploy.sh update_api_config' to update the configuration to Firestore/Datastore, then Tentacles can use them."
}

print_finished(){
  printf '%s\n' "###########################################################"
  printf '%s\n' "##          Tentacles has been installed.                ##"
  printf '%s\n' "###########################################################"
}

update_api_config(){
  printf '%s\n' "=========================="
  printf '%s\n' "Update API configurations in into Firestore."
  check_firestore_existence
  if [[ ${CLOUD_SHELL} != 'true' ]] && [[ ! -f "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
    printf '%s\n' "Current shell is not in a Cloud Shell and there is no available key file, either. Quit now."
    printf '%s\n' "  Note: In local mode, 'GOOGLE_APPLICATION_CREDENTIALS' is expected in environment variables."
    return 1
  fi
  printf '%s' "Please input the configuration file [./config_api.json]:"
  read -r app_config
  app_config=${app_config:-"./config_api.json"}
  printf '\n'
  node -e "require('./index.js').uploadApiConfig(require(process.argv[1]))" "${app_config}"
}

copy_file_to_gcs(){
  printf '%s\n' "=========================="
  target="gs://$(get_value_from_json_file "${CONFIG_FILE}" "GCS_BUCKET")/$(get_value_from_json_file "${CONFIG_FILE}" "OUTBOUND")"
  printf '%s\n' "Copy integration data file to target folder in Google Cloud Storage: ${target}"
  # gsutil support wildcard name. Use '*' to replace '[' here.
  source="$(printf '%s' "$1" | sed -r 's/\[/\*/g' )"
  gsutil cp ''"${source}"'' "${target}"
}

print_service_account(){
  printf '%s\n' "$(get_value_from_json_file "${SA_KEY_FILE}" 'client_email')"
}

install_tentacles(){

  print_welcome

  # step 0
  check_in_cloud_shell
  quit_if_failed $?

  # step 1
  prepare_dependencies
  quit_if_failed $?

  # step 2
  confirm_project
  quit_if_failed $?

  # step 3
  confirm_region
  quit_if_failed $?

  # step 4
  confirm_apis
  quit_if_failed $?

  # step 5
  check_permissions
  quit_if_failed $?

  # step 6
  enable_apis
  quit_if_failed $?

  # step 7
  create_bucket
  quit_if_failed $?

  # step 8
  confirm_folder
  quit_if_failed $?

  # step 9
  confirm_topic
  quit_if_failed $?

  # step 10
  save_config
  quit_if_failed $?

  # step 11
  create_subscriptions
  quit_if_failed $?


  # step 12
  if [[ ${NEED_SERVICE_ACCOUNT} = 'true' ]]; then
    download_service_account_key
    quit_if_failed $?
  fi

  # step 13
  deploy_tentacles

  # step 14
  post_installation

  print_finished
}

run_local_test(){
 printf '%s\n' "Invoke API based on local file and cloud-based config"
 DEBUG=true node -e "require('./index.js').localApiRequester(process.argv[1])" "$@"
}

if [[ -z "$1" ]]; then
  install_tentacles
else
  printf '%s\n' "Running single task [$*], need to set variables..."
  emulate_cloud_shell
  load_config
  "$@"
fi
