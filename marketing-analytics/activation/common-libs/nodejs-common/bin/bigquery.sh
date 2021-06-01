#!/usr/bin/env bash
#
# Copyright 2021 Google Inc.
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

#######################################
# Get the location of the given BigQuery dataset. If the dataset doesn't exit,
# it returns an empty string.
# Globals:
#   None
# Arguments:
#   Dataset name
# Returns:
#   Dataset location
#######################################
get_dataset_location() {
  local dataset_json
  dataset_json=$(bq --format json --dataset_id "$1" show 2>&1)
  if [[ "${dataset_json}" != *"Not found"* ]]; then
    local location
    location=$(node -e "console.log(JSON.parse('${dataset_json}').location)")
    printf '%s' "${location}"
  fi
}

#######################################
# Confirm or create a Cloud Storage bucket with a given location.
# Globals:
#   GCP_PROJECT
# Arguments:
#   Dataset name
#   Location
#######################################
confirm_dataset_with_location(){
  local datasetName=$1
  local location=$2
  local defaultValue="${!datasetName}"
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Checking or creating a BigQuery dataset in \
location [${location}] ..."
  while :; do
    printf '%s' "Enter the name of your dataset [${defaultValue}]: "
    local dataset
    read -r dataset
    dataset=${dataset:-$defaultValue}
    local result
    result=$(get_dataset_location "${dataset}")
    if [[ "${result}" == "${location}" ]]; then
      printf '%s\n' "OK. The dataset [${dataset}] exists in the location \
[${location}] ."
      break
    elif [[ -n "${result}" ]]; then
      printf '%s\n' "  The dataset [${dataset}]'s location is [${result}]. \
Continuing to enter another dataset..."
      continue
    else
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
  declare -g "${datasetName}_LOCATION=${location}"
  printf '\n'
}

#######################################
# Confirm or create a BigQuery dataset.
# Globals:
#   DATASET
#   GCP_PROJECT
#   DATASET_LOCATION
# Arguments:
#   None
#######################################
create_dataset() {
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
    "Data centers within member states of the European Union (EU)"
    "Data centers in the United States (US)")
  local AMERICAS=(
    "Iowa (us-central1)"
    "Las Vegas (us-west4)"
    "Los Angeles (us-west2)"
    "Montréal (northamerica-northeast1)"
    "Northern Virginia (us-east4)"
    "Oregon (us-west1)"
    "Salt Lake City (us-west3)"
    "São Paulo (southamerica-east1)"
    "South Carolina (us-east1)"
  )
  local EUROPE=(
    "Belgium (europe-west1)"
    "Finland (europe-north1)"
    "Frankfurt (europe-west3)"
    "London (europe-west2)"
    "Netherlands (europe-west4)"
    "Warsaw (europe-central2)"
    "Zürich (europe-west6)"
  )
  local ASIA_PACIFIC=(
    "Hong Kong (asia-east2)"
    "Jakarta (asia-southeast2)"
    "Mumbai (asia-south1)"
    "Osaka (asia-northeast2)"
    "Seoul (asia-northeast3)"
    "Singapore (asia-northeast1)"
    "Sydney (australia-southeast1)"
    "Taiwan (asia-east1)"
    "Tokyo (asia-northeast1)"
  )

  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Entering a Bigquery dataset name..."
  printf '%s\n' "  You can use an existing dataset or create a new one here."

  local datasetVariableName defaultValue defaultLocation
  datasetVariableName="${1:-"DATASET"}"
  defaultValue="${!datasetVariableName}"
  defaultLocation=$2

  while :; do
    printf '%s' "Enter the name of BigQuery dataset [${defaultValue}]: "
    local dataset
    read -r dataset
    dataset=${dataset:-$defaultValue}
    # Check whether dataset exists
    bq ls "${dataset}" 2>&1 > /dev/null
    if [[ $? -eq 0 ]]; then
      local location
      location=$(get_dataset_location "${dataset}")
      if [[ -n ${defaultLocation} && ${defaultLocation} != ${location} ]]; then
        printf '%s\n' "The dataset [${dataset}] exists in location \
[${location}], not the input location [${defaultLocation}]. Try another name..."
        continue
      fi
      declare -g "${datasetVariableName}=${dataset}"
      declare -g "${datasetVariableName}_LOCATION=${location}"
      printf '%s\n' "  The dataset [${dataset}] exists in your current \
project [${GCP_PROJECT}] with location [${location}]."
      break
    fi
    printf '%s\n' "  [${dataset}] doesn't exist. Continuing to create the \
dataset..."
    # Use existent DATASET_LOCATION to create dataset
    if [[ -n ${defaultLocation} ]]; then
      printf '%s' "Would you like to create the BigQuery dataset [${dataset}] \
in location ['${defaultLocation}'] ? [Y/n]: "
      local useLocation
      read -r useLocation
      useLocation=${useLocation:-"Y"}
      if [[ ${useLocation} = "Y" || ${useLocation} = "y" ]]; then
        printf '%s\n' "  Attempting to create the dataset [${dataset}] in \
location ['${defaultLocation}'] ..."
        bq --location="${DATASET_LOCATION}" mk "${dataset}"
        if [[ $? -gt 0 ]]; then
          printf '%s\n' "Failed to create the dataset [${dataset}]. Try again."
          continue
        else
          declare -g "${datasetVariableName}=${dataset}"
          declare -g "${datasetVariableName}_LOCATION=${defaultLocation}"
          break
        fi
      fi
    fi
    printf '%s\n' "Select the region of the dataset location:"
    # Select dataset location.
    local location locationType
    select location in "${DATASET_REGIONS[@]}"; do
      if [[ -n "${location}" ]]; then
        if [[ $REPLY = 1 ]]; then
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
            printf '%s\n' "  Attempting to create the dataset [${dataset}] in \
${selectedLocation}..."
            local location
            location=$(echo "${selectedLocation}" | cut -d\( -f2 | cut -d\) -f1)
            bq --location="${location}" mk "${dataset}"
            if [[ $? -gt 0 ]]; then
              printf '%s\n' "Failed to create the dataset [${dataset}]. Try \
again."
              break 2
            else
              declare -g "${datasetVariableName}=${dataset}"
              declare -g "${datasetVariableName}_LOCATION=${location}"
              break 3
            fi
          elif [[ ${REPLY} = 0 ]]; then
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
  done
  printf '%s\n\n' "OK. This solution will use the dataset [${dataset}]."
}
