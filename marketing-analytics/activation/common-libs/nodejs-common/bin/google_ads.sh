#!/usr/bin/env bash
#
# Copyright 2022 Google Inc.
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

# Google Ads API version
GOOGLE_ADS_API_VERSION=10

#######################################
# Verify whether the current OAuth token, CID and developer token can work.
# Globals:
#   None
# Arguments:
#   MCC CID
#   Developer token
#######################################
validate_googleads_account() {
  local cid developerToken accessToken request response
  cid=${1}
  developerToken=${2}
  accessToken=$(get_oauth_access_token)
  request=(
    -H "Accept: application/json"
    -H "Content-Type: application/json"
    -H "developer-token: ${developerToken}"
    -H "Authorization: Bearer ${accessToken}"
    -X POST "https://googleads.googleapis.com/v${GOOGLE_ADS_API_VERSION}/customers/${cid}/googleAds:search"
    -d '{"query": "SELECT customer.id FROM customer"}'
  )
  response=$(curl "${request[@]}" 2>/dev/null)
  local errorCode errorMessage
  errorCode=$(get_value_from_json_string "${response}" "error.code")
  if [[ -n "${errorCode}" ]]; then
    errorMessage=$(get_value_from_json_string "${response}" "error.message")
    printf '%s\n' "Validate failed: ${errorMessage}" >&2
    return 1
  fi
  return 0
}

#######################################
# Let user input MCC CID and developer token for cronjob(s).
# Globals:
#   MCC_CIDS
#   DEVELOPER_TOKEN
# Arguments:
#   None
#######################################
set_google_ads_account() {
  printf '%s\n' "Setting up Google Ads account information..."
  local developToken mccCids
  while :; do
    # Developer token
    while [[ -z ${developToken} ]]; do
      printf '%s' "  Enter the developer token[${DEVELOPER_TOKEN}]: "
      read -r input
      developToken="${input:-${DEVELOPER_TOKEN}}"
    done
    DEVELOPER_TOKEN="${developToken}"
    mccCids=""
    # MCC CIDs
    while :; do
      printf '%s' "  Enter the MCC CID: "
      read -r input
      if [[ -z ${input} ]]; then
        continue
      fi
      input="$(printf '%s' "${input}" | sed -r 's/-//g')"
      printf '%s' "    validating ${input}...... "
      validate_googleads_account ${input} ${DEVELOPER_TOKEN}
      if [[ $? -eq 1 ]]; then
        printf '%s\n' "failed.
      Press 'd' to re-enter developer token ["${developToken}"] or
            'C' to continue with this MCC CID or
            any other key to enter another MCC CID..."
        local any
        read -n1 -s any
        if [[ "${any}" == "d" ]]; then
          developToken=""
          continue 2
        elif [[ "${any}" == "C" ]]; then
          printf '%s\n' "WARNING! Continue with FAILED MCC ${input}."
        else
          continue
        fi
      else
        printf '%s\n' "succeeded."
      fi
      mccCids+=",${input}"
      printf '%s' "  Do you want to add another MCC CID? [Y/n]: "
      read -r input
      if [[ ${input} == 'n' || ${input} == 'N' ]]; then
        break
      fi
    done
    # Left Shift one position to remove the first comma.
    # After shifting, MCC_CIDS would like "11111,22222".
    MCC_CIDS="${mccCids:1}"
    printf '%s\n' "Using Google Ads MCC CIDs: ${MCC_CIDS}."
    break
  done
}

printf '%s\n' "Google Ads Bash Library is loaded."
