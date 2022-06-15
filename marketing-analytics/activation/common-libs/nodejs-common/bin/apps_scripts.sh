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

# Const the folder name of Apps Script.
DEFAULT_APPS_SCRIPT_FOLDER="apps_script"

#######################################
# Clasp login.
# Globals:
#   None
# Arguments:
#   None
#######################################
clasp_login() {
  while :; do
    local claspLogin=$(clasp login --status)
    if [[ "${claspLogin}" != "You are not logged in." ]]; then
      printf '%s' "${claspLogin} Would you like to continue with it? [Y/n]"
      local logout
      read -r logout
      logout=${logout:-"Y"}
      if [[ ${logout} == "Y" || ${logout} == "y" ]]; then
        break
      else
        clasp logout
      fi
    fi
    clasp login --no-localhost
  done
}

#######################################
# Initialize a AppsScript project. Usually it involves following steps:
# 1. Create a AppsScript project within a new Google Sheet.
# 2. Prompt to update the Google Cloud Project number of the AppsScript project
#    to enable external APIs for this AppsScript project.
# 3. Prompt to grant the access of Cloud Functions' default service account to
#    this Google Sheet, so the Cloud Functions can query this Sheet later.
# 4. Initialize the Sheet based on requests.
# Globals:
#   None
# Arguments:
#   The Google Sheet name.
#   The folder for Apps Script code, default value ${DEFAULT_APPS_SCRIPT_FOLDER}
#######################################
clasp_initialize() {
  ((STEP += 1))
  printf '%s\n' "Step ${STEP}: Starting to create Google Sheets..."
  local sheetName="${1}"
  local apps_script_src="${2-"${DEFAULT_APPS_SCRIPT_FOLDER}"}"
  clasp_login
  while :; do
    local claspStatus=$(
      clasp status -P "${apps_script_src}" >/dev/null 2>&1
      echo $?
    )
    if [[ $claspStatus -gt 0 ]]; then
      clasp create --type sheets --title "${sheetName}" --rootDir "${apps_script_src}"
      local createResult=$?
      if [[ $createResult -gt 0 ]]; then
        printf '%s' "Press any key to continue after you enable the Google \
Apps Script API: https://script.google.com/home/usersettings..."
        local any
        read -n1 -s any
        printf '\n\n'
        continue
      fi
      break
    else
      printf '%s' "AppsScript project exists. Would you like to continue with \
it? [Y/n]"
      local useCurrent
      read -r useCurrent
      useCurrent=${useCurrent:-"Y"}
      if [[ ${useCurrent} = "Y" || ${useCurrent} = "y" ]]; then
        break
      else
        printf '%s' "Would you like to delete current AppsScript and create a \
new one? [N/y]"
        local deleteCurrent
        read -r deleteCurrent
        deleteCurrent=${deleteCurrent:-"N"}
        if [[ ${deleteCurrent} = "Y" || ${deleteCurrent} = "y" ]]; then
          rm "${apps_script_src}/.clasp.json"
          continue
        fi
      fi
    fi
  done
}

#######################################
# Copy GCP project configuration file to AppsScript codes as a constant named
# `GCP_CONFIG`.
# Globals:
#   None
# Arguments:
#   The folder for Apps Script code, default value ${DEFAULT_APPS_SCRIPT_FOLDER}
#######################################
generate_config_js_for_apps_script() {
  local apps_script_src="${1-"${DEFAULT_APPS_SCRIPT_FOLDER}"}"
  local generated_file="${apps_script_src}/.generated_config.js"
  if [[ -f "${CONFIG_FILE}" ]]; then
    echo '// Copyright 2022 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/** @fileoverview Auto-generated configuration file for Apps Script. */
'>"${generated_file}"
    echo -n "const GCP_CONFIG = " >>"${generated_file}"
    cat "${CONFIG_FILE}" >>"${generated_file}"
  else
    printf '%s\n' "Couldn't find ${CONFIG_FILE}."
  fi
}

#######################################
# Clasp pushes AppsScript codes.
# Globals:
#   None
# Arguments:
#   The folder for Apps Script code, default value ${DEFAULT_APPS_SCRIPT_FOLDER}
#######################################
clasp_push_codes() {
  ((STEP += 1))
  printf '%s\n' "Step ${STEP}: Starting to push codes to the Google Sheets..."
  local apps_script_src="${1-"${DEFAULT_APPS_SCRIPT_FOLDER}"}"
  clasp status -P "${apps_script_src}" >>/dev/null
  local project_status=$?
  if [[ ${project_status} -gt 0 ]]; then
    return ${project_status}
  else
    generate_config_js_for_apps_script "${apps_script_src}"
    clasp push --force -P "${apps_script_src}"
  fi
}

#######################################
# Ask user to update the GCP number of this AppsScript.
# Globals:
#   GCP_PROJECT
# Arguments:
#   The folder for Apps Script code, default value ${DEFAULT_APPS_SCRIPT_FOLDER}
#######################################
clasp_update_project_number() {
  ((STEP += 1))
  local projectNumber=$(get_project_number)
  local apps_script_src="${1-"${DEFAULT_APPS_SCRIPT_FOLDER}"}"
  printf '%s\n' "Step ${STEP}: Update Google Cloud Platform (GCP) Project for \
Apps Script."
  printf '%s' "  "
  clasp open -P "${apps_script_src}"
  printf '%s\n' "  On the open tab of Apps Script, use 'Project \
Settings' to set the Google Cloud Platform (GCP) Project as: ${projectNumber}"
  printf '%s' "Press any key to continue after you update the GCP number..."
  local any
  read -n1 -s any
  printf '\n'
}

#######################################
# Ask user to grant the access to CF's default service account.
# Note: the target GCP needs to have OAuth consent screen.
# Globals:
#   SHEET_URL
# Arguments:
#   The folder for Apps Script code, default value ${DEFAULT_APPS_SCRIPT_FOLDER}
#######################################
grant_access_to_service_account() {
  ((STEP += 1))
  local apps_script_src="${1-"${DEFAULT_APPS_SCRIPT_FOLDER}"}"
  local defaultServiceAccount=$(get_cloud_functions_service_account \
    "${PROJECT_NAMESPACE}_main")
  local parentId=$(get_value_from_json_file "${apps_script_src}"/.clasp.json \
    parentId|cut -d\" -f2)
  printf '%s\n' "Step ${STEP}: Share the Google Sheet with ${SOLUTION_NAME}."

  printf '%s\n' "  Open Google Sheet: \
https://drive.google.com/open?id=${parentId}"
  printf '%s\n' "  Click 'Share' and grant the Viewer access to: \
${defaultServiceAccount}"
  printf '%s' "Press any key to continue after you grant the access..."
  local any
  read -n1 -s any
  printf '\n'
}
