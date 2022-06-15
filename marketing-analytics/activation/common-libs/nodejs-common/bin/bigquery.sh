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

#######################################
# Checks whether the BigQuery object (table or view) exists.
# Globals:
#  GCP_PROJECT
#  DATASET
#  BIGQUERY_LOG_TABLE
# Arguments:
#  None
#######################################
check_existence_in_bigquery() {
  bq show "${1}" >/dev/null 2>&1
  printf '%d' $?
}

#######################################
# Creates or updates the BigQuery view.
# Globals:
#  GCP_PROJECT
#  DATASET
# Arguments:
#  The name of view.
#  The query of view.
#######################################
create_or_update_view() {
  local viewName viewQuery
  viewName="${1}"
  viewQuery=${2}
  local action="mk"
  if [[ $(check_existence_in_bigquery "${DATASET}.${viewName}") -eq 0 ]]; then
    action="update"
  fi
  bq "${action}" \
    --use_legacy_sql=false \
    --view "${viewQuery}" \
    --project_id ${GCP_PROJECT} \
    "${DATASET}.${viewName}"
}
