#!/usr/bin/env bash
# Copyright 2021 Google LLC
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
 
# Usage: ./install.sh GCS_BUCKET_NAME
GCS_BUCKET_NAME=$1
DATAFLOW_DEFAULT_REGION="us-central-1"

if [ -z "${GCS_BUCKET_NAME}" ]
  then
    >&2 printf "ERROR - Missing Cloud Storage bucket name argument.\n"
    >&2 printf "Usage: $0 <GCS_BUCKET_NAME>\n"
    exit 1
fi

if [ -z "${DEVSHELL_PROJECT_ID}" ]
  then
    >&2 printf "ERROR - GCP project configuration property is not set.\n"
    >&2 printf "Please execute 'gcloud config set project <PROJECT_ID>' before running this script.\n"
    exit 1
fi

printf "\nBEGIN - Starting FoCVS installation...\n"

printf "\nINFO - Installing Python dependencies...\n\n"
rm -rf focvs-env
virtualenv focvs-env
source focvs-env/bin/activate
pip install -r requirements.txt
printf "\nINFO - Python dependencies installed successfully!\n"

printf "\nINFO - Creating FoCVS Dataflow environment and templates...\n\n"
python fcvs_pipeline_csv.py \
  --runner DataflowRunner \
  --project "${DEVSHELL_PROJECT_ID}" \
  --temp_location "gs://${GCS_BUCKET_NAME}/temp/" \
  --staging_location "gs://${GCS_BUCKET_NAME}/staging/" \
  --template_location "gs://${GCS_BUCKET_NAME}/templates/FoCVS-csv" \
  --save_main_session \
  --setup_file ./setup.py \
  --region "${DATAFLOW_DEFAULT_REGION}"

python fcvs_pipeline_bq.py \
  --runner DataflowRunner \
  --project "${DEVSHELL_PROJECT_ID}" \
  --temp_location "gs://${GCS_BUCKET_NAME}/temp/" \
  --staging_location "gs://${GCS_BUCKET_NAME}/staging/" \
  --template_location "gs://${GCS_BUCKET_NAME}/templates/FoCVS-bq" \
  --save_main_session \
  --experiments=use_beam_bq_sink \
  --setup_file ./setup.py \
  --region "${DATAFLOW_DEFAULT_REGION}"

gsutil cp FoCVS-*_metadata gs://${GCS_BUCKET_NAME}/templates

deactivate
printf "\nINFO - FoCVS Dataflow environment and templates created successfully!\n"

printf "\nEND - FoCVS installation completed successfully!\n"
