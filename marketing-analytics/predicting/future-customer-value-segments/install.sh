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

set -e

CREATE_BUCKET="false"
INSTALL_DEPENDENCIES="true"
GCS_LOCATION="us"
DATAFLOW_REGION="us-central1"

output_help_text() {
  cat <<HELP
Usage:
install.sh  [-h] -b=<GCS_BUCKET> [-l=<GCS_LOCATION>] [-r=<DATAFLOW_REGION>]
            [--create_bucket] [--no_install_dependencies]
Options:
-h, --help            show this help message and exit
-b=<GCS_BUCKET>, --bucket=<GCS_BUCKET>
                      name of the Google Cloud Storage bucket to use
-l=<GCS_LOCATION>, --location=<GCS_LOCATION>
                      location to use for the GCS bucket. E.g. "us" or "EU".
                      Refer to https://cloud.google.com/storage/docs/locations
                      for more information
-r=<DATAFLOW_REGION>, --region=<DATAFLOW_REGION>
                      region to use for the Dataflow runtime. E.g. "us-central1"
                      or "EUROPE-WEST3". Refer to
                      https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
                      for more information
-c, --create_bucket   create the GCS bucket specified by <GCS_BUCKET> at the
                      location specifed by <GCS_LOCATION>. Outputs a warning if
                      the bucket already exists
-n, --no_install_dependencies
                      skip python dependencies installation and reuse the local
                      virtual environment named 'focvs-env' that was created by
                      a previous run of this installation script. This option is
                      ignored if a 'focvs-env' local virtual environment could
                      not be found
HELP
  exit 1
}

install_dependencies() {
  if ! "${INSTALL_DEPENDENCIES}" && [ -d "focvs-env" ]; then
    printf "\nINFO - Reusing existing virtualenv 'focvs-env'\n"
    source focvs-env/bin/activate
  else
    printf "\nINFO - Installing Python dependencies...\n\n"
    rm -rf focvs-env
    virtualenv focvs-env
    source focvs-env/bin/activate
    pip install -r requirements.txt
    printf "\nINFO - Python dependencies installed successfully!\n"
  fi
}

create_gcs_bucket() {
  if "${CREATE_BUCKET}"; then
    BUCKET_EXISTS=$(gsutil ls gs://${GCS_BUCKET} > /dev/null 2>&1 && echo "true" || echo "false")

    if "${BUCKET_EXISTS}"; then
      printf "\nWARN - Bucket ${GCS_BUCKET} already exists, ignoring option --create_bucket\n"
    else
      printf "\nINFO - Creating bucket ${GCS_BUCKET} in location ${GCS_LOCATION}...\n\n"
      gsutil mb -l ${GCS_LOCATION} gs://${GCS_BUCKET}
      printf "\nINFO - Bucket ${GCS_BUCKET} created successfully!\n"
    fi
  fi
}

install_dataflow_templates() {
  printf "\nINFO - Creating FoCVS Dataflow environment and templates in region ${DATAFLOW_REGION}...\n\n"
  python fcvs_pipeline_csv.py \
    --runner DataflowRunner \
    --project "${DEVSHELL_PROJECT_ID}" \
    --temp_location "gs://${GCS_BUCKET}/temp/" \
    --staging_location "gs://${GCS_BUCKET}/staging/" \
    --template_location "gs://${GCS_BUCKET}/templates/FoCVS-csv" \
    --save_main_session \
    --setup_file ./setup.py \
    --region "${DATAFLOW_REGION}"

  python fcvs_pipeline_bq.py \
    --runner DataflowRunner \
    --project "${DEVSHELL_PROJECT_ID}" \
    --temp_location "gs://${GCS_BUCKET}/temp/" \
    --staging_location "gs://${GCS_BUCKET}/staging/" \
    --template_location "gs://${GCS_BUCKET}/templates/FoCVS-bq" \
    --save_main_session \
    --experiments=use_beam_bq_sink \
    --setup_file ./setup.py \
    --region "${DATAFLOW_REGION}"

  gsutil cp FoCVS-*_metadata gs://${GCS_BUCKET}/templates

  deactivate
  printf "\nINFO - FoCVS Dataflow environment and templates created successfully!\n"
}

for i in "$@"; do
  case $i in
    -b=*|--bucket=*)
      GCS_BUCKET="${i#*=}"
      shift
      ;;
    -l=*|--location=*)
      GCS_LOCATION="${i#*=}"
      shift
      ;;
    -r=*|--region=*)
      DATAFLOW_REGION="${i#*=}"
      shift
      ;;
    -c|--create_bucket)
      CREATE_BUCKET="true"
      shift
      ;;
    -n|--no_install_dependencies)
      INSTALL_DEPENDENCIES="false"
      shift
      ;;
    -h|--help)
      output_help_text
      shift
      ;;
    *)
      ;;
  esac
done

if [ -z "${GCS_BUCKET}" ]; then
  >&2 printf "ERROR - Missing Cloud Storage bucket name argument.\n"
  >&2 printf "Usage: $0 (-b|--bucket)=<GCS_BUCKET>\n"
  exit 1
fi

if [ -z "${DEVSHELL_PROJECT_ID}" ]; then
  >&2 printf "ERROR - GCP project configuration property is not set.\n"
  >&2 printf "Please execute 'gcloud config set project <PROJECT_ID>' before running this script.\n"
  exit 1
fi

printf "\nBEGIN - Starting FoCVS installation...\n"

install_dependencies
create_gcs_bucket
install_dataflow_templates

printf "\nEND - FoCVS installation completed successfully!\n"
