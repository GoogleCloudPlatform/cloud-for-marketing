#!/usr/bin/env bash
# Copyright 2019 Google LLC
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


python dataflow.py \
  --runner DataflowRunner \
  --project "${DEVSHELL_PROJECT_ID}" \
  --temp_location "gs://${PIPELINE_BUCKET}/temp/" \
  --staging_location "gs://${PIPELINE_BUCKET}/staging/" \
  --template_location "gs://${PIPELINE_BUCKET}/templates/Future-Customer-Value-Segments" \
  --save_main_session \
  --setup_file ./setup.py
