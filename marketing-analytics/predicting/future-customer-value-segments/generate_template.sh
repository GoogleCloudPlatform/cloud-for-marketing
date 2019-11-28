#!/usr/bin/env bash

python dataflow.py \
  --runner DataflowRunner \
  --project "${DEVSHELL_PROJECT_ID}" \
  --temp_location "gs://${PIPELINE_BUCKET}/temp/" \
  --staging_location "gs://${PIPELINE_BUCKET}/staging/" \
  --template_location "gs://${PIPELINE_BUCKET}/templates/Future-Customer-Value-Segments" \
  --save_main_session \
  --setup_file ./setup.py
