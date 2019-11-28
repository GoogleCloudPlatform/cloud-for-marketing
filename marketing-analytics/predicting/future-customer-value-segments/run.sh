#!/usr/bin/env bash

if [ -d "./output" ]
then
  rm -fr ./output
fi
mkdir ./output

python dataflow.py --runner=DirectRunner \
  --input_csv ./input_cdnow.csv \
  --output_folder ./output/ \
  --customer_id_column_position 1 \
  --transaction_date_column_position 2 \
  --sales_column_position 4 \
  --date_parsing_pattern YYYY-MM-DD \
  --model_time_granularity weekly \
  --penalizer_coef 0.0 \
  --extra_dimension_column_position 3

## Output to Cloud Storage
#  --output_folder gs://${DEVSHELL_PROJECT_ID}/output/ \
