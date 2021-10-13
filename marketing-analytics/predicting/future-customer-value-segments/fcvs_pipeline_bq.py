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

import argparse
import logging
import operator

import apache_beam as beam
from apache_beam import io
from apache_beam import pvalue
from apache_beam.options import pipeline_options
from apache_beam.options import value_provider
from apache_beam.transforms import util

import bigquery_mod as bq_mod
import common as c


class RuntimeOptions(pipeline_options.PipelineOptions):
    """Specifies runtime options for the pipeline.

    Class defining the arguments that can be passed to the pipeline to
    customize the execution.
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(f'--{c._OPTION_INPUT_BQ_QUERY}')
        parser.add_value_provider_argument(f'--{c._OPTION_INPUT_BQ_PROJECT}')
        parser.add_value_provider_argument(f'--{c._OPTION_TEMP_GCS_LOCATION}')
        parser.add_value_provider_argument(f'--{c._OPTION_OUTPUT_FOLDER}')
        parser.add_value_provider_argument(f'--{c._OPTION_OUTPUT_BQ_PROJECT}')
        parser.add_value_provider_argument(f'--{c._OPTION_OUTPUT_BQ_DATASET}')
        parser.add_value_provider_argument(
            f'--{c._OPTION_CUSTOMER_ID_COLUMN_NAME}')
        parser.add_value_provider_argument(
            f'--{c._OPTION_TRANSACTION_DATE_COLUMN_NAME}')
        parser.add_value_provider_argument(
            f'--{c._OPTION_SALES_COLUMN_NAME}')
        parser.add_value_provider_argument(
            f'--{c._OPTION_EXTRA_DIMENSION_COLUMN_NAME}')
        parser.add_value_provider_argument(f'--{c._OPTION_DATE_PARSING_PATTERN}')
        parser.add_value_provider_argument(
            f'--{c._OPTION_MODEL_TIME_GRANULARITY}',
            default=c.TimeGranularityParams.GRANULARITY_WEEKLY)
        parser.add_value_provider_argument(
            f'--{c._OPTION_FREQUENCY_MODEL_TYPE}', default=c._MODEL_TYPE_MBGNBD)
        parser.add_value_provider_argument(
            f'--{c._OPTION_CALIBRATION_START_DATE}')
        parser.add_value_provider_argument(f'--{c._OPTION_CALIBRATION_END_DATE}')
        parser.add_value_provider_argument(f'--{c._OPTION_COHORT_START_DATE}')
        parser.add_value_provider_argument(f'--{c._OPTION_COHORT_END_DATE}')
        parser.add_value_provider_argument(f'--{c._OPTION_HOLDOUT_END_DATE}')
        parser.add_value_provider_argument(
            f'--{c._OPTION_PREDICTION_PERIOD}', default=52, type=int)
        parser.add_value_provider_argument(
            f'--{c._OPTION_OUTPUT_SEGMENTS}', default=5, type=int)
        parser.add_value_provider_argument(
            f'--{c._OPTION_TRANSACTION_FREQUENCY_THRESHOLD}', default=15,
            type=int)
        parser.add_value_provider_argument(
            f'--{c._OPTION_PENALIZER_COEF}', default=0.0, type=float)
        parser.add_value_provider_argument(
            f'--{c._OPTION_ROUND_NUMBERS}', default="False")


def run(argv=None):
    """Main function.

    Main function containing the Apache Beam pipeline describing how to process
    the input CSV file to generate the LTV predictions.
    """
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args(argv)
    options = pipeline_options.PipelineOptions(pipeline_args)
    runtime_options = options.view_as(RuntimeOptions)

    with beam.Pipeline(options=options) as pipeline:
        options = (
            pipeline
            | 'Create single element Stream containing options dict' >>
                beam.Create([options.get_all_options()])
            | beam.Map(lambda x: {
                  k: v.get() if isinstance(v, value_provider.ValueProvider)
                             else v
                  for (k, v) in x.items()
              })
            | beam.Map(c.set_extra_options)
        )

        full_elog = (
            pipeline
            | bq_mod.ReadFromBigQuery(
                project=getattr(runtime_options, c._OPTION_INPUT_BQ_PROJECT),
                query=getattr(runtime_options, c._OPTION_INPUT_BQ_QUERY),
                gcs_location=getattr(runtime_options, c._OPTION_TEMP_GCS_LOCATION),
                use_standard_sql=True
            )
            | beam.FlatMap(
                c.bq_row_to_list,
                pvalue.AsSingleton(options))  # (customer_id, date_str, date,
                                              #  sales, extra_dimension?)
        )

        full_elog_merged = (
            full_elog
            | beam.Filter(lambda x: x[3] > 0)  # sales > 0
            | beam.Map(lambda x: ((x[0], x[1]), x))  # key: (customer_id, date)
            | 'Group full elog by customer and date' >> beam.GroupByKey()
            | beam.Map(c.merge_full_elog_by_customer_and_date)  # (customer_id,
                                                                #  date_str, date,
                                                                #  sales)
        )

        min_max_dates = (
            full_elog_merged
            | beam.Map(lambda x: x[2])  # date
            | beam.CombineGlobally(c.MinMaxDatesFn())
            | beam.Map(c.min_max_dates_dict)
        )

        limits_dates = (
            min_max_dates
            | beam.FlatMap(c.limit_dates_boundaries, pvalue.AsSingleton(options))
        )

        cohort = (
            full_elog_merged
            | beam.FlatMap(c.filter_customers_in_cohort,
                           pvalue.AsSingleton(limits_dates))
            | 'Distinct Customer IDs in Cohort' >> util.Distinct()
        )

        cohort_count = (
            cohort
            | 'Count cohort entries' >> beam.combiners.Count.Globally()
        )

        cohort_set = (
            cohort
            | beam.Map(lambda x: (x, 1))
        )

        all_customer_ids = (
            full_elog_merged
            | beam.Map(lambda x: x[0])  # key: customer_id
            | 'Distinct all Customer IDs' >> util.Distinct()
        )

        all_customer_ids_count = (
            all_customer_ids
            | 'Count all customers' >> beam.combiners.Count.Globally()
        )

        num_customers = (
            pipeline
            | 'Create single elem Stream I' >> beam.Create([1])
            | beam.FlatMap(c.count_customers,
                           pvalue.AsSingleton(cohort_count),
                           pvalue.AsSingleton(all_customer_ids_count),
                           pvalue.AsSingleton(options))
        )

        cal_hol_elog = (
            full_elog_merged
            | beam.FlatMap(c.filter_cohort_records_in_cal_hol,
                           pvalue.AsDict(cohort_set),
                           pvalue.AsSingleton(limits_dates))
        )

        cal_hol_elog_count = (
            cal_hol_elog
            | 'Count cal hol elog entries' >> beam.combiners.Count.Globally()
        )

        calibration = (
            cal_hol_elog
            | beam.FlatMap(c.filter_records_in_calibration,
                           pvalue.AsSingleton(limits_dates))
        )

        num_txns_total = (
            full_elog_merged
            | beam.FlatMap(c.filter_records_in_cal_hol,
                           pvalue.AsSingleton(limits_dates))
            | 'Count num txns total' >> beam.combiners.Count.Globally()
        )

        num_txns = (
            pipeline
            | 'Create single elem Stream II' >> beam.Create([1])
            | beam.FlatMap(c.count_txns,
                           pvalue.AsSingleton(cal_hol_elog_count),
                           pvalue.AsSingleton(num_txns_total),
                           pvalue.AsSingleton(options))
        )

        calcbs = (
            calibration
            | beam.Map(lambda x: (x[0], x))
            | 'Group calibration elog by customer id' >> beam.GroupByKey()
            | beam.FlatMap(
                c.create_cal_cbs,
                pvalue.AsSingleton(options),
                pvalue.AsSingleton(limits_dates)
            )  # (customer_id, number_of_transactions, average_order_value,
               #  frequency, recency, total_time_observed)
        )

        first_transaction_dates_by_customer = (
            cal_hol_elog
            | beam.Map(lambda x: (x[0], x))  # customer_id
            | 'Group cal hol elog by customer id' >> beam.GroupByKey()
            | beam.Map(lambda x: (x[0], min(map(operator.itemgetter(2), x[1])))
                       )  # item 2 -> date
        )

        cal_hol_elog_repeat = (
            cal_hol_elog
            | beam.FlatMap(c.filter_first_transaction_date_records,
                           pvalue.AsDict(first_transaction_dates_by_customer))
            | beam.FlatMap(
                c.calculate_time_unit_numbers,  # (customer_id, date,
                                                #  time_unit_number)
                pvalue.AsSingleton(options),
                pvalue.AsSingleton(limits_dates))
            | beam.Map(lambda x: (x[2], 1))  # key: time_unit_number
            | 'Group cal hol elog repeat by time unit number' >>
            beam.GroupByKey()
            | beam.Map(lambda x: (x[0], sum(x[1]))
                       )  # (time_unit_number, occurrences)
        )

        repeat_tx = (
            pipeline
            | 'Create single elem Stream III' >> beam.Create([1])
            | beam.FlatMap(c.calculate_cumulative_repeat_transactions,
                           pvalue.AsIter(cal_hol_elog_repeat)
                           )  # (time_unit_number, repeat_transactions,
                              #  repeat_transactions_cumulative)
        )

        model_validation = (
            pipeline
            | 'Create single elem Stream IV' >> beam.Create([1])
            | beam.FlatMap(c.calculate_model_fit_validation,
                           pvalue.AsSingleton(options),
                           pvalue.AsSingleton(limits_dates),
                           pvalue.AsIter(calcbs),
                           pvalue.AsIter(repeat_tx),
                           pvalue.AsSingleton(num_customers),
                           pvalue.AsSingleton(num_txns))
        )

        _ = (
            model_validation
            | beam.Map(c.raise_error_if_invalid_mape)
        )

        _ = (
            model_validation
            | beam.Map(lambda x: x[0])
            | 'Write to validation_params table' >>
                io.WriteToBigQuery(
                    table=c.TableValueProvider(
                        getattr(runtime_options, c._OPTION_OUTPUT_BQ_PROJECT),
                        getattr(runtime_options, c._OPTION_OUTPUT_BQ_DATASET),
                        'validation_params'
                    ),
                    custom_gcs_temp_location=getattr(runtime_options, c._OPTION_TEMP_GCS_LOCATION),
                    validate=False,
                    schema={
                        'fields': [
                            {'name': 'calibration_start_date', 'type': 'STRING'},
                            {'name': 'calibration_end_date', 'type': 'STRING'},
                            {'name': 'cohort_start_date', 'type': 'STRING'},
                            {'name': 'cohort_end_date', 'type': 'STRING'},
                            {'name': 'holdout_end_date', 'type': 'STRING'},
                            {'name': 'model_time_granularity', 'type': 'STRING'},
                            {'name': 'model', 'type': 'RECORD',
                                'fields': [
                                    {'name': 'frequency_model', 'type': 'STRING'},
                                    {'name': 'num_customers_cohort', 'type': 'INTEGER'},
                                    {'name': 'perc_customers_cohort', 'type': 'FLOAT'},
                                    {'name': 'num_transactions_validation', 'type': 'INTEGER'},
                                    {'name': 'perc_transactions_validation', 'type': 'FLOAT'},
                                    {'name': 'validation_mape', 'type': 'STRING'},
                                ]}
                        ]
                    },
                    write_disposition=io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=io.BigQueryDisposition.CREATE_IF_NEEDED)
        )

        fullcbs_without_extra_dimension = (
            full_elog_merged
            | beam.Map(lambda x: (x[0], x))  # key: customer_id
            | 'Group full merged elog by customer id' >> beam.GroupByKey()
            | beam.FlatMap(
                c.create_fullcbs,
                pvalue.AsSingleton(options),
                pvalue.AsSingleton(min_max_dates)
            )  # (customer_id, number_of_transactions, historical_aov,
               #  frequency, recency, total_time_observed)
        )

        full_elog_if_extra_dimension = (
            full_elog
            | 'Discard records if no extra dimension' >> beam.FlatMap(
                c.discard_if_no_extra_dimension, pvalue.AsSingleton(options))
        )

        extra_dimensions_stats = (
            full_elog_if_extra_dimension
            | beam.Map(lambda x: ((x[0], x[4]), x)
                       )  # key: (customer_id, extra_dimension)
            | 'Group full elog by customer id and extra dimension' >>
                beam.GroupByKey()
            | beam.Map(
                c.create_extra_dimensions_stats
            )  # (customer_id, extra_dimension, dimension_count, tot_sales,
               #  max_dimension_date)
        )

        top_dimension_per_customer = (
            extra_dimensions_stats
            | beam.Map(lambda x: (x[0], x))  # customer_id
            | 'Group extra dimension stats by customer id' >> beam.GroupByKey()
            | beam.Map(
                c.extract_top_extra_dimension
            )  # (customer_id, extra_dimension, dimension_count, tot_sales,
               #  max_dimension_date)
        )

        customer_dimension_map = (
            top_dimension_per_customer
            | beam.Map(
                lambda x: (x[0], x[1]))  # (customer_id, extra_dimension)
        )

        prediction = (
            pipeline
            | 'Create single elem Stream V' >> beam.Create([1])
            | beam.FlatMap(
                c.calculate_prediction,
                pvalue.AsSingleton(options),
                pvalue.AsIter(fullcbs_without_extra_dimension),
                pvalue.AsSingleton(num_customers),
                pvalue.AsSingleton(num_txns)
            )  # [customer_id, p_alive, predicted_purchases, future_aov,
               #  historical_aov, expected_value, frequency, recency,
               #  total_time_observed], prediction_params
        )

        prediction_by_customer_no_segments_no_extra_dimension = (
            prediction
            | beam.FlatMap(lambda x: x[0])  # Extract predictions by customer
        )

        prediction_by_customer_no_segments = (
            prediction_by_customer_no_segments_no_extra_dimension
            | beam.FlatMap(
                c.add_top_extra_dimension_to_fullcbs,
                pvalue.AsSingleton(options),
                pvalue.AsDict(customer_dimension_map)
            )  # [customer_id, p_alive, predicted_purchases, future_aov
               #  historical_aov, expected_value, frequency, recency,
               #  total_time_observed, extra_dimension?]
        )

        _ = (
            prediction
            | beam.Map(lambda x: x[1])  # Extract prediction params
            | 'Write to prediction_params table' >>
                io.WriteToBigQuery(
                    table=c.TableValueProvider(
                        getattr(runtime_options, c._OPTION_OUTPUT_BQ_PROJECT),
                        getattr(runtime_options, c._OPTION_OUTPUT_BQ_DATASET),
                        'prediction_params'
                    ),
                    custom_gcs_temp_location=getattr(runtime_options, c._OPTION_TEMP_GCS_LOCATION),
                    validate=False,
                    schema={
                        'fields': [
                            {'name': 'prediction_period', 'type': 'INTEGER'},
                            {'name': 'prediction_period_unit', 'type': 'STRING'},
                            {'name': 'model_time_granularity', 'type': 'STRING'},
                            {'name': 'customers_modeled', 'type': 'INTEGER'},
                            {'name': 'transactions_observed', 'type': 'INTEGER'},
                            {'name': 'frequency_model', 'type': 'STRING'},
                            {'name': 'bgnbd_model_params', 'type': 'RECORD',
                                'fields': [
                                    {'name': 'a', 'type': 'FLOAT'},
                                    {'name': 'b', 'type': 'FLOAT'},
                                    {'name': 'r', 'type': 'FLOAT'},
                                    {'name': 'alpha', 'type': 'FLOAT'}
                                ]},
                            {'name': 'bgbb_model_params', 'type': 'RECORD',
                                'fields': [
                                    {'name': 'alpha', 'type': 'FLOAT'},
                                    {'name': 'beta', 'type': 'FLOAT'},
                                    {'name': 'gamma', 'type': 'FLOAT'},
                                    {'name': 'delta', 'type': 'FLOAT'}
                                ]},
                            {'name': 'paretonbd_model_params',
                                'type': 'RECORD',
                                'fields': [
                                    {'name': 'r', 'type': 'FLOAT'},
                                    {'name': 's', 'type': 'FLOAT'},
                                    {'name': 'alpha', 'type': 'FLOAT'},
                                    {'name': 'beta', 'type': 'FLOAT'}
                                ]},
                            {'name': 'gamma_gamma_params',
                                'type': 'RECORD',
                                'fields': [
                                    {'name': 'p', 'type': 'FLOAT'},
                                    {'name': 'q', 'type': 'FLOAT'},
                                    {'name': 'v', 'type': 'FLOAT'}
                                ]}
                        ]
                    },
                    write_disposition=io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=io.BigQueryDisposition.CREATE_IF_NEEDED)
        )

        num_rows = (
            full_elog_merged
            | 'Count num rows in full elog merged' >>
                beam.combiners.Count.Globally()
        )

        segment_predictions_exact = (
            pipeline
            | 'Create single elem Stream VII' >> beam.Create([1])
            | beam.FlatMap(lambda _, rows_count: [
                rows_count <= c._SEGMENT_PREDICTION_THRESHOLD],
                           pvalue.AsSingleton(num_rows))
        )

        sharded_cust_predictions_no_segments_exact, \
            sharded_cust_predictions_no_segments_hash = (
                prediction_by_customer_no_segments
                | beam.FlatMap(
                    c.prediction_sharded,
                    pvalue.AsSingleton(options),
                    pvalue.AsSingleton(segment_predictions_exact)
                )  # [customer_id, p_alive, predicted_purchases, future_aov,
                   #  historical_aov, expected_value, frequency, recency,
                   #  total_time_observed, extra_dimension?]
                | beam.Partition(lambda x, _: 0 if x[1] else 1, 2)
            )

        # BEGIN of "exact" branch
        prediction_by_customer_exact = (
            pipeline
            | 'Create single elem Stream VIII' >> beam.Create([1])
            | beam.FlatMap(c.split_in_ntiles_exact,
                           pvalue.AsSingleton(options),
                           pvalue.AsIter(
                               sharded_cust_predictions_no_segments_exact)
                           )  # [customer_id, p_alive, predicted_purchases,
                              #  future_aov, historical_aov, expected_value,
                              #  frequency, recency, total_time_observed,
                              #  segment, extra_dimension?]
        )
        # END of "exact" branch

        # BEGIN of "hash" branch
        customer_count_by_expected_value = (
            sharded_cust_predictions_no_segments_hash
            | beam.Map(lambda x: (x[0][5], 1))  # (expected_value, 1)
            | 'Group customer predictions by expected value' >>
                beam.GroupByKey()
            | beam.Map(
                lambda x: (x[0], sum(x[1])))  # expected_value, customers_count
        )

        hash_segment_limits = (
            pipeline
            | 'Create single elem Stream IX' >> beam.Create([1])
            | beam.FlatMap(c.expected_values_segment_limits,
                           pvalue.AsSingleton(options),
                           pvalue.AsIter(customer_count_by_expected_value),
                           pvalue.AsSingleton(all_customer_ids_count))
        )

        prediction_by_customer_hash = (
            sharded_cust_predictions_no_segments_hash
            | beam.Map(lambda x: x[0])
            | beam.FlatMap(c.split_in_ntiles_hash,
                           pvalue.AsSingleton(hash_segment_limits)
                           )  # [customer_id, p_alive, predicted_purchases,
                              #  future_aov, historical_aov, expected_value,
                              #  frequency, recency, total_time_observed,
                              #  segment, extra_dimension?]
        )
        # END of "hash" branch

        prediction_by_customer = (
            # only one of these two streams will contains values
            (prediction_by_customer_exact, prediction_by_customer_hash)
            | beam.Flatten()
            | beam.Map(c.clean_nan_and_inf)
        )

        _ = (
            prediction_by_customer
            | beam.FlatMap(lambda x, opts: [x + ['']]
                           if not opts[c._OPTION_EXTRA_DIMENSION_EXISTS] else [x],
                           pvalue.AsSingleton(options))
            | 'prediction_by_customer to Dict' >> beam.Map(c.list_to_dict, [
                    'customer_id', 'p_alive', 'predicted_purchases',
                    'future_aov', 'historical_aov',
                    'expected_value', 'frequency', 'recency',
                    'total_time_observed', 'segment',
                    'extra_dimension'])
            | 'Write to prediction_by_customer table' >>
                io.WriteToBigQuery(
                    table=c.TableValueProvider(
                        getattr(runtime_options, c._OPTION_OUTPUT_BQ_PROJECT),
                        getattr(runtime_options, c._OPTION_OUTPUT_BQ_DATASET),
                        'prediction_by_customer'
                    ),
                    custom_gcs_temp_location=getattr(runtime_options, c._OPTION_TEMP_GCS_LOCATION),
                    validate=False,
                    schema='customer_id:STRING, p_alive:FLOAT64'
                           ', predicted_purchases:FLOAT64'
                           ', future_aov:FLOAT64, historical_aov:FLOAT64'
                           ', expected_value:FLOAT64, frequency:INT64'
                           ', recency:FLOAT64'
                           ', total_time_observed:FLOAT64, segment:INT64'
                           ', extra_dimension:STRING',
                    write_disposition=io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=io.BigQueryDisposition.CREATE_IF_NEEDED)
        )

        prediction_summary_temp = (
            prediction_by_customer
            | beam.Map(lambda x: (x[9], x))  # key: segment
            | 'Group customer predictions by segment' >> beam.GroupByKey()
            | beam.FlatMap(c.generate_prediction_summary,
                           pvalue.AsSingleton(options)
                           )  # (segment, average_retention_probability,
                              #  average_predicted_customer_value,
                              #  average_predicted_order_value,
                              #  average_predicted_purchases, total_customer_value,
                              #  number_of_customers)
        )

        tot_equity = (
            prediction_summary_temp
            | beam.Map(lambda x: x[5])  # total_customer_value
            | beam.CombineGlobally(sum)
        )

        prediction_summary = (
            prediction_summary_temp
            | beam.FlatMap(
                c.calculate_perc_of_total_customer_value,
                pvalue.AsSingleton(tot_equity),
                pvalue.AsSingleton(options)
                )  # (segment, average_retention_probability,
                   #  average_predicted_customer_value,
                   #  average_predicted_order_value,
                   #  average_predicted_purchases,
                   #  total_customer_value, number_of_customers,
                   #  perc_of_total_customer_value)
        )

        _ = (
            prediction_summary
            | 'prediction_summary to Dict' >> beam.Map(c.list_to_dict, [
                'segment', 'average_retention_probability',
                'average_predicted_customer_value',
                'average_predicted_order_value', 'average_predicted_purchases',
                'total_customer_value', 'number_of_customers',
                'perc_of_total_customer_value'])
            | 'Write to prediction_summary table' >> io.WriteToBigQuery(
                table=c.TableValueProvider(
                    getattr(runtime_options, c._OPTION_OUTPUT_BQ_PROJECT),
                    getattr(runtime_options, c._OPTION_OUTPUT_BQ_DATASET),
                    'prediction_summary'
                ),
                custom_gcs_temp_location=getattr(runtime_options, c._OPTION_TEMP_GCS_LOCATION),
                validate=False,
                schema='segment:INT64 ,average_retention_probability:FLOAT64'
                       ', average_predicted_customer_value:FLOAT64'
                       ', average_predicted_order_value:FLOAT64'
                       ', average_predicted_purchases:FLOAT64'
                       ', total_customer_value:FLOAT64'
                       ', number_of_customers:FLOAT64'
                       ', perc_of_total_customer_value:FLOAT64',
                write_disposition=io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=io.BigQueryDisposition.CREATE_IF_NEEDED)
        )

        prediction_summary_extra_dimension = (
            prediction_by_customer
            | 'Discard prediction if there is not extra dimension' >>
                beam.FlatMap(c.discard_if_no_extra_dimension,
                             pvalue.AsSingleton(options))
            | beam.Map(lambda x: (x[10], x))  # extra dimension
            | 'Group customer predictions by extra dimension' >>
                beam.GroupByKey()
            | beam.FlatMap(c.generate_prediction_summary_extra_dimension,
                           pvalue.AsSingleton(tot_equity),
                           pvalue.AsSingleton(options))
        )

        _ = (
            prediction_summary_extra_dimension
            | 'prediction_summary_extra_dimension to Dict' >> beam.Map(c.list_to_dict, [
                'extra_dimension', 'average_retention_probability',
                'average_predicted_customer_value',
                'average_predicted_order_value',
                'average_predicted_purchases', 'total_customer_value',
                'number_of_customers', 'perc_of_total_customer_value'])
            | 'Write to prediction_summary_extra_dimension table' >> io.WriteToBigQuery(
                table=c.TableValueProvider(
                    getattr(runtime_options, c._OPTION_OUTPUT_BQ_PROJECT),
                    getattr(runtime_options, c._OPTION_OUTPUT_BQ_DATASET),
                    'prediction_summary_extra_dimension'
                ),
                custom_gcs_temp_location=getattr(runtime_options, c._OPTION_TEMP_GCS_LOCATION),
                validate=False,
                schema='extra_dimension:STRING'
                       ', average_retention_probability:FLOAT64'
                       ', average_predicted_customer_value:FLOAT64'
                       ', average_predicted_order_value:FLOAT64'
                       ', average_predicted_purchases:FLOAT64'
                       ', total_customer_value:FLOAT64'
                       ', number_of_customers:INT64'
                       ', perc_of_total_customer_value:FLOAT64',
                write_disposition=io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=io.BigQueryDisposition.CREATE_IF_NEEDED)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
