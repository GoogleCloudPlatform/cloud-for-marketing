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

import argparse
import csv
import datetime as dt
import io
import logging
import math
import operator

import apache_beam as beam
import numpy as np
import pandas as pd
from apache_beam import pvalue
from apache_beam.io.gcp import gcsfilesystem
from apache_beam.options import pipeline_options
from apache_beam.options import value_provider
from apache_beam.transforms import util
import lifetimes


_SEGMENT_PREDICTION_THRESHOLD = 100000

_OPTION_INPUT_CSV = 'input_csv'
_OPTION_OUTPUT_FOLDER = 'output_folder'
_OPTION_CUSTOMER_ID_COLUMN_POSITION = 'customer_id_column_position'
_OPTION_TRANSACTION_DATE_COLUMN_POSITION = 'transaction_date_column_position'
_OPTION_SALES_COLUMN_POSITION = 'sales_column_position'
_OPTION_EXTRA_DIMENSION_COLUMN_POSITION = 'extra_dimension_column_position'
_OPTION_EXTRA_DIMENSION_EXISTS = 'extra_dimension_exists'
_OPTION_DATE_PARSING_PATTERN = 'date_parsing_pattern'
_OPTION_MODEL_TIME_GRANULARITY = 'model_time_granularity'
_OPTION_FREQUENCY_MODEL_TYPE = 'frequency_model_type'
_OPTION_CALIBRATION_START_DATE = 'calibration_start_date'
_OPTION_CALIBRATION_END_DATE = 'calibration_end_date'
_OPTION_COHORT_START_DATE = 'cohort_start_date'
_OPTION_COHORT_END_DATE = 'cohort_end_date'
_OPTION_HOLDOUT_END_DATE = 'holdout_end_date'
_OPTION_PREDICTION_PERIOD = 'prediction_period'
_OPTION_OUTPUT_SEGMENTS = 'output_segments'
_OPTION_TRANSACTION_FREQUENCY_THRESHOLD = 'transaction_frequency_threshold'
_OPTION_PENALIZER_COEF = 'penalizer_coef'

_MODEL_TYPE_BGNBD = 'BGNBD'
_MODEL_TYPE_PNBD = 'PNBD'

date_formats = {
    'YYYY-MM-DD': '%Y-%m-%d',
    'MM/DD/YY': '%m/%d/%y',
    'MM/DD/YYYY': '%m/%d/%Y',
    'DD/MM/YY': '%d/%m/%y',
    'DD/MM/YYYY': '%d/%m/%Y',
    'YYYYMMDD': '%Y%m%d',
}


class TimeGranularityParams(object):
    """Converts granularity into number of days and unit string.

    Util class used to convert granularity input param into values used
    by the pipeline.

    Args:
        granularity: the time granularity from the pipeline input params.
    """

    UNIT_DAYS = 'days'
    UNIT_WEEKS = 'weeks'
    UNIT_MONTHS = 'months'

    GRANULARITY_DAILY = 'daily'
    GRANULARITY_WEEKLY = 'weekly'
    GRANULARITY_MONTHLY = 'monthly'

    def __init__(self, granularity):
        """Initialize days and unit attributes given the granularity"""
        self.granularity = (granularity or '').lower()
        if self.granularity == TimeGranularityParams.GRANULARITY_DAILY:
            self.days = 1
            self.unit = TimeGranularityParams.UNIT_DAYS
        elif self.granularity == TimeGranularityParams.GRANULARITY_WEEKLY:
            self.days = 7
            self.unit = TimeGranularityParams.UNIT_WEEKS
        elif self.granularity == TimeGranularityParams.GRANULARITY_MONTHLY:
            self.days = 30
            self.unit = TimeGranularityParams.UNIT_MONTHS
        else:
            raise ValueError(
                'Model time granularity specified is not one of the following '
                'valid options: '
                '[daily, weekly, monthly]')

    def get_days(self):
        """Returns number of days for the current time granularity"""
        return self.days

    def get_time_unit(self):
        """
        Returns string describing the time unit for the current time
        granularity
        """
        return self.unit


def parse_date(date, options):
    """Parse a date in string format.

    Converts string formatted date into datetime object using the information
    available in the pipeline options dictionary.

    Args:
        date: Date to parse in string format.
        options: Dictionary containing the input params for the pipeline.

    Returns:
        Parsed date in datetime format.
    """
    return dt.datetime.strptime(date,
                                date_formats[
                                    options[_OPTION_DATE_PARSING_PATTERN]])


def parse_date_yyyymmdd(date):
    return dt.datetime.strptime(date, '%Y-%m-%d')


def date_to_str(date):
    return date.strftime('%Y-%m-%d')


def set_extra_options(options):
    """Extends the options dictionary with other entries needed by the pipeline.

    Args:
        options: Dictionary containing the input params for the pipeline.

    Returns:
        The same dictionary with extra entries.
    """
    options[_OPTION_EXTRA_DIMENSION_EXISTS] = bool(
        options[_OPTION_EXTRA_DIMENSION_COLUMN_POSITION])
    return options


def save_to_file(output_path, save_function):
    """Saves to file.

    Opens the file descriptor for the output_path and passes it to the
    save_function that can write into it.

    Args:
        output_path: The path where the file will be saved (can be local
            path or GCS path).
        save_function: A function taking as single argument the output file
            descriptor and writing data into it.
    """
    if output_path.startswith('gs://'):
        with gcsfilesystem.GCSFileSystem(None).create(output_path) as f:
            save_function(f)
    else:
        with open(output_path, 'wb') as f:
            save_function(f)


def csv_line_to_list(line, options):
    """Creates list that pipeline can process from a CSV line (in list format).

    Takes a line (list) from the input CSV file and generate a list containing
    the data needed for the calculations (with entries in the right position).

    Args:
        line: List containing a single line from the input CSV file.
        options: Pipeline options (containing also the position of the fields).

    Returns:
        A list containing the fields needed for the next steps, in the right
        order [customer_id, date, sales, extra_dimension?].
        The result is wrapped in another list since this function is called
        inside a FlatMap operator.
    """
    customer_id_pos = options[_OPTION_CUSTOMER_ID_COLUMN_POSITION] - 1
    transaction_date_pos = options[_OPTION_TRANSACTION_DATE_COLUMN_POSITION] - 1
    sales_pos = options[_OPTION_SALES_COLUMN_POSITION] - 1
    extra_dimension_exists = options[_OPTION_EXTRA_DIMENSION_EXISTS]

    element = [
        line[customer_id_pos],
        line[transaction_date_pos],
        parse_date(line[transaction_date_pos], options),
        float(line[sales_pos]),
    ]

    if extra_dimension_exists:
        extra_dimension_pos = \
            options[_OPTION_EXTRA_DIMENSION_COLUMN_POSITION] - 1
        element.append(line[extra_dimension_pos])

    return [element]


def merge_full_elog_by_customer_and_date(entry):
    """Aggregate sales for each customer and date pair.

    Takes a collection of transactions grouped by customer_id and date and
    returns a single record with the sum of all those transaction values.

    Args:
        entry: Entry records from the previous pipeline step, grouped by
            customer_id and date.

    Returns:
        Tuple containing customer_id, date_str, date and the sum of all the
        sales in the grouped records.
    """
    (customer_id, date_str), records = entry
    sales = 0.0
    date = None
    for r in records:
        if not date:
            date = r[2]
        sales += r[3]
    return customer_id, date_str, date, round(sales, 2)


def min_max_dates_dict(min_max_dates):
    """Calculates dates needed by the pipeline, starting from min and max dates.

    Generates dictionary of dates needed for the calculation starting from a
    tuple containing min_date and max_date from the input logs.

    Args:
        min_max_dates: Tuple containing min_date and max_date for the input
            data.

    Returns:
         Dictionary containing info related to dates needed for the calculation.
    """
    min_date, max_date = min_max_dates
    tot_days = (max_date - min_date).days
    if round(tot_days * 0.05) < 30:
        cohort_days = 30
    else:
        cohort_days = round(round(tot_days * 0.05) / 30) * 30

    return {
        'min_date': min_date,
        'max_date': max_date,
        'total_days': tot_days,
        'half_days': round((tot_days / 2) + 0.1),
        'cohort_days': cohort_days
    }


def limit_dates_boundaries(dates, options):
    """Generates date limits object.

    Generates date boundaries needed for the calculation based on either:
    * The dates specified in the pipeline input params options
    * The dates calculated based on the input data.

    Args:
        dates: Dictionary of dates calculated based on the input data.
        options: Pipeline options (containing also the dates to use for the
            calculation).

    Returns:
        Dictionary containing info related to dates needed for the calculation
        parsed from the pipeline input params or from the min and max dates
        for the input data.
        The result is wrapped in another list since this function is called
        inside a FlatMap operator.
    """
    if options[_OPTION_CALIBRATION_START_DATE]:
        limit_dates = {
            'auto_date_selection':
                False,
            _OPTION_CALIBRATION_START_DATE:
                parse_date_yyyymmdd(options[_OPTION_CALIBRATION_START_DATE]),
            _OPTION_COHORT_END_DATE:
                parse_date_yyyymmdd(options[_OPTION_COHORT_END_DATE]),
            _OPTION_CALIBRATION_END_DATE:
                parse_date_yyyymmdd(options[_OPTION_CALIBRATION_END_DATE]),
            _OPTION_HOLDOUT_END_DATE:
                parse_date_yyyymmdd(options[_OPTION_HOLDOUT_END_DATE]),
        }
    else:
        limit_dates = {
            'auto_date_selection':
                True,
            _OPTION_CALIBRATION_START_DATE:
                dates['min_date'],
            _OPTION_COHORT_END_DATE:
                dates['min_date'] + dt.timedelta(days=dates['cohort_days']),
            _OPTION_CALIBRATION_END_DATE:
                dates['min_date'] + dt.timedelta(days=dates['half_days']),
            _OPTION_HOLDOUT_END_DATE:
                dates['max_date'],
        }

    limit_dates.update(
        holdout_start_date=limit_dates[_OPTION_CALIBRATION_END_DATE] +
                           dt.timedelta(days=1),
        cohort_start_date=limit_dates[_OPTION_CALIBRATION_START_DATE])

    return [limit_dates]


def filter_customers_in_cohort(entry, dates):
    """Keeps only customers between cohort start and end dates.

    Args:
        entry: List containing the customer's transaction.
        dates: Dictionary of dates needed for the calculation.

    Returns:
        The customer id if the user is between the dates.
        The result is wrapped in a list since this function is called inside
        a FlatMap operator.
    """
    if dates[_OPTION_COHORT_START_DATE] <= entry[2] <= \
        dates[_OPTION_COHORT_END_DATE]:
        return [entry[0]]
    return []


def count_customers(_, cohort_ids_count, all_customer_ids_count):
    """Creates a dictionary containing statistics on the number of customers.

    Args:
        _: Ignoring the first argument since it's only a value to trigger
            the call of this function (since it's running inside a FlatMap
            operator)
        cohort_ids_count: Number of customers in the cohort period of time.
        all_customer_ids_count: Number of all customers.

    Returns:
         Dictionary containing the data regarding the number of customers
         (wrapped in a list since this function is called inside a FlatMap
         operator).
    """
    return [{
        'num_customers_cohort':
            cohort_ids_count,
        'num_customers_total':
            all_customer_ids_count,
        'perc_customers_cohort':
            round(100.0 * cohort_ids_count / all_customer_ids_count, 2),
    }]


def filter_cohort_records_in_cal_hol(entry, cohort, dates):
    """Keeps only customers between calibration start and holdout end dates and
    in cohort set.

    Args:
        entry: List containing the customer's transaction.
        cohort: Dictionary containing the IDs of the customers in the cohort
            period of time.
        dates: Dictionary of dates needed for the calculation.

    Returns:
        The customer id if the user is between the dates and in cohort set.
        The result is wrapped in a list since this function is called inside
        a FlatMap operator.
    """
    if dates[_OPTION_CALIBRATION_START_DATE] <= entry[2] <= \
        dates[_OPTION_HOLDOUT_END_DATE] and entry[0] in cohort:
        return [entry]
    return []


def filter_records_in_cal_hol(entry, dates):
    """Keeps only customers between calibration start and holdout end dates.

    Args:
        entry: List containing the customer's transaction.
        dates: Dictionary of dates needed for the calculation.

    Returns:
        The customer id if the user is between the dates.
        The result is wrapped in a list since this function is called inside
        a FlatMap operator.
    """
    if dates[_OPTION_CALIBRATION_START_DATE] <= entry[2] \
            <= dates[_OPTION_HOLDOUT_END_DATE]:
        return [entry]
    return []


def filter_records_in_calibration(entry, dates):
    """Keeps only customers between calibration start and end dates.

    Args:
        entry: List containing the customer's transaction.
        dates: Dictionary of dates needed for the calculation.

    Returns:
        The customer id if the user is between the dates.
        The result is wrapped in a list since this function is called inside
        a FlatMap operator.
    """
    if dates[_OPTION_CALIBRATION_START_DATE] <= entry[2] <=  \
        dates[_OPTION_CALIBRATION_END_DATE]:
        return [entry]
    return []


def count_txns(_, cal_hol_count, num_txns_total):
    """Creates a dictionary containing the info regarding the number of
    transactions.

    Args:
        _: Ignoring the first argument since it's only a value to trigger
            the call of this function (since it's running inside a FlatMap
            operator)
        cal_hol_count: Number of records in the cohort period of time.
        num_txns_total: Number of total transactions.

    Returns:
         Dictionary containing the data regarding the number of transactions
         (wrapped in a list since this function is called inside a FlatMap
         operator).
    """
    return [{
        'num_txns_val': cal_hol_count,
        'num_txns_total': num_txns_total,
        'perc_txns_val': round(100.0 * cal_hol_count / num_txns_total, 2),
    }]


def create_cal_cbs(customer_records, options, dates):
    """Builds customer-by-sufficient-statistic (CBS) matrix in calibration
    period, filtered on cohort.

    Args:
        customer_records: Tuple containing customer_id and a list of
            transactions for the customer.
        options: Pipeline options (containing also time granularity used
            by the model)
        dates: Dictionary of dates needed for the calculation.

    Returns:
        Tuple containing RFM statistics for the customer.
        The result is wrapped in a list since this function is called inside
        a FlatMap operator.
    """
    customer_id, records = customer_records

    number_of_transactions = 0
    tot_sales = 0.0
    min_date = None
    max_date = None
    for r in records:
        number_of_transactions += 1
        tot_sales += r[3]  # sales
        if not min_date or r[2] < min_date:  # date
            min_date = r[2]
        if not max_date or r[2] > max_date:
            max_date = r[2]

    average_order_value = tot_sales / number_of_transactions
    model_time_divisor = TimeGranularityParams(
        options[_OPTION_MODEL_TIME_GRANULARITY]).get_days()
    frequency = number_of_transactions - 1
    recency = (max_date - min_date).days / model_time_divisor
    total_time_observ = ((dates[_OPTION_CALIBRATION_END_DATE] - min_date).days +
                         1) / model_time_divisor

    return [(customer_id, number_of_transactions, average_order_value,
             frequency, recency, total_time_observ)]


def create_fullcbs(customer_records, options, dates):
    """Builds customer-by-sufficient-statistic (CBS) matrix in
    calibration+holdout period, filtered on cohort.

    Args:
        customer_records: Tuple containing customer_id and a list of
            transactions for the customer.
        options: Pipeline options (containing also time granularity used
            by the model)
        dates: Dictionary of dates needed for the calculation.

    Returns:
        Tuple containing RFM statistics for the customer.
        The result is wrapped in a list since this function is called inside
        a FlatMap operator.
    """
    customer_id, records = customer_records

    number_of_transactions = 0
    tot_sales = 0.0
    min_date = None
    max_date = None
    for r in records:
        number_of_transactions += 1
        tot_sales += r[3]  # sales
        if not min_date or r[2] < min_date:  # date
            min_date = r[2]
        if not max_date or r[2] > max_date:
            max_date = r[2]

    historical_aov = tot_sales / number_of_transactions
    model_time_divisor = TimeGranularityParams(
        options[_OPTION_MODEL_TIME_GRANULARITY]).get_days()
    frequency = number_of_transactions - 1
    recency = (max_date - min_date).days / model_time_divisor
    total_time_observed = (
        (dates['max_date'] - min_date).days + 1) / model_time_divisor

    return [(
        customer_id,
        number_of_transactions,
        historical_aov,
        frequency,
        recency,
        total_time_observed,
    )]


def create_extra_dimensions_stats(customer_records_by_extra_dim):
    """Groups entry records by customer_id and extra_dimension.

    Args:
        customer_records_by_extra_dim: Tuple containing a tuple of customer_id
            and extra_dimension and a list of transaction records related to
            that combination

    Returns:
        Tuple containing statistics for the customer and extra dimension.
    """
    (customer_id, extra_dimension), records = customer_records_by_extra_dim

    dimension_count = 0
    tot_sales = 0.0
    max_dimension_date = None
    for r in records:
        dimension_count += 1
        tot_sales += r[3]
        if not max_dimension_date or r[2] > max_dimension_date:
            max_dimension_date = r[2]

    return customer_id, extra_dimension, dimension_count, tot_sales, \
           max_dimension_date


def best_customer_record(new_record, top_record):
    """Find the best customer record.

    Compares the new_record to the current top_record to determine what's the
    best extra_dimension for the customer.
    Records are lists containing the following information in the specified
    position:
    2: dimension_count
    3: tot_sales
    4: max_dimension_date

    Args:
        new_record: New record that we want to compare to the current top
            record.
        top_record: The current top record.

    Returns:
        The best record according to the logic.
    """
    if new_record[2] > top_record[2] or \
        new_record[2] == top_record[2] and new_record[4] > top_record[4] or \
        new_record[2] == top_record[2] and new_record[4] == top_record[4] and \
        new_record[3] > top_record[3]:
        return new_record
    return top_record


def extract_top_extra_dimension(customer_records):
    """Find top extra dimension for a customer.

    Determines what's the best extra_dimension for the customer (based on
    dimension_count, tot_sales and max_dimension_date).

    Args:
        customer_records: Tuple containing a customer_id and a list of
            extra_dimension stats related to them.
    Returns:
        The best record in the list (best extra_dimension for the customer)
        based on the rules specified in best_customer_record.
    """
    _, records = customer_records
    top_record = None
    for r in records:
        if not top_record:
            top_record = r
            continue
        top_record = best_customer_record(r, top_record)

    return top_record


def add_top_extra_dimension_to_fullcbs(entry, options, customer_extra_dim):
    """Appends the top extra_dimension to the entry list.

    Args:
        entry: Record (list) where the extra_dimension field will be appended.
        options: Pipeline options (containing also if extra dimension is used).
        customer_extra_dim: Dictionary of top extra_dimensions for each
            customer.

    Returns:
        Entry list with the extra_dimension appended to the end.
    """
    if not options[_OPTION_EXTRA_DIMENSION_EXISTS]:
        return [entry]

    customer_id = entry[0]
    if customer_id in customer_extra_dim:
        return [entry + (customer_extra_dim[customer_id],)]
    return [entry]


def discard_if_no_extra_dimension(entry, options):
    """Streams the entry into the next pipeline step only if the
    EXTRA_DIMENSION_EXISTS option is set.

    Args:
        entry: Entry record.
        options: Pipeline options (containing also if extra dimension is used).

    Returns:
        The entry if the EXTRA_DIMENSION_EXISTS is set, otherwise None.
        The result is wrapped in a list since this function is called inside
        a FlatMap operator.
    """
    if not options[_OPTION_EXTRA_DIMENSION_EXISTS]:
        return []
    return [entry]


def filter_first_transaction_date_records(entry, first_trans):
    """Discards the entry if its date is the date of the first transaction for
    the customer.

    Args:
        entry: Entry record for a customer's transaction.
        first_trans: Dictionary containing the date of first transaction for
            each customer.

    Returns:
        The entry if the entry's date is not the first transaction date for
        the customer.
        The result is wrapped in a list since this function is called inside
        a FlatMap operator.
    """
    if entry[2] == first_trans.get(
            entry[0]):  # date == first_transaction[customer_id]
        return []
    return [entry]


def calculate_time_unit_numbers(entry, options, dates):
    """Calculates the time value number of the date using the time granularity
    unit.

    Args:
        entry: Entry record for a customer's transaction.
        options: Pipeline options (containing also time granularity used
            by the model)
        dates: Dictionary of dates needed for the calculation.

    Returns:
        Tuple containing customer_id, date and the time value number in the
        specified unit.
        The result is wrapped in a list since this function is called inside
        a FlatMap operator.
    """
    csdp1 = dates[_OPTION_CALIBRATION_START_DATE] + dt.timedelta(days=1)
    model_time_divisor = TimeGranularityParams(
        options[_OPTION_MODEL_TIME_GRANULARITY]).get_days()
    # (customer_id, date, time_unit_number)
    return [(entry[0], entry[2],
             math.floor((entry[2] - csdp1).days / model_time_divisor) + 1)]


def calculate_cumulative_repeat_transactions(_, repeats):
    """Sorts and combines the transactions for each time unit.

    Args:
        _: Ignoring the first argument since it's only a value to trigger
            the call of this function (since it's running inside a FlatMap
            operator)
        repeats: Number of transactions for each time unit

    Returns:
         Tuple containing the time_unit value, transaction in that time unit
         and teh cumulative sum of all the transactions so far.
    """
    sorted_repeats = sorted(repeats, key=operator.itemgetter(0))
    tot = 0
    for k, v in sorted_repeats:
        tot += v
        yield k, v, tot  # (time_unit_number, repeat_transactions,
                         #  repeat_transactions_cumulative)


def prediction_sharded(entry, options, use_exact_method):
    """Streams the entry into the next pipeline step only if the
    EXTRA_DIMENSION_EXISTS option is set.

    Args:
        entry: Entry record.
        options: Pipeline options (containing also if extra dimension is used).
        use_exact_method: Boolean, True if the exact method should be used to
            shard the customers.

    Returns:
        Tuple containing the entry record (with rounded numbers) and whether
        the exact method should be used.
        The result is wrapped in a list since this function is called inside
        a FlatMap operator.
    """
    result = [
        entry[0],
        round(entry[1], 4),
        round(entry[2], 3),
        round(entry[3], 2),
        round(entry[4], 2),
        round(entry[5], 2),
        entry[6],
        round(entry[7], 3),
        round(entry[8], 3),
    ]
    if options[_OPTION_EXTRA_DIMENSION_EXISTS]:
        result.append(entry[9])

    return [(result, use_exact_method)]


def split_in_ntiles_exact(_, options, entries):
    """Associates each entry to the right segment they belong to.

    Args:
        _: Ignoring the first argument since it's only a value to trigger
            the call of this function (since it's running inside a FlatMap
            operator)
        options: Pipeline options (containing also the number of segments).
        entries: Iterable collection of all the entries.

    Returns:
        The entries containing the right segment they belong to.
    """
    sorted_entries = sorted(
        map(operator.itemgetter(0), entries),
        key=operator.itemgetter(5),  # 5 -> expected_value
        reverse=True)

    output_segments = options[_OPTION_OUTPUT_SEGMENTS]
    per_segment = len(sorted_entries) // output_segments
    reminder = (len(sorted_entries) % output_segments) + 1
    current_segment = 1
    idx = 0

    while current_segment <= output_segments:
        n = per_segment
        if reminder > current_segment:
            n += 1
        while n > 0:
            entry = sorted_entries[idx]
            entry.insert(9, current_segment)
            yield entry
            n -= 1
            idx += 1
        current_segment += 1


def expected_values_segment_limits(_, options, expected_values_count,
                                   num_customers):
    """Finds the segments limits (borders) in case the hash method is used.

    Args:
        _: Ignoring the first argument since it's only a value to trigger
            the call of this function (since it's running inside a FlatMap
            operator)
        options: Pipeline options (containing also the number of segments).
        expected_values_count: List of pairs containing the number of
            transactions for each monetary value.
        num_customers: Number of customers.

    Returns:
        List of triples containing monetary value of the border and ratio of
        inclusion for those transactions with the exact border value (2nd value
        / 3rd value).
    """
    sorted_expected_values_count = sorted(
        expected_values_count, key=operator.itemgetter(0), reverse=True)
    result = []

    output_segments = options[_OPTION_OUTPUT_SEGMENTS]
    per_segment = num_customers // output_segments
    reminder = (num_customers % output_segments) + 1
    current_segment = 1
    n = per_segment
    if reminder > current_segment:
        n += 1
    for val, c in sorted_expected_values_count:
        n -= c
        while n <= 0:
            result.append((val, min(n + c, c), c))
            current_segment += 1
            n = per_segment + n
            if reminder > current_segment:
                n += 1
            c = per_segment + 1

    return [result[:-1]]


def split_in_ntiles_hash(entry, segment_limits):
    """Associates each entry to the right segment they belong to.

    Args:
        entry: Entry record with customer statistics.
        segment_limits: Triple containing monetary value of the border and
            ratio of inclusion for those transactions with the exact border
            value (2nd value/ 3rd value).

    Returns:
        The entries containing the right segment they belong to.
    """
    expected_value = entry[5]
    customer_id = entry[0]
    segment = 1
    for limit, take, total in segment_limits:
        if expected_value > limit:
            break
        if expected_value == limit and hash(str(customer_id)) % total < take:
            break
        segment += 1
    entry.insert(9, segment)
    return [entry]


def generate_prediction_summary(segment_records):
    """Generates prediction summary for each segment.

    Args:
        segment_records: Tuple containing the segment number and the list of
            records associated to it.

    Returns:
        Tuple containing the segment and the aggregated statistics.
    """
    segment, records = segment_records

    count = 0
    p_alive_sum = 0.0
    expected_value_sum = 0.0
    future_aov_sum = 0.0
    predicted_purchases_sum = 0.0
    for r in records:
        count += 1
        p_alive_sum += r[1]
        expected_value_sum += r[5]
        future_aov_sum += r[3]
        predicted_purchases_sum += r[2]

    average_retention_probability = round(p_alive_sum / count, 2)
    average_predicted_customer_value = round(expected_value_sum / count, 2)
    average_predicted_order_value = round(future_aov_sum / count, 2)
    average_predicted_purchases = round(predicted_purchases_sum / count, 2)
    total_customer_value = round(expected_value_sum, 2)
    number_of_customers = count

    return (segment, average_retention_probability,
            average_predicted_customer_value, average_predicted_order_value,
            average_predicted_purchases, total_customer_value,
            number_of_customers)


def generate_prediction_summary_extra_dimension(extra_dim_records, tot_equity):
    """Generates prediction summary for each extra dimension.

    Args:
        extra_dim_records: Tuple containing the extra_dimension and the list of
            records associated to it.
        tot_equity: The total customer value.

    Returns:
        Tuple containing the extra_dimension and the aggregated statistics.
        The result is wrapped in a list since this function is called inside
        a FlatMap operator.
    """
    extra_dimension, records = extra_dim_records

    count = 0
    p_alive_sum = 0.0
    expected_value_sum = 0.0
    future_aov_sum = 0.0
    predicted_purchases_sum = 0.0
    for r in records:
        count += 1
        p_alive_sum += r[1]
        expected_value_sum += r[5]
        future_aov_sum += r[3]
        predicted_purchases_sum += r[2]

    average_retention_probability = round(p_alive_sum / count, 2)
    average_predicted_customer_value = round(expected_value_sum / count, 2)
    average_predicted_order_value = round(future_aov_sum / count, 2)
    average_predicted_purchases = round(predicted_purchases_sum / count, 2)
    total_customer_value = round(expected_value_sum, 2)
    number_of_customers = count
    perc_of_total_customer_value = str(
        round(100 * total_customer_value / tot_equity, 2)) + '%'

    return [(extra_dimension, average_retention_probability,
             average_predicted_customer_value, average_predicted_order_value,
             average_predicted_purchases, total_customer_value,
             number_of_customers, perc_of_total_customer_value)]


def calculate_perc_of_total_customer_value(entry, tot_equity):
    """Appends the percentage of total customer value to the entry.

    Args:
        entry: Entry record with segments statistics.
        tot_equity: The total equity for the customers value.

    Returns:
        The input entry with the percentage of total customer value appended.
        The result is wrapped in a list since this function is called inside
        a FlatMap operator.
    """
    perc_of_total_customer_value = str(round(100 * entry[5] / tot_equity,
                                             2)) + '%'
    return [entry + (perc_of_total_customer_value, )]


def list_to_csv_line(data):
    """Converts the input list into a valid CSV line string.

    Args:
        data: The input list

    Returns:
        String containing a valid CSV representation of the input list.
    """
    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL, lineterminator='')
    writer.writerow(data)
    return output.getvalue()


def fit_bgnbd_model(data, penalizer_coef=0.0):
    """Generates BG/NBD model from the input data.

    Args:
        data: Pandas DataFrame containing the customers data.
        penalizer_coef: The coefficient applied to an l2 norm on the parameters.

    Returns:
        The BG/NBD model.
    """
    bgf = lifetimes.BetaGeoFitter(penalizer_coef=penalizer_coef)
    bgf.fit(data['frequency'], data['recency'], data['total_time_observed'])
    return bgf


def extract_bgnbd_params(model):
    """Extracts params from the BG/NBD model

    Args:
        model: the BG/NBD model.

    Returns:
        The a, b, r and alpha params of the BG/NBD model.
    """
    r, alpha, a, b = model._unload_params('r', 'alpha', 'a', 'b')
    return {'r': r, 'alpha': alpha, 'a': a, 'b': b}


def fit_pnbd_model(data, penalizer_coef=0.0):
    """Generates Pareto/NBD model from the input data.

    Args:
        data: Pandas DataFrame containing the customers data.
        penalizer_coef: The coefficient applied to an l2 norm on the parameters.

    Returns:
        The Pareto/NBD model.
    """
    pareto = lifetimes.ParetoNBDFitter(penalizer_coef=penalizer_coef)
    pareto.fit(data['frequency'], data['recency'], data['total_time_observed'])
    return pareto


def extract_pnbd_params(model):
    """Extracts params from the Pareto/NBD model.

    Args:
        model: the Pareto/NBD model.

    Returns:
        The r, s, alpha and beta params of the Pareto/NBD model.
    """
    r, alpha, s, beta = model._unload_params('r', 'alpha', 's', 'beta')
    return {'r': r, 'alpha': alpha, 's': s, 'beta': beta}


def pnbd_conditional_expected_transactions(model, p_alive, t, frequency, T):
    """Calculates predicted purchases for the pandas Series passed as arguments
    using the specified model.

    Args:
        model: The Pareto/NBD model.
        p_alive: Pandas Series containing probability of being alive.
        t: Prediction period.
        frequency: Pandas Series containing the frequency.
        T: Pandas Series containing total time observed.

    Returns:
         Pandas Series containing calculated predicted purchases.
    """
    params = extract_pnbd_params(model)
    r = params['r']
    alpha = params['alpha']
    s = params['s']
    beta = params['beta']

    p1 = (r + frequency) * (beta + T) / ((alpha + T) * (s - 1))
    p2 = (1 - ((beta + T) / (beta + T + t))**(s - 1))
    p3 = p_alive

    return p1 * p2 * p3


def extract_gamma_gamma_params(model):
    """Extracts params from the Gamma-Gamma model.

    Args:
        model: the Gamma-Gamma model.

    Returns:
        The p, q and v params of the Gamma-Gamma model.
    """
    p, q, v = model._unload_params('p', 'q', 'v')
    params = {
        'p': p,
        'q': q,
        'v': v,
    }
    return params


def calc_full_fit_period(calibration_start_date, holdout_end_date,
                         time_divisor):
    """Calculates transactions period by time unit predictions.

    Args:
         calibration_start_date: Calibration start date.
         holdout_end_date: Holdout end date.
         time_divisor: Time divisor.

    Returns:
        Total number of periods to predict.
    """
    return int(
        math.ceil((holdout_end_date - calibration_start_date).days /
                  time_divisor)) + 3  # Just in case, doesn't hurt


def expected_cumulative_transactions(frequency_model, t_cal, t_tot):
    """Calculates expected cumulative transaction for each interval.

    Args:
        frequency_model: Model fitted on customer's data.
        t_cal: NumPy array of Total Time Observed values.
        t_tot: Total number of periods to predict.

    Returns:
        Numpy ndarray containing the expected cumulative transaction for each
        interval.
    """
    intervals = range(1, t_tot)
    cust_birth = np.amax(t_cal) - t_cal
    min_cust_birth = np.amin(cust_birth)

    expected_cumulative_transactions_output = []

    for interval in intervals:
        if interval < min_cust_birth:
            expected_cumulative_transactions_output.append(0)
            continue

        # Returns 1D array
        exp_purchases = frequency_model.expected_number_of_purchases_up_to_time(
            t=(interval - cust_birth[np.where(cust_birth <= interval)]))

        expected_cumulative_transactions_output.append(np.sum(exp_purchases))

    return np.around(np.array(expected_cumulative_transactions_output), 2)


def predict_txs(frequency_model, t_cal, intervals):
    """Calculates transactions by time unit predictions.

    Args:
        frequency_model: Model fitted on customer's data.
        t_cal: NumPy array of Total Time Observed values.
        intervals: Total number of periods to predict.

    Returns:
        Pandas DataFrame containing predicted future total purchases
    """
    expected_cumulative = expected_cumulative_transactions(
        frequency_model, t_cal, intervals)
    expected_incremental = expected_cumulative - np.delete(
        np.hstack(([0], expected_cumulative)), expected_cumulative.size - 1)

    predicted = pd.DataFrame(
        data=np.vstack((expected_incremental,
                        expected_cumulative)).transpose(),
        columns=[
            'predicted_transactions', 'predicted_cumulative_transactions'
        ])

    predicted['time_unit_number'] = predicted.index.values + 1
    return predicted


def calc_calibration_period(calibration_start_date, calibration_end_date,
                            time_divisor):
    """Calculate calibration period.

    Calculates where the median line for the calibration period should be drawn
    in the plot image (based on the selected granularity time unit).

    Args:
        calibration_start_date: Calibration start date
        calibration_end_date: Calibration end date
        time_divisor: Time divisor (number of days in the selected time
            granularity).

    Returns:
        Integer value indicating where the calibration period ends using the
        selected granularity as time unit.
    """
    return int(
        math.ceil(((calibration_end_date - calibration_start_date).days /
                   time_divisor)))


def plot_repeat_transaction_over_time(data, median, output_folder, time_label):
    """Creates and saves an image containing the plot the transactions over
    time.

    Args:
        data: Pandas DataFrame containing the data to plot.
        median: Median line that shows split between calibration and holdout
            period.
        output_folder: Folder where the image file containing the plot will be
            saved.
        time_label: String describing the time granularity.

    Returns:
        Nothing. Just save the image file to the output folder.
    """
    import matplotlib
    matplotlib.use('Agg')
    from matplotlib import pyplot as plt

    if time_label == TimeGranularityParams.GRANULARITY_DAILY:
        time_label_short = 'Day'
    elif time_label == TimeGranularityParams.GRANULARITY_MONTHLY:
        time_label_short = 'Month'
    else:
        time_label_short = 'Week'

    txs = data[
        ['time_unit_number', 'repeat_transactions', 'predicted_transactions']
    ]
    txs.columns = [time_label_short, 'Actual', 'Model']
    ax = txs.plot(kind='line', x=time_label_short, style=['-', '--'])

    # Median line that shows split between calibration and holdout period
    plt.axvline(median, color='k', linestyle='--')

    plt.legend()
    plt.title('Tracking %s Transactions' % time_label.capitalize())
    plt.ylabel('Transactions')
    plt.xlabel(time_label_short)

    # Save to file
    save_to_file(output_folder + 'repeat_transactions_over_time.png',
                 lambda f: plt.savefig(f, bbox_inches='tight'))


def plot_cumulative_repeat_transaction_over_time(data, median, output_folder,
                                                 time_label):
    """Creates and saves an image containing the plot the cumulative
    transactions over time.

    Args:
        data: Pandas DataFrame containing the data to plot.
        median: Median line that shows split between calibration and holdout
            period.
        output_folder: Folder where the image file containing the plot will be
            saved.
        time_label: String describing the time granularity.

    Returns:
        Nothing. Just save the image file to the output folder.
    """
    import matplotlib
    matplotlib.use('Agg')
    from matplotlib import pyplot as plt

    if time_label == TimeGranularityParams.GRANULARITY_DAILY:
        time_label_short = 'Day'
    elif time_label == TimeGranularityParams.GRANULARITY_MONTHLY:
        time_label_short = 'Month'
    else:
        time_label_short = 'Week'

    txs = data[
        ['time_unit_number', 'repeat_transactions_cumulative',
         'predicted_cumulative_transactions']
    ]
    txs.columns = [time_label_short, 'Actual', 'Model']
    ax = txs.plot(kind='line', x=time_label_short, style=['-', '--'])

    # Median line that shows split between calibration and holdout period
    plt.axvline(median, color='k', linestyle='--')

    plt.legend()
    plt.title('Tracking Cumulative Transactions')
    plt.ylabel('Cumulative Transactions')
    plt.xlabel(time_label_short)

    # Save to file
    save_to_file(
        output_folder + 'repeat_cumulative_transactions_over_time.png',
        lambda f: plt.savefig(f, bbox_inches='tight'))


def fit_gamma_gamma_model(data, penalizer_coef=0.0):
    """Fits the gamma-gamma model with number of transactions and average order
    value data.

    Args:
        data: Pandas DataFrame containing the data to fit.
        penalizer_coef: The coefficient applied to an l2 norm on the parameters.

    Returns:
        GammaGammaFitter – fitted and with parameters estimated.
    """
    ggf = lifetimes.GammaGammaFitter(penalizer_coef=penalizer_coef)
    ggf.fit(data['number_of_transactions'], data['average_order_value'])
    return ggf


def fit_gamma_gamma_model_prediction(data, penalizer_coef=0.0):
    """Fits the gamma-gamma model with number of transactions and historical AOV
    data.

    Args:
        data: Pandas DataFrame containing the data to fit.
        penalizer_coef: The coefficient applied to an l2 norm on the parameters.

    Returns:
        GammaGammaFitter – fitted and with parameters estimated.
    """
    ggf = lifetimes.GammaGammaFitter(penalizer_coef=penalizer_coef)
    ggf.fit(data['number_of_transactions'], data['historical_aov'])
    return ggf


def gamma_gamma_validation(cbs, penalizer_coef):
    """Validates the gamma-gamma (spend) model and add the Future AOV to the
    input CBS.

    Args:
        cbs: Customer-by-sufficient-statistic (CBS) DataFrame.
        penalizer_coef: The coefficient applied to an l2 norm on the parameters.

    Returns:
        Nothing.
    """
    model = fit_gamma_gamma_model(cbs, penalizer_coef)

    # Predicted AOV (fitted off of calibration only)
    cbs['future_aov'] = model.conditional_expected_average_profit(
        cbs['number_of_transactions'],
        cbs['average_order_value'])


def frequency_model_validation(model_type, cbs, cal_start_date, cal_end_date,
                               time_divisor, time_label, hold_end_date,
                               repeat_tx, output_folder, num_customers_cohort,
                               perc_customers_cohort, num_txns_val,
                               perc_txns_val, penalizer_coef):
    """Validates frequency model.

    Validates the model type on the input data and parameters and calculate the
    Mean Absolute Percent Error (MAPE).

    Args:
        model_type: String defining the type of model to be used, it can be
            either 'BGNBD' or 'PNBD'.
        cbs: Customer-by-sufficient-statistic (CBS) DataFrame.
        cal_start_date: Calibration start date.
        cal_end_date: Calibration end date.
        time_divisor: Number of days depending on the selected granularity.
        time_label: Selected time granularity label string.
        hold_end_date: Holdout end date.
        repeat_tx: Sorted cumulative sum of transactions for each time unit.
        output_folder: Folder where the text file containing the result of the
            validation will be saved.
        num_customers_cohort: Number of customers in the cohort period of time.
        perc_customers_cohort: Percentage of customers in the cohort period of
            time.
        num_txns_val: Number of transactions in the cohort period of time.
        perc_txns_val: Percentage of transactions in the cohort period of time.
        penalizer_coef: The coefficient applied to an l2 norm on the parameters.

    Returns:
        A tuple containing a text string with the result of the validation and
        the value of the Mean Absolute Percent Error (MAPE).
    """
    if model_type == _MODEL_TYPE_BGNBD:
        frequency_model = fit_bgnbd_model(cbs, penalizer_coef)
        model_params = 'Frequency Model: BG/NBD\n'
    elif model_type == _MODEL_TYPE_PNBD:
        frequency_model = fit_pnbd_model(cbs, penalizer_coef)
        model_params = 'Frequency Model: Pareto/NBD\n'
    else:
        raise ValueError('Model type %s is not valid' % model_type)

    # Transactions by time unit predictions
    intervals = calc_full_fit_period(cal_start_date, hold_end_date,
                                     time_divisor)
    predicted = predict_txs(frequency_model, cbs['total_time_observed'].values,
                            intervals)

    # Actual transactions per time unit
    txs = repeat_tx

    # Join predicted to actual
    txs = txs.merge(predicted, how='inner', on='time_unit_number')

    # Remove the last row as it often has partial data from an incomplete week
    txs = txs.drop(txs.index[len(txs) - 1])

    # Location of the median line for validation plots
    median_line = calc_calibration_period(cal_start_date, cal_end_date,
                                          time_divisor)

    # Plot creation
    plot_repeat_transaction_over_time(txs, median_line, output_folder,
                                      time_label)
    plot_cumulative_repeat_transaction_over_time(txs, median_line,
                                                 output_folder, time_label)

    # Output customers in cohort and txns observed
    model_params += f"""
Customers modeled for validation: {num_customers_cohort} \
({perc_customers_cohort}% of total customers)
Transactions observed for validation: {num_txns_val} \
({perc_txns_val} % of total transactions)
    """
    model_params = model_params.strip() + '\n\n'

    # Calculate MAPE (Mean Absolute Percent Error)
    error_by_time = (
        txs.iloc[median_line:, :]['repeat_transactions_cumulative'] -
        txs.iloc[median_line:, :]['predicted_cumulative_transactions']
    ) / txs.iloc[median_line:, :]['repeat_transactions_cumulative'] * 100
    mape = error_by_time.abs().mean()

    model_params += f"Mean Absolute Percent Error (MAPE): \
{str(round(mape, 2))}%\n"

    # return tuple that includes the MAPE, which will be used for a
    # threshold check
    return model_params, round(mape, 2)


def raise_error_if_invalid_mape(validation_result):
    """Raise error if MAPE is not good enough.

    Raises Error if Mean absolute percentage error (MAPE) for the model is
    above a certain threshold (specified in the pipeline options params).

    Args:
        validation_result: Dictionary containing the validation results from
            the calculate_model_fit_validation function.

    Returns:
        Nothing, just raise an exception to stop the pipeline if needed.
    """
    if 'invalid_mape' in validation_result:
        raise RuntimeError(validation_result['error_message'])


def calculate_model_fit_validation(_, options, dates, calcbs, repeat_tx,
                                   num_customers, num_txns):
    """Checks whether the input transactions data is good enough for the
    pipeline to continue.

    Args:
        _: Ignoring the first argument since it's only a value to trigger
            the call of this function (since it's running inside a FlatMap
            operator)
        options: Pipeline options.
        dates: Dictionary containing important border dates to use in the
            calculation.
        calcbs: customer-by-sufficient-statistic (CBS) matrix in calibration
            period.
        repeat_tx: Sorted cumulative sum of transactions for each time unit.
        num_customers: Dictionary containing statistics regarding the number
            of customers.
        num_txns: Dictionary containing statistics regarding the number
            of transactions.

    Returns:
        Predictions per customer (as lists).
        The result is wrapped in another list since this function is called
        inside a FlatMap operator.
    """
    cbs = pd.DataFrame(
        calcbs,
        columns=[
            'customer_id', 'number_of_transactions', 'average_order_value',
            'frequency', 'recency', 'total_time_observed'
        ])
    txs = pd.DataFrame(
        repeat_tx,
        columns=[
            'time_unit_number', 'repeat_transactions',
            'repeat_transactions_cumulative'
        ])

    model_time_divisor = TimeGranularityParams(
        options[_OPTION_MODEL_TIME_GRANULARITY]).get_days()

    model_params, mape = frequency_model_validation(
        model_type=options[_OPTION_FREQUENCY_MODEL_TYPE],
        cbs=cbs,
        cal_start_date=dates[_OPTION_CALIBRATION_START_DATE],
        cal_end_date=dates[_OPTION_CALIBRATION_END_DATE],
        time_divisor=model_time_divisor,
        time_label=options[_OPTION_MODEL_TIME_GRANULARITY],
        hold_end_date=dates[_OPTION_HOLDOUT_END_DATE],
        repeat_tx=txs,
        output_folder=options[_OPTION_OUTPUT_FOLDER],
        num_customers_cohort=num_customers['num_customers_cohort'],
        perc_customers_cohort=num_customers['perc_customers_cohort'],
        num_txns_val=num_txns['num_txns_val'],
        perc_txns_val=num_txns['perc_txns_val'],
        penalizer_coef=options[_OPTION_PENALIZER_COEF],
    )

    # Validate the gamma-gamma (spend) model
    gamma_gamma_validation(cbs, options[_OPTION_PENALIZER_COEF])

    # Write params to text file
    param_output = f"""Modeling Dates
Calibration Start Date: {date_to_str(dates[_OPTION_CALIBRATION_START_DATE])}
Calibration End Date: {date_to_str(dates[_OPTION_CALIBRATION_END_DATE])}
Cohort End Date: {date_to_str(dates[_OPTION_COHORT_END_DATE])}
Holdout End Date: {date_to_str(dates[_OPTION_HOLDOUT_END_DATE])}

Model Time Granularity: {options[_OPTION_MODEL_TIME_GRANULARITY].capitalize()}
{model_params}
"""

    save_to_file(options[_OPTION_OUTPUT_FOLDER] + 'validation_params.txt',
                 lambda f: f.write(bytes(param_output, 'utf8')))

    # Let's swap out the '\n' for '<br/>' and use the param_output
    # as a teaser for the model validation email.
    result = {
        'mape_meta': param_output.replace('\n', '<br/>'),
        'mape_decimal': mape
    }

    # Let's check to see if the transaction frequency error is within
    # the allowed threshold.  If so, continue the calculation.  If not,
    # send an email explaining why and stop all calculations.
    if mape > float(options[_OPTION_TRANSACTION_FREQUENCY_THRESHOLD]):
        result['invalid_mape'] = True
        result['mape'] = str(mape)
        result['error_message'] = (
            f"Mean Absolute Percent Error (MAPE) [{mape}%]"
            " exceeded the allowable threshold of "
            f"{options[_OPTION_TRANSACTION_FREQUENCY_THRESHOLD]}"
        )

    return [result]


def calculate_prediction(_, options, fullcbs, num_customers, num_txns):
    """Calculates predictions by customer.

    Args:
        _: Ignoring the first argument since it's only a value to trigger
            the call of this function (since it's running inside a FlatMap
            operator)
        options: Pipeline options.
        fullcbs: Full customer-by-sufficient-statistic (CBS) records.
        num_customers: Dictionary containing statistics regarding the number
            of customers.
        num_txns: Dictionary containing statistics regarding the number
            of transactions.

    Returns:
        Predictions per customer (as lists).
        The result is wrapped in another list since this function is called
        inside a FlatMap operator.
    """
    model_time_granularity_single = TimeGranularityParams(
        options[_OPTION_MODEL_TIME_GRANULARITY]).get_time_unit()
    prediction_period = options[_OPTION_PREDICTION_PERIOD]

    # Param output
    param_output = f"""Prediction for: {prediction_period} \
{model_time_granularity_single}
Model Time Granularity: {options[_OPTION_MODEL_TIME_GRANULARITY].capitalize()}

Customers modeled: {num_customers['num_customers_total']}
Transactions observed: {num_txns['num_txns_total']}

"""

    columns = [
        'customer_id', 'number_of_transactions', 'historical_aov', 'frequency',
        'recency', 'total_time_observed'
    ]
    if options[_OPTION_EXTRA_DIMENSION_EXISTS]:
        columns.append('extra_dimension')

    # Read in full CBS matrix
    data = pd.DataFrame(fullcbs, columns=columns)

    # Fit the model
    frequency_model_type = options[_OPTION_FREQUENCY_MODEL_TYPE]
    if frequency_model_type == _MODEL_TYPE_BGNBD:
        frequency_model = fit_bgnbd_model(data, options[_OPTION_PENALIZER_COEF])
        bgnbd_params = extract_bgnbd_params(frequency_model)

        param_output += (
            "Frequency Model: BG/NBD\n"
            "Model Parameters\n"
        )
        for key, value in bgnbd_params.items():
            param_output += f"{key}: {value}\n"

    elif frequency_model_type == _MODEL_TYPE_PNBD:
        frequency_model = fit_pnbd_model(data, options[_OPTION_PENALIZER_COEF])
        pnbd_params = extract_pnbd_params(frequency_model)

        param_output += (
            "Frequency Model: Pareto/NBD\n"
            "Model Parameters\n\n"
        )
        for key, value in pnbd_params.items():
            param_output += f"{key}: {value}\n"

    else:
        raise ValueError('Model type %s is not valid' % frequency_model_type)

    # Predict probability alive for customers
    data['p_alive'] = frequency_model.conditional_probability_alive(
        data['frequency'], data['recency'], data['total_time_observed'])

    # Predict future purchases (X weeks/days/months)
    if frequency_model_type == _MODEL_TYPE_PNBD:
        data['predicted_purchases'] = pnbd_conditional_expected_transactions(
            frequency_model, data['p_alive'], prediction_period,
            data['frequency'], data['total_time_observed'])
    else:
        data['predicted_purchases'] = \
            frequency_model.conditional_expected_number_of_purchases_up_to_time(
                prediction_period, data['frequency'],
                data['recency'], data['total_time_observed'])

    # GammaGamma (Spend)
    gamma_gamma_model = fit_gamma_gamma_model_prediction(
        data, options[_OPTION_PENALIZER_COEF])
    gamma_gamma_params = extract_gamma_gamma_params(gamma_gamma_model)
    param_output += '\nGamma-Gamma Parameters\n'
    for key, value in gamma_gamma_params.items():
        param_output += f"{key}: {value}\n"

    # Calculate FutureAOV by customer
    data['future_aov'] = gamma_gamma_model.conditional_expected_average_profit(
        data['number_of_transactions'], data['historical_aov'])

    # Compute CLV (ExpectedValue)
    data['expected_value'] = data['predicted_purchases'] * data['future_aov']

    # Modify Recency to be human-interpretable
    data['recency'] = data['total_time_observed'] - data['recency']

    # Final output
    columns = [
        'customer_id', 'p_alive', 'predicted_purchases', 'future_aov',
        'historical_aov', 'expected_value', 'frequency', 'recency',
        'total_time_observed'
    ]
    if options[_OPTION_EXTRA_DIMENSION_EXISTS]:
        columns.append('extra_dimension')
    final_no_segments = data[columns]

    # Params to text file
    save_to_file(
        options[_OPTION_OUTPUT_FOLDER] + 'prediction_params.txt',
        lambda f: f.write(bytes(param_output, 'utf8')),
    )

    return final_no_segments.values


class MinMaxDatesFn(beam.CombineFn):
    """Find min and max dates.

    Class derived from beam.CombineFn that keeps track of the min and max dates
    seen in a stream of dates.
    """
    def create_accumulator(self):
        return None, None

    def add_input(self, acc, elem):
        if not elem:
            return acc

        min_date, max_date = acc
        if not min_date or elem < min_date:
            min_date = elem
        if not max_date or elem > max_date:
            max_date = elem

        return min_date, max_date

    def merge_accumulators(self, accs):
        min_date_g = None
        max_date_g = None
        for acc in accs:
            min_date, max_date = acc
            if not min_date_g:
                min_date_g = min_date
                max_date_g = max_date
                continue
            if min_date < min_date_g:
                min_date_g = min_date
            if max_date > max_date_g:
                max_date_g = max_date
        return min_date_g, max_date_g

    def extract_output(self, acc):
        return acc


class RuntimeOptions(pipeline_options.PipelineOptions):
    """Specifies runtime options for the pipeline.

    Class defining the arguments that can be passed to the pipeline to
    customize the execution.
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(f'--{_OPTION_INPUT_CSV}')
        parser.add_value_provider_argument(f'--{_OPTION_OUTPUT_FOLDER}')
        parser.add_value_provider_argument(
            f'--{_OPTION_CUSTOMER_ID_COLUMN_POSITION}', type=int)
        parser.add_value_provider_argument(
            f'--{_OPTION_TRANSACTION_DATE_COLUMN_POSITION}', type=int)
        parser.add_value_provider_argument(
            f'--{_OPTION_SALES_COLUMN_POSITION}', type=int)
        parser.add_value_provider_argument(
            f'--{_OPTION_EXTRA_DIMENSION_COLUMN_POSITION}', type=int)
        parser.add_value_provider_argument(f'--{_OPTION_DATE_PARSING_PATTERN}')
        parser.add_value_provider_argument(
            f'--{_OPTION_MODEL_TIME_GRANULARITY}',
            default=TimeGranularityParams.GRANULARITY_WEEKLY)
        parser.add_value_provider_argument(
            f'--{_OPTION_FREQUENCY_MODEL_TYPE}', default=_MODEL_TYPE_BGNBD)
        parser.add_value_provider_argument(
            f'--{_OPTION_CALIBRATION_START_DATE}')
        parser.add_value_provider_argument(f'--{_OPTION_CALIBRATION_END_DATE}')
        parser.add_value_provider_argument(f'--{_OPTION_COHORT_END_DATE}')
        parser.add_value_provider_argument(f'--{_OPTION_HOLDOUT_END_DATE}')
        parser.add_value_provider_argument(
            f'--{_OPTION_PREDICTION_PERIOD}', default=52, type=int)
        parser.add_value_provider_argument(
            f'--{_OPTION_OUTPUT_SEGMENTS}', default=5, type=int)
        parser.add_value_provider_argument(
            f'--{_OPTION_TRANSACTION_FREQUENCY_THRESHOLD}', default=15,
            type=int)
        parser.add_value_provider_argument(
            f'--{_OPTION_PENALIZER_COEF}', default=0.0, type=float)


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
            | beam.Map(set_extra_options)
        )

        full_elog = (
            pipeline
            | beam.io.ReadFromText(
                runtime_options.input_csv, skip_header_lines=1)
            | beam.Map(lambda x: list(csv.reader([x]))[0])
            | beam.FlatMap(
                csv_line_to_list,
                pvalue.AsSingleton(options))  # (customer_id, date_str, date,
                                              #  sales, extra_dimension?)
        )

        full_elog_merged = (
            full_elog
            | beam.Filter(lambda x: x[3] > 0)  # sales > 0
            | beam.Map(lambda x: ((x[0], x[1]), x))  # key: (customer_id, date)
            | 'Group full elog by customer and date' >> beam.GroupByKey()
            | beam.Map(merge_full_elog_by_customer_and_date)  # (customer_id,
                                                              #  date_str, date,
                                                              #  sales)
        )

        min_max_dates = (
            full_elog_merged
            | beam.Map(lambda x: x[2])  # date
            | beam.CombineGlobally(MinMaxDatesFn())
            | beam.Map(min_max_dates_dict)
        )

        limits_dates = (
            min_max_dates
            | beam.FlatMap(limit_dates_boundaries, pvalue.AsSingleton(options))
        )

        cohort = (
            full_elog_merged
            | beam.FlatMap(filter_customers_in_cohort,
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
            | beam.FlatMap(count_customers,
                           pvalue.AsSingleton(cohort_count),
                           pvalue.AsSingleton(all_customer_ids_count))
        )

        cal_hol_elog = (
            full_elog_merged
            | beam.FlatMap(filter_cohort_records_in_cal_hol,
                           pvalue.AsDict(cohort_set),
                           pvalue.AsSingleton(limits_dates))
        )

        cal_hol_elog_count = (
            cal_hol_elog
            | 'Count cal hol elog entries' >> beam.combiners.Count.Globally()
        )

        calibration = (
            cal_hol_elog
            | beam.FlatMap(filter_records_in_calibration,
                           pvalue.AsSingleton(limits_dates))
        )

        num_txns_total = (
            full_elog_merged
            | beam.FlatMap(filter_records_in_cal_hol,
                           pvalue.AsSingleton(limits_dates))
            | 'Count num txns total' >> beam.combiners.Count.Globally()
        )

        num_txns = (
            pipeline
            | 'Create single elem Stream II' >> beam.Create([1])
            | beam.FlatMap(count_txns,
                           pvalue.AsSingleton(cal_hol_elog_count),
                           pvalue.AsSingleton(num_txns_total))
        )

        calcbs = (
            calibration
            | beam.Map(lambda x: (x[0], x))
            | 'Group calibration elog by customer id' >> beam.GroupByKey()
            | beam.FlatMap(
                create_cal_cbs,
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
            | beam.FlatMap(filter_first_transaction_date_records,
                           pvalue.AsDict(first_transaction_dates_by_customer))
            | beam.FlatMap(
                calculate_time_unit_numbers,  # (customer_id, date,
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
            | beam.FlatMap(calculate_cumulative_repeat_transactions,
                           pvalue.AsIter(cal_hol_elog_repeat)
                           )  # (time_unit_number, repeat_transactions,
                              #  repeat_transactions_cumulative)
        )

        _ = (
            pipeline
            | 'Create single elem Stream IV' >> beam.Create([1])
            | beam.FlatMap(calculate_model_fit_validation,
                           pvalue.AsSingleton(options),
                           pvalue.AsSingleton(limits_dates),
                           pvalue.AsIter(calcbs),
                           pvalue.AsIter(repeat_tx),
                           pvalue.AsSingleton(num_customers),
                           pvalue.AsSingleton(num_txns))
            | beam.Map(raise_error_if_invalid_mape)
        )

        fullcbs_without_extra_dimension = (
            full_elog_merged
            | beam.Map(lambda x: (x[0], x))  # key: customer_id
            | 'Group full merged elog by customer id' >> beam.GroupByKey()
            | beam.FlatMap(
                create_fullcbs,
                pvalue.AsSingleton(options),
                pvalue.AsSingleton(min_max_dates)
            )  # (customer_id, number_of_transactions, historical_aov,
               #  frequency, recency, total_time_observed)
        )

        full_elog_if_extra_dimension = (
            full_elog
            | 'Discard records if no extra dimension' >> beam.FlatMap(
                discard_if_no_extra_dimension, pvalue.AsSingleton(options))
        )

        extra_dimensions_stats = (
            full_elog_if_extra_dimension
            | beam.Map(lambda x: ((x[0], x[4]), x)
                       )  # key: (customer_id, extra_dimension)
            | 'Group full elog by customer id and extra dimension' >>
                beam.GroupByKey()
            | beam.Map(
                create_extra_dimensions_stats
            )  # (customer_id, extra_dimension, dimension_count, tot_sales,
               #  max_dimension_date)
        )

        top_dimension_per_customer = (
            extra_dimensions_stats
            | beam.Map(lambda x: (x[0], x))  # customer_id
            | 'Group extra dimension stats by customer id' >> beam.GroupByKey()
            | beam.Map(
                extract_top_extra_dimension
            )  # (customer_id, extra_dimension, dimension_count, tot_sales,
               #  max_dimension_date)
        )

        customer_dimension_map = (
            top_dimension_per_customer
            | beam.Map(
                lambda x: (x[0], x[1]))  # (customer_id, extra_dimension)
        )

        fullcbs = (
            fullcbs_without_extra_dimension
            | beam.FlatMap(
                add_top_extra_dimension_to_fullcbs, pvalue.AsSingleton(options),
                pvalue.AsDict(customer_dimension_map)
            )  # (customer_id, number_of_transactions, historical_aov,
               #  frequency, recency, total_time_observed,
               #  extra_dimension?)
        )

        prediction_by_customer_no_segments = (
            pipeline
            | 'Create single elem Stream V' >> beam.Create([1])
            | beam.FlatMap(
                calculate_prediction, pvalue.AsSingleton(options),
                pvalue.AsIter(fullcbs), pvalue.AsSingleton(num_customers),
                pvalue.AsSingleton(num_txns)
            )  # [customer_id, p_alive, predicted_purchases, future_aov,
               #  historical_aov, expected_value, frequency, recency,
               #  total_time_observed, extra_dimension?]
        )

        num_rows = (
            full_elog_merged
            | 'Count num rows in full elog merged' >>
                beam.combiners.Count.Globally()
        )

        segment_predictions_exact = (
            pipeline
            | 'Create single elem Stream VII' >> beam.Create([1])
            | beam.FlatMap(lambda _, rows_count: [True]
                 if rows_count <= _SEGMENT_PREDICTION_THRESHOLD
                 else [False],
                           pvalue.AsSingleton(num_rows))
        )

        sharded_cust_predictions_no_segments_exact, \
            sharded_cust_predictions_no_segments_hash = (
                prediction_by_customer_no_segments
                | beam.FlatMap(
                    prediction_sharded,
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
            | beam.FlatMap(split_in_ntiles_exact,
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
            | beam.FlatMap(expected_values_segment_limits,
                           pvalue.AsSingleton(options),
                           pvalue.AsIter(customer_count_by_expected_value),
                           pvalue.AsSingleton(all_customer_ids_count))
        )

        prediction_by_customer_hash = (
            sharded_cust_predictions_no_segments_hash
            | beam.Map(lambda x: x[0])
            | beam.FlatMap(split_in_ntiles_hash,
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
        )

        _ = (
            prediction_by_customer
            | beam.FlatMap(lambda x, opts: [x + ['']]
                           if not opts[_OPTION_EXTRA_DIMENSION_EXISTS] else [x],
                           pvalue.AsSingleton(options))
            | 'prediction_by_customer to CSV line' >> beam.Map(list_to_csv_line)
            | 'Write prediction_by_customer' >>
                beam.io.WriteToText(runtime_options.output_folder,
                                    header='customer_id,p_alive'
                                           ',predicted_purchases'
                                           ',future_aov,historical_aov'
                                           ',expected_value,frequency,recency'
                                           ',total_time_observed,segment'
                                           ',extra_dimension',
                                    shard_name_template='',
                                    num_shards=1,
                                    file_name_suffix=
                                        'prediction_by_customer.csv')
        )

        prediction_summary_temp = (
            prediction_by_customer
            | beam.Map(lambda x: (x[9], x))  # key: segment
            | 'Group customer predictions by segment' >> beam.GroupByKey()
            | beam.Map(generate_prediction_summary
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
                calculate_perc_of_total_customer_value, pvalue.AsSingleton(
                    tot_equity))  # (segment, average_retention_probability,
                                  #  average_predicted_customer_value,
                                  #  average_predicted_order_value,
                                  #  average_predicted_purchases,
                                  #  total_customer_value, number_of_customers,
                                  #  perc_of_total_customer_value)
        )

        _ = (
            prediction_summary
            | 'prediction_summary to CSV line' >> beam.Map(list_to_csv_line)
            | 'Write prediction_summary' >> beam.io.WriteToText(
                runtime_options.output_folder,
                header='segment,average_retention_probability'
                ',average_predicted_customer_value'
                ',average_predicted_order_value,average_predicted_purchases'
                ',total_customer_value,number_of_customers'
                ',perc_of_total_customer_value',
                shard_name_template='',
                num_shards=1,
                file_name_suffix='prediction_summary.csv')
        )

        prediction_summary_extra_dimension = (
            prediction_by_customer
            | 'Discard prediction if there is not extra dimension' >>
                beam.FlatMap(discard_if_no_extra_dimension,
                             pvalue.AsSingleton(options))
            | beam.Map(lambda x: (x[10], x))  # extra dimension
            | 'Group customer predictions by extra dimension' >>
                beam.GroupByKey()
            | beam.FlatMap(generate_prediction_summary_extra_dimension,
                           pvalue.AsSingleton(tot_equity))
        )

        _ = (
            prediction_summary_extra_dimension
            | 'prediction_summary_extra_dimension to CSV line' >>
                beam.Map(list_to_csv_line)
            |
            'Write prediction_summary_extra_dimension' >> beam.io.WriteToText(
                runtime_options.output_folder,
                header='extra_dimension,average_retention_probability'
                ',average_predicted_customer_value'
                ',average_predicted_order_value'
                ',average_predicted_purchases,total_customer_value'
                ',number_of_customers,perc_of_total_customer_value',
                shard_name_template='',
                num_shards=1,
                file_name_suffix='prediction_summary_extra_dimension.csv')
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
