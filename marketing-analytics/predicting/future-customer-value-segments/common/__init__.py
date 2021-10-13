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

import csv
import datetime as dt
import io
import logging
import math
import operator

import apache_beam as beam
import numpy as np
import pandas as pd
from apache_beam.options import value_provider
import lifetimes
import lifetimes_ext

_SEGMENT_PREDICTION_THRESHOLD = 100000

_OPTION_INPUT_CSV = 'input_csv'
_OPTION_INPUT_BQ_QUERY = 'input_bq_query'
_OPTION_INPUT_BQ_PROJECT = 'input_bq_project'
_OPTION_TEMP_GCS_LOCATION = 'temp_gcs_location'
_OPTION_OUTPUT_FOLDER = 'output_folder'
_OPTION_OUTPUT_BQ_PROJECT = 'output_bq_project'
_OPTION_OUTPUT_BQ_DATASET = 'output_bq_dataset'
_OPTION_CUSTOMER_ID_COLUMN_POSITION = 'customer_id_column_position'
_OPTION_TRANSACTION_DATE_COLUMN_POSITION = 'transaction_date_column_position'
_OPTION_SALES_COLUMN_POSITION = 'sales_column_position'
_OPTION_EXTRA_DIMENSION_COLUMN_POSITION = 'extra_dimension_column_position'
_OPTION_CUSTOMER_ID_COLUMN_NAME = 'customer_id_column_name'
_OPTION_TRANSACTION_DATE_COLUMN_NAME = 'transaction_date_column_name'
_OPTION_SALES_COLUMN_NAME = 'sales_column_name'
_OPTION_EXTRA_DIMENSION_COLUMN_NAME = 'extra_dimension_column_name'
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
_OPTION_ROUND_NUMBERS = 'round_numbers'

_MODEL_TYPE_BGNBD = 'BGNBD'
_MODEL_TYPE_MBGNBD = 'MBGNBD'
_MODEL_TYPE_BGBB = 'BGBB'
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
    if _OPTION_EXTRA_DIMENSION_COLUMN_POSITION in options:
        options[_OPTION_EXTRA_DIMENSION_EXISTS] = bool(
            options[_OPTION_EXTRA_DIMENSION_COLUMN_POSITION])
    if _OPTION_EXTRA_DIMENSION_COLUMN_NAME in options:
        options[_OPTION_EXTRA_DIMENSION_EXISTS] = bool(
            options[_OPTION_EXTRA_DIMENSION_COLUMN_NAME])
    options[_OPTION_ROUND_NUMBERS] = False if \
        options[_OPTION_ROUND_NUMBERS].lower() == 'false' else True
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
        from apache_beam.io.gcp import gcsfilesystem

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

    element = [
        line[customer_id_pos],
        line[transaction_date_pos],
        parse_date(line[transaction_date_pos], options),
        float(line[sales_pos]),
    ]

    if options[_OPTION_EXTRA_DIMENSION_EXISTS]:
        extra_dimension_pos = \
            options[_OPTION_EXTRA_DIMENSION_COLUMN_POSITION] - 1
        extra_dim = line[extra_dimension_pos]
        if isinstance(extra_dim, float):
            extra_dim = round(extra_dim)
        element.append(str(extra_dim))

    return [element]


def bq_row_to_list(row, options):
    """Creates list that pipeline can process from a BigQuery row.

    Takes a row (dict) from the input BQ table and generate a list containing
    the data needed for the calculations (with entries in the right position).

    Args:
        row: Dict containing a single row from the input BigQuery table.
        options: Pipeline options (containing also the name of the fields).

    Returns:
        A list containing the fields needed for the next steps, in the right
        order [customer_id, date, sales, extra_dimension?].
        The result is wrapped in another list since this function is called
        inside a FlatMap operator.
    """
    customer_id = row[options[_OPTION_CUSTOMER_ID_COLUMN_NAME]]
    if isinstance(customer_id, float):
        customer_id = round(customer_id)

    element = [
        str(customer_id),
        row[options[_OPTION_TRANSACTION_DATE_COLUMN_NAME]],
        parse_date(row[options[_OPTION_TRANSACTION_DATE_COLUMN_NAME]], options),
        float(row[options[_OPTION_SALES_COLUMN_NAME]]),
    ]

    if options[_OPTION_EXTRA_DIMENSION_EXISTS]:
        extra_dim = row[options[_OPTION_EXTRA_DIMENSION_COLUMN_NAME]]
        if isinstance(extra_dim, float):
            extra_dim = round(extra_dim)
        element.append(str(extra_dim))

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
            _OPTION_COHORT_START_DATE:
                parse_date_yyyymmdd(options[_OPTION_COHORT_START_DATE]),
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
            _OPTION_COHORT_START_DATE:
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
                           dt.timedelta(days=1))

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


def round_number(num, decimal_digits, options):
    if options[_OPTION_ROUND_NUMBERS]:
        return round(num, decimal_digits)
    return num


def count_customers(_, cohort_ids_count, all_customer_ids_count, options):
    """Creates a dictionary containing statistics on the number of customers.

    Args:
        _: Ignoring the first argument since it's only a value to trigger
            the call of this function (since it's running inside a FlatMap
            operator)
        cohort_ids_count: Number of customers in the cohort period of time.
        all_customer_ids_count: Number of all customers.
        options: Pipeline options.

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
            round_number(100.0 * cohort_ids_count / all_customer_ids_count, 2, options),
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


def count_txns(_, cal_hol_count, num_txns_total, options):
    """Creates a dictionary containing the info regarding the number of
    transactions.

    Args:
        _: Ignoring the first argument since it's only a value to trigger
            the call of this function (since it's running inside a FlatMap
            operator)
        cal_hol_count: Number of records in the cohort period of time.
        num_txns_total: Number of total transactions.
        options: Pipeline options.

    Returns:
         Dictionary containing the data regarding the number of transactions
         (wrapped in a list since this function is called inside a FlatMap
         operator).
    """
    return [{
        'num_txns_val': cal_hol_count,
        'num_txns_total': num_txns_total,
        'perc_txns_val': round_number(
            100.0 * cal_hol_count / num_txns_total, 2, options),
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
    result = (*entry,)

    if not options[_OPTION_EXTRA_DIMENSION_EXISTS]:
        return [result]

    customer_id = entry[0]
    if customer_id in customer_extra_dim:
        return [result + (customer_extra_dim[customer_id],)]
    return [result]


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
        round_number(entry[1], 4, options),
        round_number(entry[2], 3, options),
        round_number(entry[3], 2, options),
        round_number(entry[4], 2, options),
        round_number(entry[5], 2, options),
        entry[6],
        round_number(entry[7], 3, options),
        round_number(entry[8], 3, options),
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


def generate_prediction_summary(segment_records, options):
    """Generates prediction summary for each segment.

    Args:
        segment_records: Tuple containing the segment number and the list of
            records associated to it.
        options: Pipeline options.

    Returns:
        Tuple containing the segment and the aggregated statistics.
        The result is wrapped in a list since this function is called inside
        a FlatMap operator.
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

    average_retention_probability = round_number(p_alive_sum / count, 2,
                                                 options)
    average_predicted_customer_value = round_number(expected_value_sum / count,
                                                    2, options)
    average_predicted_order_value = round_number(future_aov_sum / count, 2,
                                                 options)
    average_predicted_purchases = round_number(predicted_purchases_sum / count,
                                               2, options)
    total_customer_value = round_number(expected_value_sum, 2, options)
    number_of_customers = count

    return [(segment, average_retention_probability,
            average_predicted_customer_value, average_predicted_order_value,
            average_predicted_purchases, total_customer_value,
            number_of_customers)]


def generate_prediction_summary_extra_dimension(extra_dim_records, tot_equity, options):
    """Generates prediction summary for each extra dimension.

    Args:
        extra_dim_records: Tuple containing the extra_dimension and the list of
            records associated to it.
        tot_equity: The total customer value.
        options: Pipeline options.

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

    average_retention_probability = round_number(p_alive_sum / count, 2,
                                                 options)
    average_predicted_customer_value = round_number(expected_value_sum / count,
                                                    2, options)
    average_predicted_order_value = round_number(future_aov_sum / count, 2,
                                                 options)
    average_predicted_purchases = round_number(predicted_purchases_sum / count,
                                               2, options)
    total_customer_value = round_number(expected_value_sum, 2, options)
    number_of_customers = count
    perc_of_total_customer_value = round_number(100 * total_customer_value
                                                / tot_equity, 2, options)

    return [(extra_dimension, average_retention_probability,
             average_predicted_customer_value, average_predicted_order_value,
             average_predicted_purchases, total_customer_value,
             number_of_customers, perc_of_total_customer_value)]


def calculate_perc_of_total_customer_value(entry, tot_equity, options):
    """Appends the percentage of total customer value to the entry.

    Args:
        entry: Entry record with segments statistics.
        tot_equity: The total equity for the customers value.
        options: Pipeline options.

    Returns:
        The input entry with the percentage of total customer value appended.
        The result is wrapped in a list since this function is called inside
        a FlatMap operator.
    """
    perc_of_total_customer_value = round_number(100 * entry[5] / tot_equity, 2,
                                                options)
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


def list_to_dict(data, keys):
    """Converts the input list into a Dictionary.

    Args:
        data: The input list
        keys: Keys to be used in the dictionary

    Returns:
        Dictionary containing a representation of the input list.
    """
    result = {}
    for i, k in enumerate(keys):
        result[k] = data[i]
    return result


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


def fit_mbgnbd_model(data, penalizer_coef=0.0):
    """Generates MBG/NBD model from the input data.

    Args:
        data: Pandas DataFrame containing the customers data.
        penalizer_coef: The coefficient applied to an l2 norm on the parameters.

    Returns:
        The MBG/NBD model.
    """
    bgf = lifetimes.ModifiedBetaGeoFitter(penalizer_coef=penalizer_coef)
    bgf.fit(data['frequency'], data['recency'], data['total_time_observed'])
    return bgf


def extract_bgnbd_params(model):
    """Extracts params from the (M)BG/NBD model

    Args:
        model: the (M)BG/NBD model.

    Returns:
        The a, b, r and alpha params of the (M)BG/NBD model.
    """
    r, alpha, a, b = model._unload_params('r', 'alpha', 'a', 'b')
    return {'r': r, 'alpha': alpha, 'a': a, 'b': b}


def fit_bgbb_model(data, penalizer_coef=0.0):
    """Generates BG/BB model from the input data.

    Args:
        data: Pandas DataFrame containing the customers data.
        penalizer_coef: The coefficient applied to an l2 norm on the parameters.

    Returns:
        The BG/BB model.
    """
    bbf = lifetimes_ext.BetaGeoBetaBinomFitter(penalizer_coef=penalizer_coef)
    bbf.fit(data['frequency'], data['recency'], data['total_time_observed'])
    return bbf


def extract_bgbb_params(model):
    """Extracts params from the BG/BB model

    Args:
        model: the BG/BB model.

    Returns:
        The alpha, beta, gamma and delta params of the BG/BB model.
    """
    alpha, beta, gamma, delta = model._unload_params(
        'alpha', 'beta', 'gamma', 'delta')
    return {'alpha': alpha, 'beta': beta, 'gamma': gamma, 'delta': delta}


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
                  time_divisor)) + 3  # 3: to account for February / leap years
                                      # Has no negative side-effect


def expected_cumulative_transactions(model_type, frequency_model, t_cal, t_tot):
    """Calculates expected cumulative transactions for each interval.

    Args:
        model_type: Type of the model in use.
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

        if model_type == _MODEL_TYPE_BGBB:
            exp_purchases = \
                frequency_model.expected_number_of_transactions_in_first_n_periods(
                n=interval)["model"].tolist()
                # (frequency, model)
        else:
            exp_purchases = frequency_model.expected_number_of_purchases_up_to_time(
                t=(interval - cust_birth[np.where(cust_birth <= interval)]))

        expected_cumulative_transactions_output.append(np.sum(exp_purchases))

    return np.around(np.array(expected_cumulative_transactions_output), 2)


def predict_txs(model_type, frequency_model, t_cal, intervals):
    """Calculates transactions by time unit predictions.

    Args:
        model_type: Type of the model in use.
        frequency_model: Model fitted on customer's data.
        t_cal: NumPy array of Total Time Observed values.
        intervals: Total number of periods to predict.

    Returns:
        Pandas DataFrame containing predicted future total purchases
    """
    expected_cumulative = expected_cumulative_transactions(
        model_type, frequency_model, t_cal, intervals)
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
    if not output_folder:
        return

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
    if not output_folder:
        return

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


def frequency_model_validation_to_text(model_params):
    """Generate readable text from validation of frequency model.

    Validates the model type on the input data and parameters and calculate the
    Mean Absolute Percent Error (MAPE).

    Args:
        model_params: Model parameters from the validation.

    Returns:
        Text string with the result of the validation.
    """
    output_text = f"""Frequency Model: {model_params['frequency_model']}

Customers modeled for validation: {model_params['num_customers_cohort']} \
({model_params['perc_customers_cohort']}% of total customers)
Transactions observed for validation: {model_params['num_transactions_validation']} \
({model_params['perc_transactions_validation']} % of total transactions)

Validation Mean Absolute Percent Error (MAPE): {model_params['validation_mape']}%"""

    return output_text


def frequency_model_validation(model_type, cbs, cal_start_date, cal_end_date,
                               time_divisor, time_label, hold_end_date,
                               repeat_tx, output_folder, num_customers_cohort,
                               perc_customers_cohort, num_txns_val,
                               perc_txns_val, penalizer_coef):
    """Validates frequency model.

    Validates the model type on the input data and parameters and calculate the
    Mean Absolute Percent Error (MAPE).

    Args:
        model_type: String defining the type of model to be used.
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
        A Dict containing the params for the model validation.
    """
    model_params = {}
    if model_type == _MODEL_TYPE_BGNBD:
        frequency_model = fit_bgnbd_model(cbs, penalizer_coef)
        model_params['frequency_model'] = 'BG/NBD'
    elif model_type == _MODEL_TYPE_MBGNBD:
        frequency_model = fit_mbgnbd_model(cbs, penalizer_coef)
        model_params['frequency_model'] = 'MBG/NBD'
    elif model_type == _MODEL_TYPE_BGBB:
        frequency_model = fit_bgbb_model(cbs, penalizer_coef)
        model_params['frequency_model'] = 'BG/BB'
    elif model_type == _MODEL_TYPE_PNBD:
        frequency_model = fit_pnbd_model(cbs, penalizer_coef)
        model_params['frequency_model'] = 'Pareto/NBD'
    else:
        raise ValueError('Model type %s is not valid' % model_type)

    # Transactions by time unit predictions
    intervals = calc_full_fit_period(cal_start_date, hold_end_date,
                                     time_divisor)
    predicted = predict_txs(model_type, frequency_model,
                            cbs['total_time_observed'].values, intervals)

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
    model_params['num_customers_cohort'] = num_customers_cohort
    model_params['perc_customers_cohort'] = perc_customers_cohort
    model_params['num_transactions_validation'] = num_txns_val
    model_params['perc_transactions_validation'] = perc_txns_val

    # Calculate MAPE (Mean Absolute Percent Error)
    # NB: Do not use this as a performance metric, as it is computed on
    #     cumulative values and will most of the time underestimate the error.
    #     We are using it only for validation of fit purposes.
    error_by_time = (
        txs.iloc[median_line:, :]['repeat_transactions_cumulative'] -
        txs.iloc[median_line:, :]['predicted_cumulative_transactions']
    ) / txs.iloc[median_line:, :]['repeat_transactions_cumulative'] * 100
    mape = error_by_time.abs().mean()

    model_params['validation_mape'] = (
        'N/A' if model_type == _MODEL_TYPE_BGBB else str(round(mape, 2)))

    # return tuple that includes the validation MAPE, which will be used for a
    # threshold check
    return model_params


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
    _, error = validation_result

    if error:
        raise RuntimeError(error)


def calculate_model_fit_validation_to_text(model_params, options):
    """Generate readable text from validation of frequency model and save
    it to a file.

    Args:
        model_params: Model parameters from the validation.
        options: Pipeline options.

    Returns:
        Save results to file.
    """
    param_output = f"""Modeling Dates
Calibration Start Date: {model_params['calibration_start_date']}
Calibration End Date: {model_params['calibration_end_date']}
Cohort Start Date: {model_params['cohort_start_date']}
Cohort End Date: {model_params['cohort_end_date']}
Holdout End Date: {model_params['holdout_end_date']}

Model Time Granularity: {model_params['model_time_granularity']}
{frequency_model_validation_to_text(model_params['model'])}
"""

    save_to_file(options[_OPTION_OUTPUT_FOLDER] + 'validation_params.txt',
                 lambda f: f.write(bytes(param_output, 'utf8')))


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

    model_params = frequency_model_validation(
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

    validation_params = {
        'calibration_start_date': date_to_str(dates[_OPTION_CALIBRATION_START_DATE]),
        'calibration_end_date': date_to_str(dates[_OPTION_CALIBRATION_END_DATE]),
        'cohort_start_date': date_to_str(dates[_OPTION_COHORT_START_DATE]),
        'cohort_end_date': date_to_str(dates[_OPTION_COHORT_END_DATE]),
        'holdout_end_date': date_to_str(dates[_OPTION_HOLDOUT_END_DATE]),
        'model_time_granularity': options[_OPTION_MODEL_TIME_GRANULARITY].capitalize(),
        'model': model_params,
    }

    # Let's check to see if the transaction frequency error is within
    # the allowed threshold.  If so, continue the calculation.  If not,
    # fail with an error and stop all calculations.
    error = None
    if (
            options[_OPTION_FREQUENCY_MODEL_TYPE] != _MODEL_TYPE_BGBB and
            float(model_params['validation_mape']) > float(
                options[_OPTION_TRANSACTION_FREQUENCY_THRESHOLD])):
        model_params['invalid_mape'] = True
        error = (
            f"Validation Mean Absolute Percent Error (MAPE) [{model_params['validation_mape']}%]"
            " exceeded the allowable threshold of "
            f"{options[_OPTION_TRANSACTION_FREQUENCY_THRESHOLD]}"
        )

    return [(validation_params, error)]


def model_params_to_string(model_params):
    if not model_params:
        return ''

    result = ''
    for key, val in model_params.items():
        result += f'{key}: {val}\n'
    return result


def calculate_prediction_to_text(prediction_params, options):
    """Generate readable text prediction parameters and save it to a file.

    Args:
        prediction_params: Parameters generated during the calculation.
        options: Pipeline options.

    Returns:
        Save results to file.
    """
    param_output = f"""Prediction for: {prediction_params['prediction_period']} \
{prediction_params['prediction_period_unit']}
Model Time Granularity: {prediction_params['model_time_granularity']}

Customers modeled: {prediction_params['customers_modeled']}
Transactions observed: {prediction_params['transactions_observed']}

Frequency Model: {prediction_params['frequency_model']}
Model Parameters
{model_params_to_string(prediction_params['bgnbd_model_params'] or
                        prediction_params['bgbb_model_params'] or
                        prediction_params['paretonbd_model_params'])}
Gamma-Gamma Parameters
{model_params_to_string(prediction_params['gamma_gamma_params'])}"""

    # Params to text file
    save_to_file(
        options[_OPTION_OUTPUT_FOLDER] + 'prediction_params.txt',
        lambda f: f.write(bytes(param_output, 'utf8')),
    )


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
        Predictions per customer (as lists) and prediction parameters Dict.
        The result is wrapped in another list since this function is called
        inside a FlatMap operator.
    """
    model_time_granularity_single = TimeGranularityParams(
        options[_OPTION_MODEL_TIME_GRANULARITY]).get_time_unit()
    prediction_period = options[_OPTION_PREDICTION_PERIOD]

    prediction_params = {
        'prediction_period': prediction_period,
        'prediction_period_unit': model_time_granularity_single,
        'model_time_granularity': options[_OPTION_MODEL_TIME_GRANULARITY]
            .capitalize(),
        'customers_modeled': num_customers['num_customers_total'],
        'transactions_observed': num_txns['num_txns_total']
    }

    columns = [
        'customer_id', 'number_of_transactions', 'historical_aov', 'frequency',
        'recency', 'total_time_observed'
    ]

    # Read in full CBS matrix
    data = pd.DataFrame(fullcbs, columns=columns)

    # Fit the model
    frequency_model_type = options[_OPTION_FREQUENCY_MODEL_TYPE]
    if frequency_model_type == _MODEL_TYPE_BGNBD:
        frequency_model = fit_bgnbd_model(data, options[_OPTION_PENALIZER_COEF])
        bgnbd_params = extract_bgnbd_params(frequency_model)

        prediction_params['frequency_model'] = 'BG/NBD'
        prediction_params['bgnbd_model_params'] = bgnbd_params
        prediction_params['bgbb_model_params'] = None
        prediction_params['paretonbd_model_params'] = None

    elif frequency_model_type == _MODEL_TYPE_MBGNBD:
        frequency_model = fit_mbgnbd_model(data, options[_OPTION_PENALIZER_COEF])
        mbgnbd_params = extract_bgnbd_params(frequency_model)

        prediction_params['frequency_model'] = 'MBG/NBD'
        prediction_params['bgnbd_model_params'] = mbgnbd_params
        prediction_params['bgbb_model_params'] = None
        prediction_params['paretonbd_model_params'] = None

    elif frequency_model_type == _MODEL_TYPE_BGBB:
        frequency_model = fit_bgbb_model(data, options[_OPTION_PENALIZER_COEF])
        bgbb_params = extract_bgbb_params(frequency_model)

        prediction_params['frequency_model'] = 'BG/BB'
        prediction_params['bgnbd_model_params'] = None
        prediction_params['bgbb_model_params'] = bgbb_params
        prediction_params['paretonbd_model_params'] = None

    elif frequency_model_type == _MODEL_TYPE_PNBD:
        frequency_model = fit_pnbd_model(data, options[_OPTION_PENALIZER_COEF])
        pnbd_params = extract_pnbd_params(frequency_model)

        prediction_params['frequency_model'] = 'Pareto/NBD'
        prediction_params['bgnbd_model_params'] = None
        prediction_params['bgbb_model_params'] = None
        prediction_params['paretonbd_model_params'] = pnbd_params

    else:
        raise ValueError('Model type %s is not valid' % frequency_model_type)

    # Predict probability alive for customers
    if frequency_model_type == _MODEL_TYPE_BGBB:
        data['p_alive'] = frequency_model.conditional_probability_alive(
            prediction_period, data['frequency'], data['recency'],
            data['total_time_observed'])
    else:
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
    prediction_params['gamma_gamma_params'] = gamma_gamma_params

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
    final_no_segments = data[columns]

    return [[final_no_segments.values, prediction_params]]


def clean_nan_and_inf(entry):
    result = []
    for x in entry:
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            logging.getLogger().log(logging.WARNING,
                                    f'NaN or Inf found in {str(entry)}')
            result.append(0)
        else:
            result.append(x)

    return result


class MinMaxDatesFn(beam.CombineFn):
    """Find min and max dates.

    Class derived from beam.CombineFn that keeps track of the min and max dates
    seen in a stream of dates.
    """
    def create_accumulator(self):
        return dt.datetime.max, dt.datetime.min

    def add_input(self, acc, elem):
        if not elem:
            return acc

        min_date, max_date = acc
        if elem < min_date:
            min_date = elem
        elif elem > max_date:
            max_date = elem

        return min_date, max_date

    def merge_accumulators(self, accs):
        min_date_g = dt.datetime.max
        max_date_g = dt.datetime.min
        for acc in accs:
            min_date, max_date = acc
            if min_date < min_date_g:
                min_date_g = min_date
            if max_date > max_date_g:
                max_date_g = max_date
        return min_date_g, max_date_g

    def extract_output(self, acc):
        return acc


class TableValueProvider(value_provider.ValueProvider):
    def __init__(self, project_vp, dataset_vp, table_name):
        self.project_vp = project_vp
        self.dataset_vp = dataset_vp
        self.table_name = table_name

    def is_accessible(self):
        return self.project_vp.is_accessible() and self.dataset_vp.is_accessible()

    def get(self):
        return f'{self.project_vp.get()}:{self.dataset_vp.get()}.{self.table_name}'
