import argparse
import csv
import io
import logging
import math
from datetime import datetime
from datetime import timedelta
from operator import itemgetter

import apache_beam as beam
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.options.value_provider import ValueProvider
from apache_beam.transforms.util import Distinct
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsDict, AsIter, AsSingleton

import pandas as pd
import numpy as np
from lifetimes import BetaGeoFitter, ParetoNBDFitter, GammaGammaFitter

date_formats = {
    'YYYY-MM-DD': '%Y-%m-%d',
    'MM/DD/YY': '%m/%d/%y',
    'MM/DD/YYYY': '%m/%d/%Y',
    'DD/MM/YY': '%d/%m/%y',
    'DD/MM/YYYY': '%d/%m/%Y',
    'YYYYMMDD': '%Y%m%d',
}


def parse_date(date, options):
    return datetime.strptime(date, date_formats[options['date_parsing_pattern']])


def parse_date_yyyymmdd(date):
    return datetime.strptime(date, '%Y-%m-%d')


def date_to_str(date):
    return date.strftime('%Y-%m-%d')


def time_granularity_params(granularity):
    granularity = (granularity or '').lower()
    if granularity == 'daily':
        return 1, 'days'
    if granularity == 'weekly':
        return 7, 'weeks'
    if granularity == 'monthly':
        return 30, 'months'
    raise ValueError('Model time granularity specified is not one of the following valid options: '
                     '[daily, weekly, monthly]')


def set_extra_options(options):
    options['extra_dimension_exists'] = bool(options['extra_dimension_column_position'])
    return options


def save_to_file(output_path, save_function):
    if output_path.startswith('gs://'):
        with GCSFileSystem(None).create(output_path) as f:
            save_function(f)
    else:
        with open(output_path, 'wb') as f:
            save_function(f)


def csv_line_to_tuple(line, options):
    customer_id_pos = options['customer_id_column_position'] - 1
    transaction_date_pos = options['transaction_date_column_position'] - 1
    sales_pos = options['sales_column_position'] - 1
    extra_dimension_exists = options['extra_dimension_exists']

    if extra_dimension_exists:
        extra_dimension_pos = options['extra_dimension_column_position'] - 1
        return [(
            line[customer_id_pos],
            line[transaction_date_pos],
            parse_date(line[transaction_date_pos], options),
            float(line[sales_pos]),
            line[extra_dimension_pos],
        )]

    return [(
        line[customer_id_pos],
        line[transaction_date_pos],
        parse_date(line[transaction_date_pos], options),
        float(line[sales_pos]),
    )]


def merge_full_elog_by_customer_and_date(entry):
    (customer_id, date_str), records = entry
    sales = 0.0
    date = None
    for r in records:
        if not date:
            date = r[2]
        sales += r[3]

    return customer_id, date_str, date, round(sales, 2)


def min_max_dates_dict(min_max_dates):
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


def limit_dates_dict(dates, options):
    if options['calibration_start_date']:
        limit_dates = {
            'auto_date_selection': False,
            'calibration_start_date': parse_date_yyyymmdd(options['calibration_start_date']),
            'cohort_end_date': parse_date_yyyymmdd(options['cohort_end_date']),
            'calibration_end_date': parse_date_yyyymmdd(options['calibration_end_date']),
            'holdout_end_date': parse_date_yyyymmdd(options['holdout_end_date']),
        }
    else:
        limit_dates = {
            'auto_date_selection': True,
            'calibration_start_date': dates['min_date'],
            'cohort_end_date': dates['min_date'] + timedelta(days=dates['cohort_days']),
            'calibration_end_date': dates['min_date'] + timedelta(days=dates['half_days']),
            'holdout_end_date': dates['max_date'],
        }

    limit_dates.update(
        holdout_start_date=limit_dates['calibration_end_date'] + timedelta(days=1),
        cohort_start_date=limit_dates['calibration_start_date']
    )

    return [limit_dates]


def customers_in_cohort(entry, dates):
    if dates['cohort_start_date'] <= entry[2] <= dates['cohort_end_date']:
        return [entry[0]]
    return []


def count_customers(_, cohort_ids_count, all_customer_ids_count):
    return [{
        'num_customers_cohort': cohort_ids_count,
        'num_customers_total': all_customer_ids_count,
        'perc_customers_cohort': round(100.0 * cohort_ids_count / all_customer_ids_count, 2),
    }]


def cohort_records_in_cal_hol(entry, cohort, dates):
    if dates['calibration_start_date'] <= entry[2] <= dates['holdout_end_date'] and \
            entry[0] in cohort:
        return [entry]
    return []


def records_in_cal_hol(entry, dates):
    if dates['calibration_start_date'] <= entry[2] <= dates['holdout_end_date']:
        return [entry]
    return []


def records_in_calibration(entry, dates):
    if dates['calibration_start_date'] <= entry[2] <= dates['calibration_end_date']:
        return [entry]
    return []


def count_txns(_, cal_hol_count, num_txns_total):
    return [{
        'num_txns_val': cal_hol_count,
        'num_txns_total': num_txns_total,
        'perc_txns_val': round(100.0 * cal_hol_count / num_txns_total, 2),
    }]


def create_cal_cbs(customer_records, options, dates):
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
    model_time_divisor, _ = time_granularity_params(options['model_time_granularity'])
    frequency = number_of_transactions - 1
    recency = (max_date - min_date).days / model_time_divisor
    total_time_observed = ((dates['calibration_end_date'] - min_date).days + 1) / model_time_divisor

    return [(
        customer_id,
        number_of_transactions,
        average_order_value,
        frequency,
        recency,
        total_time_observed
    )]


def create_fullcbs(customer_records, options, dates):
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
    model_time_divisor, _ = time_granularity_params(options['model_time_granularity'])
    frequency = number_of_transactions - 1
    recency = (max_date - min_date).days / model_time_divisor
    total_time_observed = ((dates['max_date'] - min_date).days + 1) / model_time_divisor

    return [(
        customer_id,
        number_of_transactions,
        historical_aov,
        frequency,
        recency,
        total_time_observed,
    )]


def create_extra_dimensions_stats(customer_records_by_extra_dim):
    (customer_id, extra_dimension), records = customer_records_by_extra_dim

    dimension_count = 0
    tot_sales = 0.0
    max_dimension_date = None
    for r in records:
        dimension_count += 1
        tot_sales += r[3]
        if not max_dimension_date or r[2] > max_dimension_date:
            max_dimension_date = r[2]

    return customer_id, extra_dimension, dimension_count, tot_sales, max_dimension_date


def extract_top_extra_dimension(customer_records):
    _, records = customer_records
    top_record = None
    for r in records:
        if not top_record:
            top_record = r
            continue
        if r[2] > top_record[2] or \
                r[2] == top_record[2] and r[4] > top_record[4] or \
                r[2] == top_record[2] and r[4] == top_record[4] and r[3] > top_record[3]:
            # 2: dimension_count, 3: tot_sales, 4: max_dimension_date
            top_record = r

    return top_record


def add_top_extra_dimension_to_fullcbs(entry, options, customer_extra_dim):
    if not options['extra_dimension_exists']:
        return [entry]

    customer_id = entry[0]
    if customer_id in customer_extra_dim:
        return [entry + (customer_extra_dim[customer_id],)]
    return [entry]


def discard_if_no_extra_dimension(entry, options):
    if not options['extra_dimension_exists']:
        return []
    return [entry]


def filter_first_transaction_date_records(entry, first_trans):
    if entry[2] == first_trans.get(entry[0]):  # date == first_transaction[customer_id]
        return []
    return [entry]


def calculate_time_unit_numbers(entry, options, dates):
    csdp1 = dates['calibration_start_date'] + timedelta(days=1)
    model_time_divisor, _ = time_granularity_params(options['model_time_granularity'])

    # (customer_id, date, time_unit_number)
    return [(entry[0], entry[2], math.floor((entry[2] - csdp1).days / model_time_divisor) + 1)]


def calculate_cumulative_repeat_transactions(_, repeats):
    sorted_repeats = sorted(repeats, key=itemgetter(0))
    tot = 0
    for k, v in sorted_repeats:
        tot += v
        yield k, v, tot  # (time_unit_number, repeat_transactions, repeat_transactions_cumulative)


def prediction_sharded(entry, options, use_exact_method):
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
    if options['extra_dimension_exists']:
        result.append(entry[9])

    return [(result, use_exact_method)]


def split_in_ntiles_exact(_, options, entries):
    sorted_entries = sorted(
        map(itemgetter(0), entries),
        key=itemgetter(5),  # 5 -> expected_value
        reverse=True)

    output_segments = options['output_segments']
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


def expected_values_segment_limits(_, options, expected_values_count, num_customers):
    sorted_expected_values_count = sorted(expected_values_count, key=itemgetter(0), reverse=True)
    result = []

    output_segments = options['output_segments']
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

    return (
        segment,
        average_retention_probability,
        average_predicted_customer_value,
        average_predicted_order_value,
        average_predicted_purchases,
        total_customer_value,
        number_of_customers
    )


def generate_prediction_summary_extra_dimension(extra_dim_records, tot_equity):
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
    perc_of_total_customer_value = str(round(100 * total_customer_value / tot_equity, 2)) + '%'

    return [(
        extra_dimension,
        average_retention_probability,
        average_predicted_customer_value,
        average_predicted_order_value,
        average_predicted_purchases,
        total_customer_value,
        number_of_customers,
        perc_of_total_customer_value
    )]


def calculate_perc_of_total_customer_value(entry, tot_equity):
    perc_of_total_customer_value = str(round(100 * entry[5] / tot_equity, 2)) + '%'
    return [entry + (perc_of_total_customer_value,)]


def list_to_csv_line(data):
    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL, lineterminator='')
    writer.writerow(data)
    return output.getvalue()


def fit_bgnbd_model(data, penalizer_coef=0.0):
    # fit the model
    bgf = BetaGeoFitter(penalizer_coef=penalizer_coef)
    bgf.fit(data['frequency'], data['recency'], data['total_time_observed'])
    return bgf


def extract_bgnbd_params(model):
    r, alpha, a, b = model._unload_params('r', 'alpha', 'a', 'b')
    params = {'r': r, 'alpha': alpha, 'a': a, 'b': b}
    return params


def fit_pnbd_model(data, penalizer_coef=0.0):
    # fit the model
    pareto = ParetoNBDFitter(penalizer_coef=penalizer_coef)
    pareto.fit(data['frequency'], data['recency'], data['total_time_observed'])
    return pareto


def extract_pnbd_params(model):
    r, alpha, s, beta = model._unload_params('r', 'alpha', 's', 'beta')
    params = {'r': r, 'alpha': alpha, 's': s, 'beta': beta}
    return params


def pnbd_conditional_expected_transactions(model, p_alive, t, frequency, T):
    params = extract_pnbd_params(model)
    r = params['r']
    alpha = params['alpha']
    s = params['s']
    beta = params['beta']

    p1 = (r + frequency) * (beta + T) / ((alpha + T) * (s - 1))
    p2 = (1 - ((beta + T) / (beta + T + t)) ** (s - 1))
    p3 = p_alive

    return p1 * p2 * p3


def extract_gamma_gamma_params(model):
    p, q, v = model._unload_params('p', 'q', 'v')
    params = {
        'p': p,
        'q': q,
        'v': v,
    }
    return params


def calc_full_fit_period(calibration_start_date, holdout_end_date, time_divisor):
    return int(
        math.ceil((holdout_end_date - calibration_start_date).days / time_divisor)) + 3  # Just in case, doesn't hurt


def expected_cumulative_transactions(frequency_model, t_cal, t_tot):
    """
    :param frequency_model:
    :param t_cal: np.array of Total Time Observed values
    :param t_tot: Total number of periods to predict
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
    expected_cumulative = expected_cumulative_transactions(frequency_model, t_cal, intervals)
    expected_incremental = expected_cumulative - np.delete(
        np.hstack(([0], expected_cumulative)), expected_cumulative.size - 1)

    predicted = pd.DataFrame(
        data=np.vstack((expected_incremental, expected_cumulative)).transpose(),
        columns=['predicted_transactions', 'predicted_cumulative_transactions'])

    predicted['time_unit_number'] = predicted.index.values + 1
    return predicted


def calc_calibration_period(calibration_start_date, calibration_end_date, time_divisor):
    return int(math.ceil(((calibration_end_date - calibration_start_date).days / time_divisor)))


def plot_repeat_transaction_over_time(data_frame, median, output_folder, time_label):
    import matplotlib
    matplotlib.use('Agg')
    from matplotlib import pyplot as plt

    if time_label == 'daily':
        time_label_short = 'Day'
    elif time_label == 'monthly':
        time_label_short = 'Month'
    else:
        time_label_short = 'Week'

    txs = data_frame[[
        'time_unit_number', 'repeat_transactions', 'predicted_transactions'
    ]]
    txs.columns = [time_label_short, 'Actual', 'Model']
    ax = txs.plot(kind='line', x=time_label_short, style=['-', '--'])

    # Median line that shows split between calibration and holdout period
    plt.axvline(median, color='k', linestyle='--')

    plt.legend()
    plt.title('Tracking %s Transactions' % time_label.capitalize())
    plt.ylabel('Transactions')
    plt.xlabel(time_label_short)

    # Save to file
    save_to_file(
        output_folder + 'repeat_transactions_over_time.png',
        lambda f: plt.savefig(f, bbox_inches='tight')
    )

    return ax


def plot_cumulative_repeat_transaction_over_time(data_frame, median, output_folder, time_label):
    import matplotlib
    matplotlib.use('Agg')
    from matplotlib import pyplot as plt

    if time_label == 'daily':
        time_label_short = 'Day'
    elif time_label == 'monthly':
        time_label_short = 'Month'
    else:
        time_label_short = 'Week'

    txs = data_frame[[
        'time_unit_number', 'repeat_transactions_cumulative', 'predicted_cumulative_transactions'
    ]]
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
        lambda f: plt.savefig(f, bbox_inches='tight')
    )

    return ax


def fit_gamma_gamma_model(data, penalizer_coef=0.0):
    # fit the model
    ggf = GammaGammaFitter(penalizer_coef=penalizer_coef)
    ggf.fit(data['number_of_transactions'], data['average_order_value'])
    return ggf


def fit_gamma_gamma_model_prediction(data, penalizer_coef=0.0):
    # fit the model
    ggf = GammaGammaFitter(penalizer_coef=penalizer_coef)
    ggf.fit(data['number_of_transactions'], data['historical_aov'])
    return ggf


def gamma_gamma_validation(cbs, penalizer_coef):
    # Fit the model
    model = fit_gamma_gamma_model(cbs, penalizer_coef)

    # Predicted AOV (fitted off of calibration only)
    cbs['future_aov'] = model.conditional_expected_average_profit(
        cbs['number_of_transactions'], cbs['average_order_value'])


def frequency_model_validation(model_type, cbs, cal_start_date, cal_end_date,
                               time_divisor, time_label, hold_end_date,
                               repeat_tx, output_folder,
                               num_customers_cohort, perc_customers_cohort,
                               num_txns_val, perc_txns_val, penalizer_coef):
    # Fit the model
    if model_type == 'BGNBD':
        frequency_model = fit_bgnbd_model(cbs, penalizer_coef)
        model_params = 'Frequency Model: BG/NBD\n\n'
    elif model_type == 'PNBD':
        frequency_model = fit_pnbd_model(cbs, penalizer_coef)
        model_params = 'Frequency Model: Pareto/NBD\n\n'
    else:
        raise ValueError('Model type %s is not valid' % model_type)

    # Transactions by tiem unit predictions
    intervals = calc_full_fit_period(cal_start_date, hold_end_date, time_divisor)
    predicted = predict_txs(frequency_model, cbs['total_time_observed'].values, intervals)

    # Actual transactions per time unit
    txs = repeat_tx

    # Join predicted to actual
    txs = txs.merge(predicted, how='inner', on='time_unit_number')

    # Remove the last row as it often has partial data from an incomplete week
    txs = txs.drop(txs.index[len(txs) - 1])

    # Location of the median line for validation plots
    median_line = calc_calibration_period(cal_start_date, cal_end_date, time_divisor)

    # Plot creation
    plot_repeat_transaction_over_time(txs, median_line, output_folder, time_label)
    plot_cumulative_repeat_transaction_over_time(txs, median_line, output_folder, time_label)

    # Output customers in cohort and txns observed
    model_params += ('Customers modeled for validation: ' +
                     str(num_customers_cohort) +
                     ' (' +
                     str(perc_customers_cohort) + '% of total customers)')
    model_params += ('\nTransactions observed for validation: ' +
                     str(num_txns_val) +
                     ' (' +
                     str(perc_txns_val) +
                     '% of total transactions)')
    model_params += '\n\n'

    # Calculate MAPE (Mean Absolute Percent Error)
    error_by_time = (
                            txs.iloc[median_line:, :]['repeat_transactions_cumulative'] -
                            txs.iloc[median_line:, :]['predicted_cumulative_transactions']
                    ) / txs.iloc[median_line:, :]['repeat_transactions_cumulative'] * 100
    mape = error_by_time.abs().mean()

    model_params += ('Mean Absolute Percent Error (MAPE): ' + str(round(mape, 2)) + '%\n')

    # return tuple that includes the MAPE, which wil be used for a threshold check
    return model_params, round(mape, 2)


def raise_error_if_invalid_mape(validation_result):
    if 'invalid_mape' in validation_result:
        raise RuntimeError(validation_result['error_message'])


def calculate_model_fit_validation(_, options, dates, calcbs, repeat_tx, num_customers, num_txns):
    # calcbs = (customer_id, number_of_transactions, average_order_value, frequency, recency, total_time_observed)
    # repeat_tx = (time_unit_number, repeat_transactions, repeat_transactions_cumulative)
    cbs = pd.DataFrame(calcbs, columns=['customer_id', 'number_of_transactions', 'average_order_value',
                                        'frequency', 'recency', 'total_time_observed'])
    txs = pd.DataFrame(repeat_tx, columns=['time_unit_number', 'repeat_transactions', 'repeat_transactions_cumulative'])
    model_time_divisor, _ = time_granularity_params(options['model_time_granularity'])

    model_params, mape = frequency_model_validation(
        model_type=options['frequency_model_type'],
        cbs=cbs,
        cal_start_date=dates['calibration_start_date'],
        cal_end_date=dates['calibration_end_date'],
        time_divisor=model_time_divisor,
        time_label=options['model_time_granularity'],
        hold_end_date=dates['holdout_end_date'],
        repeat_tx=txs,
        output_folder=options['output_folder'],
        num_customers_cohort=num_customers['num_customers_cohort'],
        perc_customers_cohort=num_customers['perc_customers_cohort'],
        num_txns_val=num_txns['num_txns_val'],
        perc_txns_val=num_txns['perc_txns_val'],
        penalizer_coef=options['penalizer_coef'],
    )

    # Validate the gamma-gamma (spend) model
    gamma_gamma_validation(cbs, options['penalizer_coef'])

    # Write params to text file
    param_output = 'Modeling Dates'
    param_output += '\nCalibration Start Date: ' + date_to_str(dates['calibration_start_date'])
    param_output += '\nCalibration End Date: ' + date_to_str(dates['calibration_end_date'])
    param_output += '\nCohort End Date: ' + date_to_str(dates['cohort_end_date'])
    param_output += '\nHoldout End Date: ' + date_to_str(dates['holdout_end_date'])
    param_output += '\n\n'
    param_output += 'Model Time Granularity: ' + (
        options['model_time_granularity'].capitalize()) + '\n'
    param_output += (model_params + '\n')
    save_to_file(
        options['output_folder'] + 'validation_params.txt',
        lambda f: f.write(bytes(param_output, 'utf8'))
    )

    # Let's swap out the '\n' for '<br/>' and use the param_output
    # as a teaser for the model validation email.
    result = {
        'mape_meta': param_output.replace('\n', '<br/>'),
        'mape_decimal': mape
    }

    # Let's check to see if the transaction frequency error is within
    # the allowed threshold.  If so, continue the calculation.  If not,
    # send an email explaining why and stop all calculations.
    if mape > float(options['transaction_frequency_threshold']):
        err_str = 'Mean Absolute Percent Error (MAPE) [' + str(mape) + '%]'
        err_str += ' exceeded the allowable threshold of '
        err_str += str(options['transaction_frequency_threshold'])
        result['invalid_mape'] = True
        result['mape'] = str(mape)
        result['error_message'] = err_str

    return [result]


def calculate_prediction(_, options, fullcbs, num_customers, num_txns):
    _, model_time_granularity_single = time_granularity_params(options['model_time_granularity'])
    prediction_period = options['prediction_period']

    # Param output
    param_output = 'Prediction for: ' + str(prediction_period) + ' '
    param_output += model_time_granularity_single + '\n'
    param_output += 'Model Time Granularity: ' + (
        options['model_time_granularity'].capitalize()) + '\n'

    # Output number of customers and transactions
    param_output += ('\nCustomers modeled: ' + str(num_customers['num_customers_total']))
    param_output += ('\nTransactions observed: ' + str(num_txns['num_txns_total']))
    param_output += '\n\n'

    # Read in full CBS matrix

    columns = ['customer_id', 'number_of_transactions', 'historical_aov', 'frequency', 'recency', 'total_time_observed']
    if options['extra_dimension_exists']:
        columns.append('extra_dimension')

    data = pd.DataFrame(fullcbs, columns=columns)

    # Fit the model
    frequency_model_type = options['frequency_model_type']
    if frequency_model_type == 'BGNBD':
        frequency_model = fit_bgnbd_model(data, options['penalizer_coef'])
        bgnbd_params = extract_bgnbd_params(frequency_model)

        param_output += 'Frequency Model: BG/NBD\n'
        param_output += 'Model Parameters\n'
        for key, value in bgnbd_params.items():
            param_output += (key + ': ' + str(value) + '\n')

    elif frequency_model_type == 'PNBD':
        frequency_model = fit_pnbd_model(data, options['penalizer_coef'])
        pnbd_params = extract_pnbd_params(frequency_model)

        param_output += 'Frequency Model: Pareto/NBD\n'
        param_output += 'Model Parameters\n'
        for key, value in pnbd_params.items():
            param_output += (key + ': ' + str(value) + '\n')

    else:
        raise ValueError('Model type %s is not valid' % frequency_model_type)

    # Predict probability alive for customers
    data['p_alive'] = frequency_model.conditional_probability_alive(
        data['frequency'], data['recency'], data['total_time_observed'])

    # Predict future purchases (X weeks/days/months)
    if frequency_model_type == 'PNBD':
        data['predicted_purchases'] = pnbd_conditional_expected_transactions(
            frequency_model, data['p_alive'], prediction_period,
            data['frequency'], data['total_time_observed'])
    else:
        data['predicted_purchases'] = \
            frequency_model.conditional_expected_number_of_purchases_up_to_time(
                prediction_period, data['frequency'],
                data['recency'], data['total_time_observed'])

    # GammaGamma (Spend)
    gamma_gamma_model = fit_gamma_gamma_model_prediction(data, options['penalizer_coef'])
    gamma_gamma_params = extract_gamma_gamma_params(gamma_gamma_model)
    param_output += '\nGamma-Gamma Parameters\n'
    for key, value in gamma_gamma_params.items():
        param_output += (key + ': ' + str(value) + '\n')

    # Calculate FutureAOV by customer
    data['future_aov'] = gamma_gamma_model.conditional_expected_average_profit(
        data['number_of_transactions'], data['historical_aov'])

    # Compute CLV (ExpectedValue)
    data['expected_value'] = data['predicted_purchases'] * data['future_aov']

    # Modify Recency to be human-interpretable
    data['recency'] = data['total_time_observed'] - data['recency']

    # Final output
    columns = ['customer_id', 'p_alive', 'predicted_purchases', 'future_aov', 'historical_aov',
               'expected_value', 'frequency', 'recency', 'total_time_observed']
    if options['extra_dimension_exists']:
        columns.append('extra_dimension')
    final_no_segments = data[columns]

    # Write the prediction by customer file without segments to temp folder
    # save_to_file(
    #     options['output_folder'] + 'prediction_by_customer_no_segments.csv',
    #     lambda f: f.write(bytes(final_no_segments.to_csv(), 'utf8')),
    # )

    # Params to text file
    save_to_file(
        options['output_folder'] + 'prediction_params.txt',
        lambda f: f.write(bytes(param_output, 'utf8')),
    )

    return final_no_segments.values
    # return final_no_segments.to_dict('records')


class MinMaxDatesFn(beam.CombineFn):
    def create_accumulator(self, *args, **kwargs):
        return None, None

    def add_input(self, acc, elem, *args, **kwargs):
        if not elem:
            return acc

        min_date, max_date = acc
        if not min_date or elem < min_date:
            min_date = elem
        if not max_date or elem > max_date:
            max_date = elem

        return min_date, max_date

    def merge_accumulators(self, accs, *args, **kwargs):
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

    def extract_output(self, acc, *args, **kwargs):
        return acc


class RuntimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input_csv')
        parser.add_value_provider_argument('--output_folder')
        parser.add_value_provider_argument('--customer_id_column_position', type=int)
        parser.add_value_provider_argument('--transaction_date_column_position', type=int)
        parser.add_value_provider_argument('--sales_column_position', type=int)
        parser.add_value_provider_argument('--extra_dimension_column_position', type=int)
        parser.add_value_provider_argument('--date_parsing_pattern')
        parser.add_value_provider_argument('--model_time_granularity', default='Weekly')
        parser.add_value_provider_argument('--frequency_model_type', default='BGNBD')
        parser.add_value_provider_argument('--calibration_start_date')
        parser.add_value_provider_argument('--calibration_end_date')
        parser.add_value_provider_argument('--cohort_end_date')
        parser.add_value_provider_argument('--holdout_end_date')
        parser.add_value_provider_argument('--prediction_period', default=52, type=int)
        parser.add_value_provider_argument('--output_segments', default=5, type=int)
        parser.add_value_provider_argument('--transaction_frequency_threshold', default=15, type=int)
        parser.add_value_provider_argument('--penalizer_coef', default=0.0, type=float)


def run(argv=None):
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    runtime_options = pipeline_options.view_as(RuntimeOptions)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        options = (
            pipeline
            | 'Create single element Stream containing options dict' >>
            beam.Create([pipeline_options.get_all_options()])
            | beam.Map(lambda x: {
                  k: v.get() if isinstance(v, ValueProvider) else v
                  for (k, v) in x.items()
              })
            | beam.Map(set_extra_options)
        )

        full_elog = (
            pipeline
            | beam.io.ReadFromText(runtime_options.input_csv, skip_header_lines=1)
            | beam.Map(lambda x: list(csv.reader([x]))[0])
            | beam.FlatMap(csv_line_to_tuple, AsSingleton(options))  # (customer_id, date_str, date, sales,
                                                                     #  extra_dimension?)
        )

        full_elog_merged = (
            full_elog
            | beam.Filter(lambda x: x[3] > 0)  # sales > 0
            | beam.Map(lambda x: ((x[0], x[1]), x))  # key: (customer_id, date)
            | 'Group full elog by customer and date' >> beam.GroupByKey()
            | beam.Map(merge_full_elog_by_customer_and_date)  # (customer_id, date_str, date, sales)
        )

        min_max_dates = (
            full_elog_merged
            | beam.Map(lambda x: x[2])  # date
            | beam.CombineGlobally(MinMaxDatesFn())
            | beam.Map(min_max_dates_dict)
        )

        limits_dates = (
            min_max_dates
            | beam.FlatMap(limit_dates_dict, AsSingleton(options))
        )

        cohort = (
            full_elog_merged
            | beam.FlatMap(customers_in_cohort, AsSingleton(limits_dates))
            | 'Distinct Customer IDs in Cohort' >> Distinct()
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
            | 'Distinct all Customer IDs' >> Distinct()
        )

        all_customer_ids_count = (
            all_customer_ids
            | 'Count all customers' >> beam.combiners.Count.Globally()
        )

        num_customers = (
            pipeline
            | 'Create single elem Stream I' >> beam.Create([1])
            | beam.FlatMap(count_customers,
                           AsSingleton(cohort_count),
                           AsSingleton(all_customer_ids_count))
        )

        cal_hol_elog = (
            full_elog_merged
            | beam.FlatMap(cohort_records_in_cal_hol, AsDict(cohort_set), AsSingleton(limits_dates))
        )

        cal_hol_elog_count = (
            cal_hol_elog
            | 'Count cal hol elog entries' >> beam.combiners.Count.Globally()
        )

        calibration = (
            cal_hol_elog
            | beam.FlatMap(records_in_calibration, AsSingleton(limits_dates))
        )

        num_txns_total = (
            full_elog_merged
            | beam.FlatMap(records_in_cal_hol, AsSingleton(limits_dates))
            | 'Count num txns total' >> beam.combiners.Count.Globally()
        )

        num_txns = (
            pipeline
            | 'Create single elem Stream II' >> beam.Create([1])
            | beam.FlatMap(count_txns,
                           AsSingleton(cal_hol_elog_count),
                           AsSingleton(num_txns_total))
        )

        calcbs = (
            calibration
            | beam.Map(lambda x: (x[0], x))
            | 'Group calibration elog by customer id' >> beam.GroupByKey()
            | beam.FlatMap(create_cal_cbs,
                           AsSingleton(options),
                           AsSingleton(limits_dates))  # (customer_id, number_of_transactions, average_order_value,
                                                       #  frequency, recency, total_time_observed)
        )

        first_transaction_dates_by_customer = (
            cal_hol_elog
            | beam.Map(lambda x: (x[0], x))  # customer_id
            | 'Group cal hol elog by customer id' >> beam.GroupByKey()
            | beam.Map(lambda x: (x[0], min(map(itemgetter(2), x[1]))))  # item 2 -> date
        )

        cal_hol_elog_repeat = (
            cal_hol_elog
            | beam.FlatMap(filter_first_transaction_date_records,
                           AsDict(first_transaction_dates_by_customer))
            | beam.FlatMap(calculate_time_unit_numbers,  # (customer_id, date, time_unit_number)
                           AsSingleton(options),
                           AsSingleton(limits_dates))
            | beam.Map(lambda x: (x[2], 1))  # key: time_unit_number
            | 'Group cal hol elog repeat by time unit number' >> beam.GroupByKey()
            | beam.Map(lambda x: (x[0], sum(x[1])))  # (time_unit_number, occurrences)
        )

        repeat_tx = (
            pipeline
            | 'Create single elem Stream III' >> beam.Create([1])
            | beam.FlatMap(calculate_cumulative_repeat_transactions,
                           AsIter(cal_hol_elog_repeat))  # (time_unit_number, repeat_transactions,
                                                         #  repeat_transactions_cumulative)
        )

        (
            pipeline
            | 'Create single elem Stream IV' >> beam.Create([1])
            | beam.FlatMap(calculate_model_fit_validation,
                           AsSingleton(options),
                           AsSingleton(limits_dates),
                           AsIter(calcbs),
                           AsIter(repeat_tx),
                           AsSingleton(num_customers),
                           AsSingleton(num_txns))
            | beam.Map(raise_error_if_invalid_mape)
        )

        fullcbs_without_extra_dimension = (
            full_elog_merged
            | beam.Map(lambda x: (x[0], x))  # key: customer_id
            | 'Group full merged elog by customer id' >> beam.GroupByKey()
            | beam.FlatMap(create_fullcbs,
                           AsSingleton(options),
                           AsSingleton(min_max_dates))  # (customer_id, number_of_transactions, historical_aov,
                                                        #  frequency, recency, total_time_observed)
        )

        full_elog_if_extra_dimension = (
            full_elog
            | 'Discard records if no extra dimension' >> beam.FlatMap(discard_if_no_extra_dimension,
                                                                      AsSingleton(options))
        )

        extra_dimensions_stats = (
            full_elog_if_extra_dimension
            | beam.Map(lambda x: ((x[0], x[4]), x))  # key: (customer_id, extra_dimension)
            | 'Group full elog by customer id and extra dimension' >> beam.GroupByKey()
            | beam.Map(create_extra_dimensions_stats)  # (customer_id, extra_dimension, dimension_count, tot_sales,
                                                       #  max_dimension_date)
        )

        top_dimension_per_customer = (
            extra_dimensions_stats
            | beam.Map(lambda x: (x[0], x))  # customer_id
            | 'Group extra dimension stats by customer id' >> beam.GroupByKey()
            | beam.Map(extract_top_extra_dimension)  # (customer_id, extra_dimension, dimension_count, tot_sales,
                                                     #  max_dimension_date)
        )

        customer_dimension_map = (
            top_dimension_per_customer
            | beam.Map(lambda x: (x[0], x[1]))  # (customer_id, extra_dimension)
        )

        fullcbs = (
            fullcbs_without_extra_dimension
            | beam.FlatMap(add_top_extra_dimension_to_fullcbs,
                           AsSingleton(options),
                           AsDict(customer_dimension_map))  # (customer_id, number_of_transactions, historical_aov,
                                                            #  frequency, recency, total_time_observed,
                                                            #  extra_dimension?)
        )

        prediction_by_customer_no_segments = (
            pipeline
            | 'Create single elem Stream V' >> beam.Create([1])
            | beam.FlatMap(calculate_prediction,
                           AsSingleton(options),
                           AsIter(fullcbs),
                           AsSingleton(num_customers),
                           AsSingleton(num_txns))  # [customer_id, p_alive, predicted_purchases, future_aov,
                                                   #  historical_aov, expected_value, frequency, recency,
                                                   #  total_time_observed, extra_dimension?]
        )

        num_rows = (
            full_elog_merged
            | 'Count num rows in full elog merged' >> beam.combiners.Count.Globally()
        )

        segment_prediction_threshold = 100000

        segment_predictions_exact = (
            pipeline
            | 'Create single elem Stream VII' >> beam.Create([1])
            | beam.FlatMap(lambda _, rows_count: [True] if rows_count <= segment_prediction_threshold else [False],
                           AsSingleton(num_rows))
        )

        sharded_cust_predictions_no_segments_exact, sharded_cust_predictions_no_segments_hash = (
            prediction_by_customer_no_segments
            | beam.FlatMap(prediction_sharded,
                           AsSingleton(options),
                           AsSingleton(
                               segment_predictions_exact))  # [customer_id, p_alive, predicted_purchases, future_aov,
                                                            #  historical_aov, expected_value, frequency, recency,
                                                            #  total_time_observed, extra_dimension?]
            | beam.Partition(lambda x, _: 0 if x[1] else 1, 2)
        )

        # BEGIN of "exact" branch
        prediction_by_customer_exact = (
            pipeline
            | 'Create single elem Stream VIII' >> beam.Create([1])
            | beam.FlatMap(split_in_ntiles_exact,
                           AsSingleton(options),
                           AsIter(sharded_cust_predictions_no_segments_exact))  # [customer_id, p_alive,
                                                                                #  predicted_purchases, future_aov,
                                                                                #  historical_aov, expected_value,
                                                                                #  frequency, recency,
                                                                                #  total_time_observed, segment,
                                                                                #  extra_dimension?]
        )
        # END of "exact" branch

        # BEGIN of "hash" branch
        customer_count_by_expected_value = (
            sharded_cust_predictions_no_segments_hash
            | beam.Map(lambda x: (x[0][5], 1))  # (expected_value, 1)
            | 'Group customer predictions by expected value' >> beam.GroupByKey()
            | beam.Map(lambda x: (x[0], sum(x[1])))  # expected_value, customers_count
        )

        hash_segment_limits = (
            pipeline
            | 'Create single elem Stream IX' >> beam.Create([1])
            | beam.FlatMap(expected_values_segment_limits,
                           AsSingleton(options),
                           AsIter(customer_count_by_expected_value),
                           AsSingleton(all_customer_ids_count))
        )

        prediction_by_customer_hash = (
            sharded_cust_predictions_no_segments_hash
            | beam.Map(lambda x: x[0])
            | beam.FlatMap(split_in_ntiles_hash,
                           AsSingleton(hash_segment_limits))  # [customer_id, p_alive, predicted_purchases,
                                                              #  future_aov, historical_aov, expected_value,
                                                              #  frequency, recency, total_time_observed,
                                                              #  segment, extra_dimension?]
        )
        # END of "hash" branch

        prediction_by_customer = (
            (prediction_by_customer_exact,
             prediction_by_customer_hash)  # only one of these two streams will contains values
            | beam.Flatten()
        )

        (
            prediction_by_customer
            | beam.FlatMap(lambda x, opts: [x + ['']] if not opts['extra_dimension_exists'] else [x],
                           AsSingleton(options))
            | 'prediction_by_customer to CSV line' >> beam.Map(list_to_csv_line)
            | 'Write prediction_by_customer' >>
            beam.io.WriteToText(runtime_options.output_folder,
                                header='customer_id,p_alive,predicted_purchases,future_aov,historical_aov,'
                                       'expected_value,frequency,recency,total_time_observed,segment,extra_dimension',
                                shard_name_template='',
                                num_shards=1,
                                file_name_suffix='prediction_by_customer.csv')
        )

        prediction_summary_temp = (
            prediction_by_customer
            | beam.Map(lambda x: (x[9], x))  # key: segment
            | 'Group customer predictions by segment' >> beam.GroupByKey()
            | beam.Map(generate_prediction_summary)  # (segment, average_retention_probability,
                                                     #  average_predicted_customer_value, average_predicted_order_value,
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
            | beam.FlatMap(calculate_perc_of_total_customer_value,
                           AsSingleton(tot_equity))  # (segment, average_retention_probability,
                                                     #  average_predicted_customer_value, average_predicted_order_value,
                                                     #  average_predicted_purchases, total_customer_value,
                                                     #  number_of_customers, perc_of_total_customer_value)
        )

        (
            prediction_summary
            | 'prediction_summary to CSV line' >> beam.Map(list_to_csv_line)
            | 'Write prediction_summary' >>
            beam.io.WriteToText(runtime_options.output_folder,
                                header='segment,average_retention_probability,average_predicted_customer_value,'
                                       'average_predicted_order_value,average_predicted_purchases,total_customer_value,'
                                       'number_of_customers,perc_of_total_customer_value',
                                shard_name_template='',
                                num_shards=1,
                                file_name_suffix='prediction_summary.csv')
        )

        prediction_summary_extra_dimension = (
            prediction_by_customer
            | 'Discard prediction if there is not extra dimension' >> beam.FlatMap(discard_if_no_extra_dimension,
                                                                                   AsSingleton(options))
            | beam.Map(lambda x: (x[10], x))  # extra dimension
            | 'Group customer predictions by extra dimension' >> beam.GroupByKey()
            | beam.FlatMap(generate_prediction_summary_extra_dimension,
                           AsSingleton(tot_equity))
        )

        (
            prediction_summary_extra_dimension
            | 'prediction_summary_extra_dimension to CSV line' >> beam.Map(list_to_csv_line)
            | 'Write prediction_summary_extra_dimension' >>
            beam.io.WriteToText(runtime_options.output_folder,
                                header='extra_dimension,average_retention_probability,average_predicted_customer_value,'
                                       'average_predicted_order_value,average_predicted_purchases,total_customer_value,'
                                       'number_of_customers,perc_of_total_customer_value',
                                shard_name_template='',
                                num_shards=1,
                                file_name_suffix='prediction_summary_extra_dimension.csv')
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
