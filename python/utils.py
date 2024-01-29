"""
Functions
"""

import csv
import os
import syslog
from datetime import datetime, timedelta

import influxdb_client
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from constants import (AGGREGATED_QUERY, BUILD_ROWS, COLUMNS, CSV_TITLES,
                       FILTERS, FINAL_QUERY, FIRST_QUERY, INITIAL, LINE1,
                       LINE2, LINE3, LINE4, LINE5, LINE6)
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

BUCKET_NAME = os.environ.get('INFLUXDB_BUCKET_NAME')
ORGANIZATION = os.environ.get('INFLUXDB_ORGANIZATION')
TOKEN = os.environ.get('INFLUXDB_TOKEN')
BASE_URL=os.environ.get('INFLUXDB_URL')
FILES_DIRECTORY=os.environ.get('FILES_DIRECTORY', './')

client = influxdb_client.InfluxDBClient(
    url=BASE_URL,
    token=TOKEN,
    org=ORGANIZATION
)

today = datetime.now()
yesterday = today - timedelta(days=1)


def parse_date(datetime_str):
    # with time
    try:
        # Attempt to parse the input string as a date
        parsed_date = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        return True, parsed_date
    except ValueError:
        pass

    # without time
    try:
        parsed_date = datetime.strptime(
            f'{datetime_str} 00:00:00', '%Y-%m-%d %H:%M:%S'
        )
        return False, parsed_date
    # default value
    except ValueError:
        return False, yesterday


def parse_start_date(str_datetime):
    with_time, parsed_date = parse_date(str_datetime)
    if with_time:
        return parsed_date.strftime("%Y-%m-%dT %H:%M:%SZ")
    return f'{parsed_date.strftime("%Y-%m-%dT")}00:00:00Z'


def parse_end_date(str_datetime):
    with_time, parsed_date = parse_date(str_datetime)
    if with_time:
        return parsed_date.strftime("%Y-%m-%dT %H:%M:%SZ")
    return f'{parsed_date.strftime("%Y-%m-%dT")}23:59:59Z'


def get_template_type(csv_line):
    if csv_line == LINE1:
        return 1
    if csv_line == LINE2:
        return 2
    if csv_line == LINE3:
        return 3
    if csv_line == LINE4:
        return 4
    if csv_line == LINE5:
        return 5
    if csv_line == LINE6:
        return 6
    return 0


def get_formated_line(template_type, csv_line):
    return BUILD_ROWS[template_type](csv_line)


def build_specific_query_string(
    start, stop, measurement, specific_filter, bucket, every, fn
):
    """
    Build the main query
    """
    first_arguments = {
        'bucket': bucket,
        'start': start,
        'stop': stop,
        'measurement': measurement,
        'filters': FILTERS[measurement].get(specific_filter, 'default'),
    }
    second_arguments = {
        'every': every,
        'fn': fn,
    }
    first = FIRST_QUERY % first_arguments
    second = FINAL_QUERY % second_arguments

    if measurement == 'cpu':
        additional = f'''
        |> filter(fn: (r) => r["{measurement}"] == "cpu-total")'''
        query = first + additional + second
    else:
        query = first + second

    return query


def build_aggregated_query_string(start, stop, bucket, every, fn):
    arguments = {
        'bucket': bucket,
        'start': start,
        'stop': stop,
        'every': every,
        'fn': fn,
    }

    query = AGGREGATED_QUERY % arguments

    return query

def build_parquet_file(query_string, filename):
    start_process = datetime.now()
    log_message = f'Start: {start_process.strftime("%Y-%m-%d %H:%M:%S")}'
    syslog.syslog(syslog.LOG_INFO, log_message)
    print(log_message)

    query_api = client.query_api()
    result = query_api.query(
        org=ORGANIZATION, query=query_string
    )

    # Assuming you want to convert the result to a Pandas DataFrame
    end_process = datetime.now()
    log_message = f'End: {end_process.strftime("%Y-%m-%d %H:%M:%S")}'
    syslog.syslog(syslog.LOG_INFO, log_message)
    print(log_message)

    process_time = end_process - start_process
    log_message = f'The query took: {process_time}'
    syslog.syslog(syslog.LOG_INFO, log_message)
    print(log_message)

    data = []
    for table in result:
        for record in table.records:
            if not record.values.get('error'):
                data.append([
                    record.values.get('table'),
                    record.values.get('result'),
                    record.values.get('_measurement'),
                    record.values.get('_field'),
                    str(record.values.get('_value')),
                    record.values.get('_start'),
                    record.values.get('_stop'),
                    record.values.get('_time'),
                    record.values.get('host', ''),
                    record.values.get('cpu', ''),
                    record.values.get('device', ''),
                    record.values.get('fstype', ''),
                    record.values.get('node', ''),
                    record.values.get('path', ''),
                    record.values.get('interface', ''),
                ])


    # Create a Pandas DataFrame
    log_message = 'Creating data frame'
    syslog.syslog(syslog.LOG_INFO, log_message)
    print(log_message)

    # df = pd.DataFrame(data, columns=COLUMNS, dtype=COLUMN_TYPES)
    df = pd.DataFrame(data, columns=COLUMNS)

    # Convert the Pandas DataFrame to an Arrow Table
    new_table = pa.Table.from_pandas(df)

    # Specify the file path
    # current_year_month = yesterday.strftime('%Y%m')
    # filename = f'{current_year_month}_servers.parquet'

    # Write the Arrow Table to a Parquet file
    log_message = 'Creating/Appending parquet file!'
    syslog.syslog(syslog.LOG_INFO, log_message)
    print(log_message)

    full_path = f'{FILES_DIRECTORY}/{filename}'
    if os.path.exists(full_path):
        existing_table = pq.read_table(full_path)
        combined_table = pa.concat_tables([existing_table, new_table])
    else:
        combined_table = new_table

    with pq.ParquetWriter(full_path, combined_table.schema) as writer:
        writer.write_table(combined_table)

    client.close()


def build_csv_file(query_string, filename):
    start_process = datetime.now()
    log_message = f'Start: {start_process.strftime("%Y-%m-%d %H:%M:%S")}'
    syslog.syslog(syslog.LOG_INFO, log_message)
    print(log_message)

    query_api = client.query_api()
    result = query_api.query_csv(
        org=ORGANIZATION, query=query_string
    )

    # Assuming you want to convert the result to a Pandas DataFrame
    end_process = datetime.now()
    log_message = f'End: {end_process.strftime("%Y-%m-%d %H:%M:%S")}'
    syslog.syslog(syslog.LOG_INFO, log_message)
    print(log_message)

    process_time = end_process - start_process
    log_message = f'The query took: {process_time}'
    syslog.syslog(syslog.LOG_INFO, log_message)
    print(log_message)

    # Specify the file path
    # current_year_month = yesterday.strftime('%Y%m')
    # filename = f'{current_year_month}_servers.csv'

    # Write the Arrow Table to a Parquet file
    log_message = 'Creating/Appending csv file!'
    syslog.syslog(syslog.LOG_INFO, log_message)
    print(log_message)

    line_template = 1
    formated_rows = []
    for csv_line in result:
        if any(
            keyword in csv_line
            for keyword in ['#datatype', '#group', '#result', '#default']
        ):
            # skip line
            pass
        elif all(item in csv_line for item in INITIAL):
            line_template = get_template_type(csv_line=csv_line)
        else:

            formated_row = get_formated_line(
                template_type=line_template, csv_line=csv_line
            )
            formated_rows.append(formated_row)

    full_path = f'{FILES_DIRECTORY}/{filename}'
    mode = 'a' if os.path.exists(full_path) else 'w'

    with open(full_path, mode=mode, newline='') as file:
        writer = csv.writer(file)
        writer.writerow(CSV_TITLES)
        for row in formated_rows:
            writer.writerow(row)

    client.close()
