import argparse
import syslog
from datetime import datetime, timedelta

from constants import FILTERS, MEASUREMENTS
from dotenv import load_dotenv
from utils import (build_aggregated_query_string, build_csv_file,
                   build_parquet_file, build_specific_query_string,
                   parse_end_date, parse_start_date)

load_dotenv()  # take environment variables from .env.

# Create the parser
parser = argparse.ArgumentParser()

today = datetime.now()
yesterday = today - timedelta(days=1)

# Add an argument
parser.add_argument(
    '-t', '--type', type=str, required=True, choices=['normal', 'aggregated']
)
parser.add_argument(
    '-s', '--start', type=parse_start_date,
    default=yesterday.strftime("%Y-%m-%dT")
)
parser.add_argument(
    '-e', '--end', type=parse_end_date,
    default=yesterday.strftime("%Y-%m-%dT")
)
parser.add_argument(
    '-m', '--measurement', type=str, choices=MEASUREMENTS
)
parser.add_argument('-b', '--bucket', type=str, default='server')
parser.add_argument('-ev', '--every', type=str, default='1h')
parser.add_argument('-fn', '--function', type=str, default='mean')
parser.add_argument('-sf', '--specific-filter', type=str, default='default')
parser.add_argument(
    '-o', '--output', type=str, required=True, default='parquet',
    choices=['parquet', 'csv']
)
# Parse the argument
args = parser.parse_args()

if args.type == 'normal' and not args.measurement:
    raise argparse.ArgumentTypeError(
        'With the normal query execution it is necessary to set a '
        'measurement filter'
    )

if args.measurement and not FILTERS[args.measurement].get(args.specific_filter):
    raise argparse.ArgumentTypeError(
        f'The specific filter doesn\'t not exist for measurement '
        f'{args.measurment}'
    )

# Query script
if args.type == 'normal':
    query_string = build_specific_query_string(
        start=args.start, stop=args.end, measurement=args.measurement,
        specific_filter=args.specific_filter, bucket=args.bucket,
        every=args.every, fn=args.function
    )
else:
    query_string = build_aggregated_query_string(
        start=args.start, stop=args.end, bucket=args.bucket,
        every=args.every, fn=args.function
    )

log_message = 'Getting data with the following parameters:'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)
log_message = f'Bucket: {args.bucket}'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)
log_message = f'Measurement: {args.measurement}'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)
log_message = f'From: {args.start} - To: {args.end}'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)
log_message = f'Interval: every {args.every}'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)
log_message = f'Aggregating function: {args.function}'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)
log_message = f'Output type: {args.output}'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)
# log_message = query_string
# syslog.syslog(syslog.LOG_INFO, log_message)
# print(log_message)

log_message = f'Querying data from InfluxDB for date: {yesterday.strftime("%Y-%m-%d")}!'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)

# call function
if args.output == 'parquet':
    build_parquet_file(query_string)
else:
    build_csv_file(query_string)

log_message = 'Process finished!'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)
