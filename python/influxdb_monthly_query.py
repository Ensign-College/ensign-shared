import influxdb_client
import os
import syslog
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from constants import COLUMNS, MEASUREMENTS
from utils import build_query_string
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

BUCKET_NAME = os.environ.get('INFLUXDB_BUCKET_NAME')
ORGANIZATION = os.environ.get('INFLUXDB_ORGANIZATION')
TOKEN = os.environ.get('INFLUXDB_TOKEN')
BASE_URL=os.environ.get('INFLUXDB_URL')
PARQUET_FILES_DIRECTORY=os.environ.get('PARQUET_FILES_DIRECTORY')
START_DATETIME=os.environ.get('START_DATETIME')
END_DATETIME=os.environ.get('END_DATETIME')


measurement = sys.argv[1]
if measurement not in MEASUREMENTS:
    print(f'Exiting program. "{measurement}" is not a valid measurement')
    sys.exit()

try:
    specific_filter = sys.argv[2]
except IndexError:
    specific_filter = 'default'

today = datetime.now()
yesterday = today - timedelta(days=1)

if not START_DATETIME or not END_DATETIME:
    START_DATETIME = f'{yesterday.strftime("%Y-%m-%dT")}00:00:00Z'
    END_DATETIME= f'{yesterday.strftime("%Y-%m-%dT")}23:59:00Z'
    # END_DATETIME= f'{yesterday.strftime("%Y-%m-%dT")}00:10:00Z'

client = influxdb_client.InfluxDBClient(
    url=BASE_URL,
    token=TOKEN,
    org=ORGANIZATION
)

# Query script
query_api = client.query_api()
query_string, used_parameters = build_query_string(
    start=START_DATETIME, stop=END_DATETIME, measurement=measurement,
    specific_filter=specific_filter
)

log_message = 'Getting data with the following parameters:'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)
log_message = f'Bucket: {used_parameters["bucket"]}'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)
log_message = f'Measurement: {used_parameters["measurement"]}'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)
log_message = f'From: {used_parameters["start"]} - To: {used_parameters["stop"]}'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)
log_message = f'Interval: every {used_parameters["every"]}'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)
log_message = f'Aggregating function: {used_parameters["fn"]}'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)
# log_message = query_string
# syslog.syslog(syslog.LOG_INFO, log_message)
# print(log_message)

log_message = f'Querying data from InfluxDB for date: {yesterday.strftime("%Y-%m-%d")}!'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)

start_process = datetime.now()
log_message = f'Start: {start_process.strftime("%Y-%m-%d %H:%M:%S")}'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)

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
current_year_month = yesterday.strftime('%Y%m')
parquet_file = f'{current_year_month}_servers.parquet'

# Write the Arrow Table to a Parquet file
log_message = 'Creating/Appending parquet file!'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)

full_path = f'{PARQUET_FILES_DIRECTORY}/{parquet_file}'
if os.path.exists(full_path):
    existing_table = pq.read_table(full_path)
    combined_table = pa.concat_tables([existing_table, new_table])
else:
    combined_table = new_table

with pq.ParquetWriter(full_path, combined_table.schema) as writer:
    writer.write_table(combined_table)

log_message = 'Process finished!'
syslog.syslog(syslog.LOG_INFO, log_message)
print(log_message)
