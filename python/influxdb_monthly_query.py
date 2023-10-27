import influxdb_client
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

BUCKET_NAME = os.environ.get('INFLUXDB_BUCKET_NAME')
ORGANIZATION = os.environ.get('INFLUXDB_ORGANIZATION')
TOKEN = os.environ.get('INFLUXDB_TOKEN')
BASE_URL=os.environ.get('INFLUXDB_URL')
PARQUET_FILES_DIRECTORY=os.environ.get('PARQUET_FILES_DIRECTORY')
START_DATETIME=os.environ.get('START_DATETIME')
END_DATETIME=os.environ.get('END_DATETIME')

today = datetime.now()
yesterday = today - timedelta(days=1)

if not START_DATETIME or not END_DATETIME:
    START_DATETIME = f'{yesterday.strftime("%Y-%m-%dT")}00:00:00Z'
    END_DATETIME= f'{yesterday.strftime("%Y-%m-%dT")}23:59:00Z'


client = influxdb_client.InfluxDBClient(
    url=BASE_URL,
    token=TOKEN,
    org=ORGANIZATION
)

# Query script
query_api = client.query_api()
servers_main_query = f'''
from(bucket: "server")
    |> range(start: {START_DATETIME}, stop: {END_DATETIME})
    |> filter(fn: (r) => r["_measurement"] == "cpu" or r["_measurement"] == "disk" or r["_measurement"] == "diskio" or r["_measurement"] == "kernel" or r["_measurement"] == "mem" or r["_measurement"] == "net" or r["_measurement"] == "netstat" or r["_measurement"] == "processes" or r["_measurement"] == "swap" or r["_measurement"] == "system")
    |> yield(name: "mean")
'''

print(f'Getting data from InfluxDB for date: {yesterday.strftime("%Y-%m-%d")}!')
start_process = datetime.now()
print('start: ', start_process)

result = query_api.query(org=ORGANIZATION, query=servers_main_query)

# Assuming you want to convert the result to a Pandas DataFrame
end_process = datetime.now()
print('end: ', end_process)

process_time = end_process - start_process
print(f'The query took: {process_time}')

data = []
for index, table in enumerate(result):
    if index == 0:
        columns = table.columns
    for record in table.records:
        data.append(record.values)

# Create a Pandas DataFrame
print('Creating data frame')
df = pd.DataFrame(data, columns=columns)

# Convert the Pandas DataFrame to an Arrow Table
new_table = pa.Table.from_pandas(df)

# Specify the file path
current_year_month = yesterday.strftime('%Y%m')
parquet_file = f'{current_year_month}_servers.parquet'

# Write the Arrow Table to a Parquet file
print('Creating/Appending parquet file!')
full_path = f'{PARQUET_FILES_DIRECTORY}/{parquet_file}'
if os.path.exists(full_path):
    existing_table = pq.read_table(full_path)
    combined_table = pa.concat_tables([existing_table, new_table])
else:
    combined_table = new_table
    # pq.write_table(new_table, full_path)

with pq.ParquetWriter(full_path, combined_table.schema) as writer:
    writer.write_table(combined_table)

print('Process finished!')
