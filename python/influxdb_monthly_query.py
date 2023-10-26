import influxdb_client
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

BUCKET_NAME = os.environ.get('INFLUXDB_BUCKET_NAME')
ORGANIZATION = os.environ.get('INFLUXDB_ORGANIZATION')
TOKEN = os.environ.get('INFLUXDB_TOKEN')
BASE_URL=os.environ.get('INFLUXDB_URL')
PARQUET_FILES_DIRECTORY=os.environ.get('PARQUET_FILES_DIRECTORY')


client = influxdb_client.InfluxDBClient(
    url=BASE_URL,
    token=TOKEN,
    org=ORGANIZATION
)

# Query script
query_api = client.query_api()
servers_main_query = '''
from(bucket: "server")
    |> range(start: -5m, stop: now())
    |> filter(fn: (r) => r["_measurement"] == "cpu" or r["_measurement"] == "disk" or r["_measurement"] == "diskio" or r["_measurement"] == "kernel" or r["_measurement"] == "mem" or r["_measurement"] == "net" or r["_measurement"] == "netstat" or r["_measurement"] == "processes" or r["_measurement"] == "swap" or r["_measurement"] == "system")
    |> yield(name: "mean")
'''

print('Getting data from InfluxDB!')
print('start: ', datetime.now())
result = query_api.query(org=ORGANIZATION, query=servers_main_query)

# Assuming you want to convert the result to a Pandas DataFrame
print('end: ', datetime.now())
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
current_date = datetime.now()
current_year_month = current_date.strftime('%Y%m')
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
