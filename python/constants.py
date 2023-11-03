"""
Initial
"""

MEASUREMENTS = [
    'cpu', 'disk', 'diskio', 'kernel', 'mem', 'net', 'netstat', 'processes',
    'swap', 'system'
]
COLUMNS = [
    'table', 'result', 'measurement', 'field', 'value', 'start', 'stop',
    'time', 'host', 'cpu', 'device', 'fstype', 'node', 'path', 'interface'
]
COLUMN_TYPES={
    'table': "int",
    'result': "string",
    'measurement': "string",
    'field': "string",
    'value': "string",
    'start': "datetime",
    'stop': "datetime",
    'time': "datetime",
    'host': "string",
    'cpu': "string",
    'device': "string",
    'fstype': "string",
    'node': "string",
    'path': "string",
    'interface': "string",
}


FIRST_QUERY = '''
    from(bucket: "%(bucket)s")
        |> range(start: %(start)s, stop: %(stop)s)
        |> filter(fn: (r) => r["_measurement"] == "%(measurement)s")'''
FINAL_QUERY = '''
        |> aggregateWindow(every: 10s, fn: last, createEmpty: false)
'''

def build_query_string(
    start, stop, measurement, bucket='server', every='10s', fn='mean'
):
    """
    Build the main query
    """
    first = FIRST_QUERY % {
        'bucket': bucket,
        'start': start,
        'stop': stop,
        'measurement': measurement,
    }
    second = FINAL_QUERY % {
        'every': every,
        'fn': fn,
    }

    if measurement == 'cpu':
        additional = f'''
        |> filter(fn: (r) => r["{measurement}"] == "cpu-total")'''
        query = first + additional + second
    else:
        query = first + second

    return query
