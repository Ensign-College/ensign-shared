"""
Initial
"""

FIRST_QUERY = '''
    from(bucket: "%(bucket)s")
        |> range(start: %(start)s, stop: %(stop)s)
        |> filter(fn: (r) => r["_measurement"] == "%(measurement)s")'''
FINAL_QUERY = '''
        |> aggregateWindow(every: %(every)s, fn: %(fn)s, createEmpty: false)
        |> yield(name: "mean")
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
