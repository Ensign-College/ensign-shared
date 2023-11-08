"""
Functions
"""

from constants import FIRST_QUERY, FINAL_QUERY, FILTERS


def build_query_string(
    start, stop, measurement, specific_filter, bucket='server', every='5m',
    fn='mean',
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

    return query, {**first_arguments, **second_arguments}
