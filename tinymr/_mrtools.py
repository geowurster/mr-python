"""
Helpers for MapReduce implementations.
"""


from collections import namedtuple

import six

from tinymr.tools import sorter


def strip_sort_key(kv_stream):

    """
    Given a stream of `(key, [(sort, data), (sort, data)])` with sort key
    intact, remove the key from the values.

    Example:

        [
            ('key1', [(10, data1), (3, data25)]),
            ('key2', [(200, data100), (250, data67))
        ]

    Produces:

        [
            ('key1', [data1, data25]),
            ('key2', [data100, data67)
        ]

    Parameters
    ----------
    kv_stream : dict or iter
        Dictionary like `{key: [(sort, data)]}` or a stream of tuples like
        `(key, [(sort, data])`.

    Yields
    ------
    tuple
        `(key, [data, data, ...])`
    """

    kv_stream = six.iteritems(kv_stream) if isinstance(kv_stream, dict) else kv_stream
    return ((k, tuple(i[-1] for i in v)) for k, v in kv_stream)


def sort_partitioned_values(kv_stream):

    """
    Given a stream of `(key, [(sort, data), (sort, data)])` sort the values
    for every key.

    Example:

        [
            ('key1', [(10, data), (3, data)]),
            ('key2', [(200, data), (250, data))
        ]

    Produces:

        [
            ('key1', [(3, data), (10, data)]),
            ('key2', [(200, data), (250, data))
        ]

    Parameters
    ----------
    kv_stream : dict or iter
        Dictionary like `{key: [(sort, data]}` or a stream of tuples like
        `(key, [(sort, data])`.

    Yields
    ------
    tuple
        `(key, [(sort, data), (sort, data), ...])`
    """

    kv_stream = six.iteritems(kv_stream) if isinstance(kv_stream, dict) else kv_stream
    return ((k, sorter(v, key=lambda x: x[0])) for k, v in kv_stream)


class ReduceJobConf(
        namedtuple('ReduceJob', ['reducer', 'sort', 'jobs', 'chunksize'])):

    """
    Describes a reduce job.  Makes keeping track of multiple reducers easier.

    Parameters
    ----------
    reducer : callable
        Does the reducing.  Has a signature like `reducer(key, iter(values))`.
    sort : bool
        Determines if the partitioned values should be sorted.
    jobs : int
        Number of jobs to run in parallel.
    chunksize : int
        Amount of data to pass to one `job`.
    """
