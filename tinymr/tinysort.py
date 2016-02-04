"""
Sort stuff.  Big stuff, little stuff, any stuff!
"""


from __future__ import absolute_import

import functools
import itertools as it
import logging
import os
import six
import tempfile

from tinymr import _mrtools
from tinymr import serialize
from tinymr import tools


logger = logging.getLogger('tinysort')


def merge_files(*paths, **kwargs):

    """
    Merge sorted files into a single stream.  Files must contain data that can
    be de-serialized with the same format.

    Parameters
    ----------
    paths : *args
        Sorted files to merge.
    deserializer : tinymr.base.BaseSerializer, optional
        Type of data contained within the files.
    """

    deserializer = kwargs.pop('deserializer', serialize.Pickle())
    atomic = kwargs.pop('atomic', True)

    handles = {}
    try:
        for p in paths:
            handles[p] = open(p, mode=deserializer._read_mode)

        readers = (deserializer._loader(f) for f in handles.values())
        for item in tools.heapq_merge(*readers, **kwargs):
            yield item

    finally:
        for fp, f in six.iteritems(handles):
            f.close()
            if atomic:
                os.remove(fp)


def _sort_and_dump(kwargs):

    """
    Sort some data, dump it into a file, and return the file path.

    This function takes a single argument so it can be used with `map()`.

    Parameters
    ----------
    kwargs : dict

        data : iter
            Data to process.

        max_memory : int
            Maximum amount of memory for `tempfile.SpooledTemporaryFile()`.

        mode : str
            I/O mode for tempfile.

        sort_args : dict
            **kwargs for `_mrtools.sorter()`.
    """

    data = kwargs.pop('data')
    serializer = kwargs.pop('serializer')
    sort_args = kwargs.pop('sort_args')
    _, path = tempfile.mkstemp()

    logger.debug("Sorting %s items into %s", len(data), path)

    data = _mrtools.sorter(data, **sort_args)

    return serializer._writefile(data, path, mode=serializer._write_mode)


def sort_stream(
        stream,
        serializer=serialize.Pickle(),
        chunksize=100000,
        jobs=1,
        yield_paths=False,
        allow_memory_sort=True,
        **kwargs):

    """
    Sort a stream of data.

    Parameters
    ----------
    stream : iter
        Data to sort.
    serializer : tinymr.base.BaseSerializer, optional
        Serialization class to use for reading/writing tempfiles.
    chunksize : int, optional
        Dump N items into each tempfile.
    """

    # TODO: This is a somewhat naive implementation.  We could incorporate
    # something like tempfile.SpooledTemporaryFile() or be smarter about when
    # data is flushed to disk.  Would require more pickling to get data in
    # and out of multiprocessing jobs, but would limit disk I/O.  If we don't
    # exceed available memory don't flush the data to disk, just return a
    # a StringIO().

    logger.debug(
        "Sorting a stream of data with %s jobs, chunksize %s, and "
        "serializer %s", jobs, chunksize, serializer)

    chunked = tools.slicer(stream, chunksize)
    initial = next(chunked)

    if allow_memory_sort and len(initial) < chunksize:

        logger.debug(
            "Got %s items and chunksize %s- sorting in memory",
            len(initial), chunksize)

        for item in _mrtools.sorter(initial, **kwargs):
            yield item

    else:

        logger.debug("Can't sort in-memory")

        tasks = ({
            'data': data,
            'serializer': serializer,
            'sort_args': kwargs
        } for data in it.chain([initial], chunked))

        with tools.runner(_sort_and_dump, tasks, jobs) as run:
            paths = tuple(run)

        logger.debug("Sorted data into %s files", len(paths))

        if yield_paths:
            for p in paths:
                yield p

        else:
            for item in merge_files(*paths, **kwargs):
                yield item


def sort_file(path, deserializer=serialize.Pickle(), **kwargs):

    """
    Sort a file with `sort_stream()`.  Mostly used by `sort_files()`.

    Parameters
    ----------
    path : str
        Path to the file on disk.
    kwargs : **kwargs, optional
        Arguments for `sort_stream()`.

    Yield
    -----
    object
    """

    logger.debug("sort_file() with deserializer %s: %s", deserializer, path)

    reader = deserializer._readfile(path)
    for item in sort_stream(reader, **kwargs):
        yield item


def _file_sorter(*args, **kwargs):

    return tuple(sort_file(*args, **kwargs))


def sort_files(*paths, **kwargs):

    jobs = kwargs.pop('jobs', 1)
    kwargs.update(yield_paths=True, allow_memory_sort=False)
    key = kwargs.get('key', lambda x: x)

    file_sorter = functools.partial(_file_sorter, **kwargs)

    logger.debug(
        "sort_files() - sorting %s files with %s jobs", len(paths), jobs)

    with tools.runner(file_sorter, paths, jobs) as run:
        paths = tuple(it.chain(*run))

    logger.debug(
        "sort_files() - done sorting.  Merging %s files ...", len(paths))

    for item in merge_files(*paths, key=key, atomic=True):
        yield item
