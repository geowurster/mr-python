"""
Sort stuff.  Big stuff, little stuff, any stuff!
"""


from contextlib import contextmanager
import functools
import itertools as it
import logging
import os
import tempfile

from tinymr._backport_heapq import merge as heapq_merge
from tinymr import _mrtools
from tinymr import serialize
from tinymr import tools


logger = logging.getLogger('tinysort')


@contextmanager
def delete_files(*paths):

    try:
        yield paths
    finally:
        for p in paths:
            logger.debug("delete_files() - deleting %s", p)
            try:
                os.remove(p)
            except OSError:
                pass


@contextmanager
def batch_open(*paths, mode='r', opener=open, **kwargs):

    handles = []
    try:

        for p in paths:
            handles.append(opener(p, mode, **kwargs))
        yield tuple(handles)

    finally:
        for h in handles:
            h.close()


def mem_sort(stream, chunksize, jobs=1, **kwargs):

    slc = tools.slicer(stream, chunksize)
    first = next(slc)

    if len(first) < chunksize:
        logger.debug(
            "mem_sort() - only found %s objects and %s chunksize - sorting "
            "without parallelism overhead", len(first), chunksize)
        return iter(_mrtools.sorter(first, **kwargs))

    else:

        logger.debug(
            "mem_sort() - sorting with %s jobs and chunksize %s", jobs, chunksize)

        srt = functools.partial(_mrtools.sorter, **kwargs)
        with tools.runner(srt, it.chain([first], slc), jobs) as results:
            for obj in heapq_merge(*results, key=kwargs.get('key')):
                yield obj


def sort_into(values, f, **kwargs):

    """
    Sort `values` into an open file-like object.

    Parameters
    ----------
    values : iter
        Data to sort.
    f : file
        Open file-like object supporting `f.write()`.
    kwargs : **kwargs, optional
        Keyword arguments for `sorted()`.
    """

    for item in _mrtools.sorter(values, **kwargs):
        f.write(item)


def _mp_sort_into_files(kwargs):
    values = kwargs['values']
    serializer = kwargs['serializer']
    kwargs = kwargs['kwargs']

    _, path = tempfile.mkstemp()

    with serializer.open(path, 'w') as dst:
        sort_into(values, dst, **kwargs)

    return path


def sort_into_files(
        stream, chunksize, jobs=1, serializer=serialize.Pickle(), **kwargs):

    """
    Sort a stream of data into one or more tempfiles on disk.  Data is chunked
    into pieces and optionally processed in parallel.

    Parameters
    ----------
    stream : iter
        Data to sort.
    chunksize : int
        Maximum amount of data to sort into each file.
    jobs : int, optional
        Process data across N cores.  Each job gets `chunksize` amount of data.
    kwargs : **kwargs, optional
        Keyword arguments for `sort_into()`.

    Returns
    -------
    tuple
        Paths to tempfiles on disk.
    """

    logger.debug(
        "sort_into_files() - sorting stream with chunksize=%s, jobs=%s, and "
        "serializer=%s", chunksize, jobs, serializer)

    tasks = ({
        'values': values,
        'serializer': serializer,
        'kwargs': kwargs
    } for values in tools.slicer(stream, chunksize))

    with tools.runner(_mp_sort_into_files, tasks, jobs) as run:
        return tuple(run)


def _mp_sort_files(kwargs):
    infile = kwargs['infile']
    deserializer = kwargs['deserializer']
    chunksize = kwargs['chunksize']
    kwargs = kwargs['kwargs']

    with deserializer.open(infile) as src:
        return sort_into_files(src, chunksize=chunksize, jobs=1, **kwargs)


def sort_files_into_files(
        *paths,
        chunksize=100000,
        jobs=1,
        deserializer=serialize.Pickle(),
        **kwargs):

    logger.debug(
        "sort_files_into_files() - sorting %s files with chunksize=%s, "
        "jobs=%s, and deserializer=%s",
        len(paths), chunksize, jobs, deserializer)

    tasks = ({
        'infile': fp,
        'kwargs': kwargs,
        'chunksize': chunksize,
        'deserializer': deserializer
    } for fp in paths)

    with tools.runner(_mp_sort_files, tasks, jobs) as run:
        return tuple(it.chain(*run))


def sort_files(*paths, **kwargs):

    logger.debug(
        "sort_files() - sorting %s files with %s jobs and total line count %s",
        len(paths), kwargs.get('jobs', 1),
        sum((tools.count_lines(p) for p in paths)))

    serializer = kwargs.get('serializer', serialize.Pickle())
    key = kwargs.get('key', None)

    with delete_files(*sort_files_into_files(*paths, **kwargs)) as paths:

        logger.debug(
            "sort_files() - merging results from %s files with serializer=%s",
            len(paths), serializer)

        with batch_open(*paths, opener=serializer.open) as handles:

            for obj in heapq_merge(*handles, key=key):
                yield obj
