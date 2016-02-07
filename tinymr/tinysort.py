"""
Sort stuff.  Big stuff, little stuff, any stuff!
"""


from contextlib import contextmanager
import itertools as it
import logging
import os
import tempfile

from tinymr._backport_heapq import merge as heapq_merge
from tinymr import _mrtools
from tinymr import serialize
from tinymr import tools


logger = logging.getLogger('tinysort')


CHUNKSIZE = 100000


@contextmanager
def delete_files(*paths):

    """
    Register file paths that are deleted on context exit.
    """

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

    """
    Like `open()` but operates on multiple file paths.

    Parameters
    ----------
    paths : *str
        File paths to open.
    mode : str, optional
        Like `open(mode='r')`.
    opener : function, optional
        The function responsible for actually opening the file paths.
    kwargs : **kwargs, optional
        Additional keyword arguments for `opener`.

    Returns
    -------
    tuple
        File handles.
    """

    handles = []
    try:

        for p in paths:
            handles.append(opener(p, mode, **kwargs))
        yield tuple(handles)

    finally:
        for h in handles:
            h.close()


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

    """
    `multiprocessing` function for `sort_into_files()`.
    """

    values = kwargs['values']
    serializer = kwargs['serializer']
    kwargs = kwargs['kwargs']

    _, path = tempfile.mkstemp()

    with serializer.open(path, 'w') as dst:
        sort_into(values, dst, **kwargs)

    return path


def sort_into_files(
        stream,
        chunksize=CHUNKSIZE,
        jobs=1,
        serializer=serialize.Pickle(),
        **kwargs):

    """
    Sort a stream of data into a bunch of tiny files on disk. 

    The input `stream` is broken up into pieces containing no more than
    `chunksize` objects.  Each piece is passed to a `multiprocessing` operation
    that sorts it, flushes to a tempfile, and returns the tempfile path.

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
    
    """
    `multiprocessing` function for use with `sort_files()`
    """
    
    infile = kwargs['infile']
    reader = kwargs['reader']
    chunksize = kwargs['chunksize']
    kwargs = kwargs['kwargs']

    with reader.open(infile) as src:
        return sort_into_files(src, chunksize=chunksize, jobs=1, **kwargs)


def sort_files_into_files(
        *paths,
        chunksize=CHUNKSIZE,
        jobs=1,
        reader=serialize.Pickle(),
        **kwargs):

    """
    Sort big files on disk into a bunch of tiny files on disk.
    
    Each input file is split into chunks with at most `chunksize` objects, which
    are sorted and the serialized to disk.

    Parameters
    ----------
    paths : *str
        Input file paths.
    chunksize : int, optional
        Maximum number of objects to sort in memory in ech job.  The resulting
        output tempfiles will contain at most `chunksize` objects.
    jobs : int, optional
        Process files in parallel across N cores.
    reader : tinymr.base.BaseSerializer, optional
        Serializer for reading data from disk.
    kwargs : **kwargs
        Keyword arguments for `sort_into_files()`.

    Returns
    -------
    tuple
        Tempfile paths.
    """

    logger.debug(
        "sort_files_into_files() - sorting %s files with chunksize=%s, "
        "jobs=%s, and reader=%s",
        len(paths), chunksize, jobs, reader)

    tasks = ({
        'infile': fp,
        'kwargs': kwargs,
        'chunksize': chunksize,
        'reader': reader
    } for fp in paths)

    with tools.runner(_mp_sort_files, tasks, jobs) as run:
        return tuple(it.chain(*run))


def sort_files(*paths, **kwargs):

    """
    Sorts files on disk into an output stream.
    
    Wrapper for `sort_files_into_files()` that merges with `heapq.merge()`.
    """

    logger.debug(
        "sort_files() - sorting %s files with %s jobs",
        len(paths), kwargs.get('jobs', 1))

    serializer = kwargs.get('serializer', serialize.Pickle())
    key = kwargs.get('key', None)

    with delete_files(*sort_files_into_files(*paths, **kwargs)) as paths:

        logger.debug(
            "sort_files() - merging results from %s files with serializer=%s",
            len(paths), serializer)

        with batch_open(*paths, opener=serializer.open) as handles:

            for obj in heapq_merge(*handles, key=key):
                yield obj
