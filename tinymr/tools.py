"""
Tools for building MapReduce implementations.
"""


from __future__ import absolute_import


from collections import defaultdict
import itertools as it
import io
import multiprocessing as mp
import operator
import os

import six
from six.moves import zip

from tinymr._backport_heapq import merge as heapq_merge
from tinymr import errors


# Make instance methods pickle-able in Python 2
# Instance methods are not available as a type, so we have to create a tiny
# class so we can grab an instance method
# We then register our improved _reduce_method() with copy_reg so pickle knows
# what to do.
if six.PY2:  # pragma: no cover
    import copy_reg

    class _I:
        def m(self):
            pass

    def _reduce_method(m):
        if m.im_self is None:
            return getattr, (m.im_class, m.im_func.func_name)
        else:
            return getattr, (m.im_self, m.im_func.func_name)

    copy_reg.pickle(type(_I().m), _reduce_method)


def slicer(iterable, chunksize):

    """
    Read an iterator in chunks.

    Example:

        >>> for p in slicer(range(5), 2):
        ...     print(p)
        (0, 1)
        (2, 3)
        (4,)

    Parameters
    ----------
    iterable : iter
        Input stream.
    chunksize : int
        Number of records to include in each chunk.  The last chunk will be
        incomplete unless the number of items in the stream is evenly
        divisible by `size`.

    Yields
    ------
    tuple
    """

    iterable = iter(iterable)
    while True:
        v = tuple(it.islice(iterable, chunksize))
        if v:
            yield v
        else:
            raise StopIteration


class runner(object):

    """
    The `multiprocessing` module can be difficult to debug and introduces some
    overhead that isn't needed when only running one job.  Use a generator in
    this case instead.

    Wrapped in a class to make the context syntax optional.
    """

    def __init__(self, func, iterable, jobs):

        """
        Parameters
        ----------
        func : callable
            Callable object to map across `iterable`.
        iterable : iter
            Data to process.
        jobs : int
            Number of `multiprocessing` jobs.
        """

        self._func = func
        self._iterable = iterable
        self._jobs = jobs
        self._closed = False

        if jobs < 1:
            raise ValueError("jobs must be >= 1, not: {}".format(jobs))
        elif jobs == 1:
            self._pool = None
            self._proc = (func(i) for i in iterable)
        else:
            self._pool = mp.Pool(jobs)
            self._proc = self._pool.imap_unordered(func, iterable)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __repr__(self):
        return "{cname}(func={func}, iterable={iterable}, jobs={jobs})".format(
            cname=self.__class__.__name__,
            func=repr(self._func),
            iterable=repr(self._iterable),
            jobs=self._jobs)

    def __iter__(self):
        return self._proc

    def __next__(self):
        return next(self._proc)

    next = __next__

    def close(self):

        """
        Close the `multiprocessing` pool if we're using it.
        """

        if self._pool is not None:
            self._pool.close()
        self._closed = True


def mapkey(key, values):

    """
    Given a key and a series of values, create a series of `(key, value)`
    tuples.

    Example:

        >>> for pair in mapkey('key', range(5)):
        ...     print(pair)
        ('key', 0)
        ('key', 1)
        ('key', 2)
        ('key', 3)
        ('key', 4)

    Parameters
    ----------
    key : object
        Object to use as the first element of each output tuples.

    Returns
    -------
    iter
    """

    return zip(it.repeat(key), values)


def partition(key_values):

    """
    Given a stream of `(key, value)` tuples, group them by key into a dict.
    Equivalent to the code below, but faster:

        >>> from itertools import groupby
        >>> {k: list(v) for k, v in groupby(key_values, key=lambda x: x[0])}

    Example:

        >>> data = [('key1', 1), ('key1', 2), ('key2', None)]
        >>> partition(data)
        {
            'key1': [('key1', 1), ('key1', 2)],
            'key2': [('key2', None)]
        }

    Parameters
    ----------
    key_values : iter
        Tuples - typically `(key, value)`, although only the first key is

    Returns
    -------
    dict
    """

    out = defaultdict(list)
    for data in key_values:
        out[data[0]].append(data[1:])

    return dict(out)


class Orderable(object):

    """
    Wraps an object to make it orderable.  The `make_orderable()` function is
    intended to be the primary interface.
    """

    __slots__ = ['_obj', '_lt', '_le', '_gt', '_ge', '_eq']

    def __init__(self, obj, lt=True, le=True, gt=False, ge=False, eq=False):

        """
        Default parameters make the object sort as less than or equal to.

        Parameters
        ----------
        obj : object
            The object being made orderable.
        lt : bool, optional
            Set `__lt__()` evaluation.
        le : bool, optional
            Set `__le__()` evaluation.
        gt : bool, optional
            Set `__gt__()` evaluation.
        ge : bool, optional
            Set `__ge__()` evaluation.
        """

        self._obj = obj
        self._lt = lt
        self._le = le
        self._gt = gt
        self._ge = ge
        self._eq = eq

    @property
    def obj(self):

        """
        Handle to the object being made orderable.
        """

        return self._obj

    def __lt__(self, other):
        return self._lt

    def __le__(self, other):
        return self._le

    def __gt__(self, other):
        return self._gt

    def __ge__(self, other):
        return self._ge

    def __eq__(self, other):

        if isinstance(other, Orderable):
            return operator.eq(self.obj, other.obj)
        else:
            return operator.eq(self.obj, other)

    if six.PY2:
        def __getstate__(self):
            return {k: getattr(self, k) for k in self.__slots__}

        def __setstate__(self, state):
            for k, v in six.iteritems(state):
                setattr(self, k, v)


def make_orderable(*args, **kwargs):

    """
    Make any object orderable.

    Parameters
    ----------
    See `Orderable.__init__()`.

    Returns
    -------
    Orderable
    """

    return Orderable(*args, **kwargs)


class _OrderableNone(Orderable):

    """
    Like `None` but orderable.
    """

    def __init__(self):

        """
        Use the instantiated `OrderableNone` variable.
        """

        super(_OrderableNone, self).__init__(None)

    def __eq__(self, other):
        if isinstance(other, OrderableNone):
            return True
        else:
            return False

    def __str__(self):

        """
        For serialization purposes we produce the name of the class as a string.
        """

        return 'OrderableNone'


# Instantiate so we can make it more None-like
OrderableNone = _OrderableNone()


def merge_partitions(*partitions, **kwargs):

    """
    Merge data from multiple `partition()` operations into one dictionary.

    Parameters
    ----------
    partitions : *args
        Dictionaries from `partition()`.
    sort : bool, optional
        Sort partitioned data as it is merged.  Uses `heapq.merge()` so within
        each partition's key, all values must be sorted smallest to largest.

    Returns
    -------
    dict
        {key: [values]}
    """

    sort = kwargs.pop('sort', False)
    assert not kwargs, "Unrecognized kwargs: {}".format(kwargs)

    partitions = (
        six.iteritems(p) if isinstance(p, dict) else p for p in partitions)

    out = defaultdict(list)

    if not sort:
        for ptn in partitions:
            for key, values in ptn:
                out[key].extend(values)
    else:
        for ptn in partitions:
            for key, values in ptn:
                out[key] = tuple(
                    heapq_merge(out[key], values, key=lambda x: x[0]))

    return dict(out)


def count_lines(
        path,
        buffer=io.DEFAULT_BUFFER_SIZE,
        linesep=os.linesep,
        encoding='utf-8'):

    """
    Quickly count the number of lines in a text file.  Useful for computing an
    optimal `chunksize`.

    Comparable to `$ wc -l` for files larger than ``~100 MB``, and significantly
    faster as the file gets smaller (ignoring Python interpreter startup and
    imports).  For reference just looping over all the lines in a 1.2 GB file
    takes ~6 or 7 seconds, but `count_lines()` takes ~1.5.

    For reference just looping over all the lines in the ``1.2 GB`` file takes
    ``~6 to 7 sec``.

    Speed is achieved by reading the file in blocks and counting the occurrence
    of `linesep`.  For `linesep` strings that are larger than 1 byte we
    check to make sure a `linesep` was not split across blocks.

    Scott Persinger on StackOverflow gets credit for the core logic.
    http://stackoverflow.com/questions/845058/how-to-get-line-count-cheaply-in-python

    Parameters
    ----------
    path : str
        Path to input text file.
    buffer : int, optional
        Buffer size in bytes.
    linesep : str, optional
        Newline character.  Cannot be longer than 2 bytes.
    encoding : str, optional
        Encoding of newline character so it can be converted to `bytes()`.

    Returns
    -------
    int
    """

    nl = bytearray(linesep.encode(encoding))
    size = os.stat(path).st_size

    if len(nl) > 2:
        raise ValueError(
            "Cannot handle linesep characters larger than 2 bytes.")

    # File is small enough to just process in one go
    elif size < buffer:
        with open(path, 'rb', buffering=buffer) as f:
            return getattr(f, 'raw', f).read(buffer).count(nl)  # No raw in PY2

    # Process in chunks
    # Number of chunks is pre-determined so that the last chunk can be
    # read into a fresh array to avoid double-counting
    else:

        buff = bytearray(buffer)
        blocks = size // buffer
        lines = 0

        with open(path, 'rb') as f:

            # Optimize the loops a bit in case we are working with a REALLY
            # big file
            readinto = getattr(f, 'raw', f).readinto  # no raw in PY2
            count = buff.count

            for i in six.moves.xrange(blocks):
                readinto(buff)
                lines += count(nl)

                # linesep is something like \r\n, which means it could be split
                # across blocks, so we need an additional check
                if buff[-1] == nl[0]:
                    lines += 1

            # The last bit of data in the file is smaller than a block
            # We can't just read this into the constant buffer because
            # it the remaining bytes would still be populated by the
            # previous block, which could produce duplicate counts.
            lines += getattr(f, 'raw', f).read().count(nl)  # No raw in PY2

        return lines


def popitems(dictionary, sort=False, **kwargs):

    """
    Like `dict.popitem()` but produces an iterator over all key, value pairs.
    Used to maintain a low memory footprint.  Can optionally produce keys in
    sorted order.

    Parameters
    ----------
    dictionary : dict
        `dict()` to process.
    sort : bool, optional
        Produce keys in sorted order.
    kwargs : **kwargs, optional
        Keyword arguments for `sorter()` - only used if `sort=True`.

    Yields
    ------
    tuple
        (key, value)
    """

    if not sort:
        while True:
            try:
                yield dictionary.popitem()
            except KeyError:
                raise StopIteration
    else:
        for k in sorted(dictionary.keys(), **kwargs):
            yield k, dictionary.pop(k)
