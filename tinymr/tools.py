"""
Tools for building MapReduce implementations.
"""


from collections import defaultdict
import heapq
import itertools as it
import multiprocessing as mp

import six
from six.moves import zip

from tinymr._backport_heapq import merge as heapq_merge
from tinymr import errors


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


def sorter(*args, **kwargs):

    """
    Wrapper for the builtin `sorted()` that produces a better error when
    unorderable types are encountered.

    Instead of:

        >>> sorted(['1', 1])
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        TypeError: unorderable types: int() < str()

    we get a `tinymr.errors.UnorderableKeys` exception.

    Python 2 is much more forgiving of unorderable types so the example above
    does not raise an exception.

    Parameters
    ----------
    *args : *args
        Positional arguments for `sorted()`.
    **kwargs : **kwargs
        Keyword arguments for `sorted()`.

    Raises
    ------
    tinymr.errors.UnorderableKeys

    Returns
    -------
    list
        Output from `sorted()`.
    """

    try:
        return sorted(*args, **kwargs)
    except TypeError as e:
        if 'unorderable' in str(e):
            raise errors._UnorderableKeys
        else:
            raise e


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
    Make any object orderable.
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
        eq : bool or None, optional
            Set `__eq__()` evaluation.  Set to `None` to enable a real
            equality check.
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
        if self._eq is None:
            return isinstance(other, self.__class__) and other.obj == self.obj
        else:
            return self._eq


class _OrderableNone(Orderable):

    """
    Like `None` but orderable.
    """

    def __init__(self):

        """
        Use the instantiated `OrderableNone` variable.
        """

        super(_OrderableNone, self).__init__(None, eq=None)


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

    partitions = (six.iteritems(ptn) if isinstance(ptn, dict) else ptn for ptn in partitions)

    out = defaultdict(list)

    if not sort:
        for ptn in partitions:
            for key, values in ptn:
                out[key].extend(values)
    else:
        for ptn in partitions:
            for key, values in ptn:
                out[key] = tuple(heapq_merge(out[key], values, key=lambda x: x[0]))

    return dict(out)
