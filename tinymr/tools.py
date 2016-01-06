"""
Tools for building MapReduce implementations.
"""


import itertools as it
import multiprocessing as mp
import multiprocessing.pool

from six.moves import zip

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


def runner(func, iterable, jobs):

    """
    The `multiprocessing` module can be difficult to debug and introduces some
    overhead that isn't needed when only running one job.  Use a generator in
    this case instead.

    Parameters
    ----------
    func : callable
        Callable object to map across `iterable`.
    iterable : iter
        Data to process.
    jobs : int
    """

    if jobs < 1:
        raise ValueError("jobs must be >= 1, not: {}".format(jobs))
    elif jobs == 1:
        return (func(i) for i in iterable)
    else:
        return mp.Pool(jobs).imap_unordered(func, iterable)


# class DefaultOrderedDict(OrderedDict):
#
#     def __init__(self, default_factory, *args, **kwargs):
#
#         if not callable(default_factory):
#             raise TypeError("default_factory must be callable")
#
#         super(DefaultOrderedDict, self).__init__(*args, **kwargs)
#         self.default_factory = default_factory
#
#     def __missing__(self, key):
#         v = self.default_factory()
#         super(DefaultOrderedDict, self).__setitem__(key, v)
#         return v
#
#     def __repr__(self):
#         return "{cname}({df}, {dr})".format(
#             cname=self.__class__.__name__,
#             df=self.default_factory,
#             dr=super(DefaultOrderedDict, self).__repr__())
#
#     def copy(self):
#         return self.__class__(self.default_factory, self)


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

    def __init__(self, **kwargs):

        """
        See `Orderable()` - `obj` is automatically set to `None`.
        """

        assert 'obj' not in kwargs, "Cannot supply obj argument"

        super(_OrderableNone, self).__init__(None, **kwargs)


# Instantiate so we can make it more None-like
OrderableNone = _OrderableNone(eq=False)
