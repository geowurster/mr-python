"""In-memory MapReduce. Get weird."""


import abc
from collections import defaultdict
from inspect import isgeneratorfunction
import itertools as it
from functools import partial
import operator as op
import sys

from .exceptions import KeyCountError


class MapReduce(object):

    """"""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def mapper(self, item):
        """"""

    @abc.abstractmethod
    def reducer(self, key, values):
        """"""

    def output(self, items):
        return items

    @property
    def sort_map_with_value(self):
        return False

    @property
    def sort_map_reverse(self):
        return False

    @property
    def sort_reduce_with_value(self):
        return False

    @property
    def sort_reduce_reverse(self):
        return False

    def close(self):
        """"""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __partition_and_sort(
            self, sequence, sort_with_value, reverse):

        sequence = iter(sequence)
        first = next(sequence)
        sequence = it.chain([first], sequence)

        if len(first) not in (2, 3):
            raise KeyCountError(
                "Expected data of size 2 or 3, not {}. Example: {}".format(
                    len(first), first))

        has_sort_element = len(first) == 3
        need_sort = has_sort_element or sort_with_value

        if has_sort_element:
            sequence = map(op.itemgetter(0, slice(1, 3)), sequence)

        if not need_sort:
            getval = None
            sortkey = None

        elif not has_sort_element and sort_with_value:
            getval = lambda x: x
            sortkey = None

        else:
            getval = op.itemgetter(1)
            if sort_with_value:
                sortkey = None
            else:
                sortkey = op.itemgetter(0)

        partitioned = defaultdict(list)
        for ptn, vals in sequence:
            partitioned[ptn].append(vals)

        if need_sort:
            partitioned = {
                p: (v.sort(key=sortkey, reverse=reverse), list(map(getval, v)))[1]
                for p, v in iteritems(partitioned)
            }

        return partitioned

    def __call__(self, sequence, mapper_map=None, reducer_map=None):

        # If 'mapper()' is a generator and it will be executed in some job
        # pool, wrap it in a function that expands the returned generator
        # so that the pool can serialize results and send back. Be sure to
        # wrap properly to preserve any docstring present on the method.
        mapper = self.mapper
        if mapper_map is not None and isgeneratorfunction(self.mapper):
            mapper = partial(_wrap_mapper, mapper=self.mapper)

        # Same as 'mapper()' but for 'reducer()'.
        reducer = self.reducer
        if reducer_map is not None:
            reducer = partial(_wrap_reducer, reducer=self.reducer)

        # Run map phase. If 'mapper()' is a generator flatten everything to
        # a single sequence.
        mapper_map = mapper_map or map
        mapped = mapper_map(mapper, sequence)
        if isgeneratorfunction(self.mapper):
            mapped = it.chain.from_iterable(mapped)

        # Partition and sort (if necessary).
        partitioned = self.__partition_and_sort(
            mapped,
            sort_with_value=self.sort_map_with_value,
            reverse=self.sort_map_reverse)

        # Run reducer. Be sure not to hold on to a pointer to the partitioned
        # dictionary. Instead replace it with a pointer to a generator.
        reducer_map = reducer_map or it.starmap
        partitioned = iteritems(partitioned)
        reduced = reducer_map(reducer, partitioned)

        # If reducer is a generator expand to a single sequence.
        if isgeneratorfunction(self.reducer):
            reduced = it.chain.from_iterable(reduced)

        # Partition and sort (if necessary).
        partitioned = self.__partition_and_sort(
            reduced,
            sort_with_value=self.sort_reduce_with_value,
            reverse=self.sort_reduce_reverse)

        # The reducer can yield several values or it can return a single value.
        # When the operating under the latter condition extract that value and
        # pass that on as the single output value.
        if not isgeneratorfunction(self.reducer):
            partitioned = {k: next(iter(v)) for k, v in iteritems(partitioned)}

        # Be sure not to pass a 'defualtdict()' as output.
        return self.output(dict(partitioned))


def _wrap_mapper(item, mapper):
    return tuple(mapper(item))


def _wrap_reducer(key_value, reducer):
    return tuple(reducer(*key_value))


if sys.version_info.major == 2:

    import copy_reg

    map = it.imap

    def iteritems(d):
        return d.iteritems()

    def _reduce_method(m):
        if m.im_self is None:
            return getattr, (m.im_class, m.im_func.func_name)
        else:
            return getattr, (m.im_self, m.im_func.func_name)

    copy_reg.pickle(type(MapReduce.mapper), _reduce_method)

else:

    def iteritems(d):
        return d.items()
