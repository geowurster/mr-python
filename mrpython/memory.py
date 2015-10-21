"""
In-memory MapReduce - for when you have a small amount of data that needs to
step through the MapReduce process without any I/O or parallelization.  BYOMR.

To set up a job:

    1. Gather input objects and assemble into an iterator.
    2. Define a `mapper` function that accepts one of these objects and yields
       one or more `key, value` tuples.
    3. Define a `reducer` function that accepts one of these `key, value`
       tuples and returns an object.
    4. Use `mr_memory(stream, mapper, reducer)`.  Output is a dictionary with
       keys from the mapper and sorted values from the reducer.
    5. The `mapit()` and `reduceit()` functions offer additional features that
       `mr_memory()` does not expose for more complicated use cases.

Consider the canonical MapReduce example: word count (oh brother ...):

    >>> import os
    >>>
    >>> from mrpython import mr_memory
    >>>
    >>> def mapper(line):
    ...     for word in line.split():
    ...         yield word, 1
    >>>
    >>> def reducer(word, frequency):
    ...     return sum(frequency)
    >>>
    >>> with open('infile.txt') as src, open('outfile.txt') as dst:
    ...     for word, count in mr_memory(src, mapper, reducer).items():
    ...         dst.write("{word}: {count}".format(word=word, count=count) + os.linesep)
"""


from collections import defaultdict
from collections import OrderedDict
from contextlib import contextmanager
from itertools import chain
from multiprocessing import Pool
from types import GeneratorType

import six


def mapit(stream, mapper, keysort_kwargs=None, valsort_kwargs=None):

    """
    Run an in-memory map operation.

    Parameters
    ----------
    stream : iter
        A stream of input objects.
    mapper : callable
        A function that accepts a single item from the `stream` and yields
        `key, value` tuples.  Can yield 0, 1, or many tuples.  Key can be
        any hashable object.
    keysort_kwargs : dict or None, optional
        Keyword arguments to add to `sorted()` when sorting keys in the output
        dictionary.
    valsort_kwargs : dict or None, optional
        Keyword arguments to add to `sorted()` when sorting values in the
        output dictionary.

    Returns
    -------
    dict
        Keys and values from the mapper.  Both are sorted.
    """

    keysort_kwargs = keysort_kwargs or {}
    valsort_kwargs = valsort_kwargs or {}

    partitioned = defaultdict(list)
    try:
        for item in stream:
            for key, data in list(mapper(item)):
                partitioned[key].append(data)

        return OrderedDict(
            (k, sorted(partitioned[k], **keysort_kwargs))
            for k in sorted(partitioned.keys(), **valsort_kwargs))

    finally:
        # Make sure we destroy what could be a large in-memory object
        partitioned = None


def reduceit(partitioned, reducer):

    """
    Run an in-memory reduce operation on the output of `mapit()`.

    Parameters
    ----------
    partitioned : dict
        The output from `mapit()`.  Keys and sorted values from the map phase.
    reducer : callable
        A function that accepts two positional arguments: key and sorted values.
        Must return a single value.

    Returns
    -------
    dict
        Input keys with values from the reduce phase.
    """

    return {k: reducer(k, v) for k, v in six.iteritems(partitioned)}


def mr_memory(stream, mapper, reducer):

    """
    Execute an in-memory map and reduce and receive a dictionary with sorted
    keys and values.
    """

    return reduceit(mapit(stream, mapper), reducer)



class MRMemory(object):

    """
    In-memory MapReduce for tiny datasets.  No `combiner` since all the data
    is already in memory.

    Order of operations:

        1. Map
        2. Partition
        3. Sort
        4. Reduce
    """

    def __call__(self, stream):

        """
        Construct and execute the MapReduce pipeline.
        """

        mapped = chain(*(self.mapper(item) for item in stream))
        with self._object_manager(self.partitioner(mapped)) as partitioned:
            sorted = ((k, self.sorter(k, v)) for k, v in six.iteritems(partitioned))
            for key, values in sorted:
                yield key, self.reducer(key, values)

    @contextmanager
    def _object_manager(self, obj):

        """
        Manage in-memory objects to make sure they are destroyed as quickly as
        possible.
        """

        try:
            yield obj
        finally:
            obj = None

    def mapper(self, item):

        """
        Receives an item from the input stream.  Yields `key, value` tuples.

        Parameters
        ----------
        item : object
            Something from the input stream.

        Yields
        ------
        tuple
            0, 1, or many `key, value` tuples.
        """

        raise NotImplementedError

    def partitioner(self, key_value_pairs):

        """
        Assemble data into groups, usually by key from the mapper.

        Parameters
        ----------
        key : object
            From the mapper.
        values : iter
            Unsorted values from the map phase.

        Returns
        ------
        dict
            Data organized by key from the mapper.  Values are lists of values
            from the mapper.

                {key from mapper: [list of values]}
        """

        partitioned = defaultdict(list)
        for key, value in key_value_pairs:
            partitioned[key].append(value)
        return partitioned

    def sorter(self, key, values):

        """
        Sort values before reducing.

        Parameters
        ----------
        key : object
            Key from the mapper.
        values : iter
            Unsorted values from the map phase.

        Yields
        ------
        objects
            Sorted items from one key from the reducer.
        """

        return (i for i in sorted(values))

    def reducer(self, key, values):

        """
        Receives a key identifying the data being processed and some data.

        Parameters
        ----------
        key : object
            Identifies the data being processed.
        values : iter
            Sorted data to process.

        Yields
        ------
        object
            Processed data.
        """

        raise NotImplementedError
