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

    >>> import re
    >>> from mrpython import MRMemory
    >>> class WordCount(MRMemory):
    ...
    ...     def __init__(self):
    ...         self.p = re.compile('[^a-zA-Z]+')
    ...
    ...     def scrub_word(self, word):
    ...         return self.p.sub('', word)
    ...
    ...     def mapper(self, item):
    ...         for word in item.split():
    ...             word = self.scrub_word(word).strip().lower()
    ...             yield word, 1
    ...
    ...     def reducer(self, key, values):
    ...         return sum(values)
    ...
    >>> mr = WordCount()
    >>> with open('LICENSE.txt') as f:
    ...     for word, count in mr(f):
    ...         print("{word}: {count}".format(word=word,  count=count))
"""


from collections import defaultdict
from contextlib import contextmanager
from itertools import chain
from multiprocessing import Pool

import six


class MRMemory(object):

    """
    In-memory MapReduce for tiny datasets.  No `combiner` since all the data
    is already in memory and only one mapper is executed.

    Order of operations:

        1. Map
        2. Partition
        3. Sort
        4. Reduce
    """

    def __call__(self, stream, jobs=1):

        """
        Construct and execute the MapReduce pipeline.

        Parameters
        ----------
        stream : iter
            Input for map phase.
        jobs : int, optional
            Semi-experimental.  Execute sorting and reducing in parallel.
            Seems to only be faster for very large datasets but requires all
            keys and values to be pickle-able and uses significantly more RAM.
            You probably want `jobs=1`, which is the default.

        Yields
        ------
        tuple
            key, value pairs from the reduce phase.
        """

        if jobs < 1:
            raise ValueError("jobs must be >= 1, not {}".format(jobs))

        mapped = chain(*(self.mapper(item) for item in stream))
        with self._object_manager(self.partitioner(mapped)) as partitioned:

            if jobs == 1:
                sorted = ((k, self.sorter(k, v)) for k, v in six.iteritems(partitioned))
                for key, values in sorted:
                    yield key, self.reducer(key, values)

            else:
                sorted_gen = Pool(jobs).imap_unordered(
                    self._p_sorter, six.iteritems(partitioned))
                for key, red in Pool(jobs).imap_unordered(self._p_reducer, sorted_gen):
                    yield key, red

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

    def _p_sorter(self, key_values):

        """
        Helper method for sorting multiple keys in parallel.  Have to expand
        generators into tuples so they can be pickled.

        Parameters
        ----------
        key_values : tuple
            key, value from mapper.

        Returns
        -------
        tuple
            key, self.sorter(key, values)
        """

        key, values = key_values
        values = tuple(values)
        return key, tuple(self.sorter(key, values))

    def _p_reducer(self, key_values):

        """
        Helper method for reducing multiple keys in parallel.  Have to expand
        generators into tuples so they can be pickled.

        Parameters
        ----------
        key_values : tuple
            key, value from sorter.

        Returns
        -------
        tuple
            key, self.reducer(key, values)
        """

        key, values = key_values
        values = tuple(values)
        return key, self.reducer(key, values)

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
