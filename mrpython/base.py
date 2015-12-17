"""
Base classes.  Subclass away!
"""


from collections import defaultdict
from contextlib import contextmanager
from itertools import chain


class MRBase(object):

    """
    Base class for various MapReduce implementations.  Not all methods are
    used by every implementation.
    """

    def mapper(self, item):

        """
        Receives an item from the input data stream and yields one or more
        2 or 3 element tuples.

        Elements are used for:

            1. Partitioning
            2. Sorting
            3. The data

        If a two element tuple is produced then the first element will be used
        for partitioning and sorting.

        Parameters
        ----------
        item : object
            Something to process.

        Yields
        ------
        tuple
        """

        raise NotImplementedError

    def combiner(self, key, values):

        """
        Not used by every MR implementation.  Receives the sorted output from
        a single `mapper()` and acts as an initial `reducer()` to cut the data
        volumn.

        See `reducer()` for more information.
        """

        raise NotImplementedError

    def sorter(self, key, pairs):
        
        """
        Produces sorted data without any keys.

        Parameters
        ----------
        key : object
            Identifier for this group of data.
        pairs : iter
            Produces two element tuples, the first of which is used for sorting
            and the second is yielded as data.

        Yields
        ------
        iter
            Sorted data without keys.
        """

        for v in sorted(pairs, key=lambda x: x[0]):
            yield v[-1]

    def reducer(self, key, values):

        """
        Receives sorted data for a single key for processing and yields 3
        element tuples.

        Parameters
        ----------
        key : object
            The key for this group of data.
        values : iter
            Data to process.

        Yields
        ------
        tuple
            3 elements: `(partition, sort, data)`.
        """

        raise NotImplementedError

    def final_reducer(self, pairs):

        """
        Receives `(key, value)` pairs from each reducer.  The output of this
        method is handed back to the parent process, so do whatever you want!

        Default implementation is to yield a generator of `(key, values)` pairs
        where `key` is a reducer key and `values` is the output from that
        reducer as a tuple - this is always an iterable object but MAY be a
        generator.

        Properties
        ----------
        pairs : iter
            `(key, data)` pairs from each reducer.

        Returns
        -------
        object
        """

        return ((key, tuple(values)) for key, values in pairs)

    @contextmanager
    def _partition(self, psd_stream):

        """
        Context manager to partition data and destroy it when finished.

        Parameters
        ----------
        psd_stream : iter
            Produces `(partition, sort, data)` tuples.
        sort_key : bool, optional
            Some MapReduce implementations don't benefit from sorting, and
            therefore do not pass around a sort key.  Set to `False` in this
            case.

        Returns
        -------
        defaultdict
            Keys are partition keys and values are lists of `(sort, data)` tuples.
        """

        partitioned = defaultdict(list)

        try:

            for pkey, skey, data  in psd_stream:
                partitioned[pkey].append((skey, data))

            yield partitioned

        finally:
            partitioned = None

    @contextmanager
    def _partition_no_sort(self, pd_stream):

        """
        Same as `_partition()` except the input stream is `(partition, data)`
        tuples.  This exists as its own method to take advantage of the small
        optimization of knowing there are always 2 keys instead of 3.
        """

        partitioned = defaultdict(list)
        try:

            for pkey, data in pd_stream:
                partitioned[pkey].append(data)

            yield partitioned

        finally:
            partitioned = None

    def _sort(self, kv_stream):

        """
        Sorts partitioned data.

        Parameters
        ----------
        partitioned : iter
            Stream of `(sort, values)` where `sort` is the key to sort on.

        Yields
        ------
        tuple
            `(key, data)`
        """

        return ((key, self.sorter(key, data)) for key, data in kv_stream)

    def _map(self, stream):

        """
        Apply `mapper()` across the input stream.
        """

        return chain(*(self.mapper(item) for item in stream))

    def _reduce(self, kv_stream):

        """
        Apply the `reducer()` across a stream of `(key, values)` pairs.
        """

        return chain(*(self.reducer(key, values) for key, values in kv_stream))
