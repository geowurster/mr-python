"""
Base classes.  Subclass away!
"""


from collections import defaultdict
from contextlib import contextmanager
from itertools import chain

from tinymr import errors
from tinymr import tools

import six


class MRBase(object):

    """
    Base class for various MapReduce implementations.  Not all methods are
    used by every implementation.
    """

    _closed = False

    def __enter__(self):

        """
        See `__exit__` for context manager usage.
        """

        if self.closed:
            raise IOError("MapReduce task is closed - cannot reuse.")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):

        """
        Some MapReduce jobs benefit from storing data on the task class, but
        this data
        """

        self._closed = True
        self.close()

    def __del__(self):
        self.close()

    @property
    def sort(self):

        """
        Disable all sorting phases.  Setting individual properties overrides
        this setting for individual phases.

        Returns
        -------
        bool
        """

        return True

    @property
    def sort_map(self):

        """
        Sort the output from the map phase before the combine phase.

        Returns
        -------
        bool
        """

        return self.sort

    @property
    def sort_combine(self):

        """
        Sort the output from the combine phase before the partition phase.

        Returns
        -------
        bool
        """

        return self.sort

    @property
    def sort_final_reduce(self):

        """
        Pass data to the `final_reducer()` sorted by key.

        Returns
        -------
        bool
        """

        return self.sort

    @property
    def sort_reduce(self):

        """
        Sort the output from the `reducer()` phase before the `final_reducer().

        Returns
        -------
        bool
        """

        return self.sort

    @property
    def closed(self):

        """
        Indicates whether or not the MapReduce task is closed.

        Returns
        -------
        bool
        """

        return self._closed

    def close(self):

        """
        Allows the user an opportunity to destroy any connections or data
        structures created in an `init` step.  See `__exit__` for more
        information.  Note that the task will only be marked as `closed` if
        used as a context manager or if this method sets `_closed = True`.

        If a task does not need a teardown step then the instance can be
        re-used multiple times with different datasets as long as it is _not_
        used as a context manager.
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

        # Not required so we raise a special exception that we can catch later
        # Raising NotImplementedError also causes linters and code inspectors
        # to prompt the user to implement this method when it is not required.
        raise errors._CombinerNotImplemented

    def init_reduce(self):

        """
        Called immediately prior to the reduce phase and gives the user an
        opportunity to make adjustments now that the entire dataset has been
        observed in the map phase.
        """

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

            for kv_data in psd_stream:
                partitioned[kv_data[0]].append(kv_data)

            yield partitioned

        finally:
            partitioned = None

    def _sorter(self, key_values, fake=False):

        """
        Produces sorted data without any keys.

        Parameters
        ----------
        data : iter
            Produces tuples from the map phase.
        fake : bool, optional
            Don't do the sort - just strip off the data key.

        Yields
        ------
        iter
            Sorted data without keys.
        """

        for key, values in key_values:
            values = iter(values) if fake else tools.sorter(values, key=lambda x: x[-2])
            yield key, (v[-1] for v in values)

    def _parallel_sorter(self, data):

        """
        Wrapper for `_sorter()` for use when processing in parallel.

        Parameters
        ----------
        data : iter
            Data for sorting.
        """

        return [(k, tuple(v)) for k, v in self._sorter(data)]

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

    def _combine(self, kv_stream):

        """
        Apply the `combiner()` across a stream of `(key, values)` pairs.
        """

        return chain(*(self.combiner(key, values) for key, values in kv_stream))

    def _final_reducer_sorter(self, kv_stream):

        """
        Sort data by key before it enters the `final_reducer()`.

        Parameters
        ----------
        kv_stream : iter
            Producing `(key, iter(values))`.

        Yields
        ------
        tuple
        """

        return ((k, v) for k, v in tools.sorter(kv_stream, key=lambda x: x[0]))

    def _runtime_validate(self):

        """
        Validate a MapReduce task when it is executed.  We give the user the
        `__init__()` so we can't do any validation there.  Call at the beginning
         of `__call__()`.
        """

        if self.closed:
            raise errors._ClosedTask

    @contextmanager
    def _merge_partitions(self, *partitions):

        """
        When processing in parallel data can be pre-partitioned within each
        `multiprocessing` job.  Take advantage of this and merge the
        data once it is provided by each job.

        Partitioned data must look like:

            (
                key, [(key, data), (key, data), (key, data)],
                key2, [(key2, data), (key2, data), (key2, data)],
            )

        So in a word count task with a `combiner()` one piece of partitioned
        data might look like:

            (
                'the', [('the', 4), ('the', 2), ('the', 5)],
                'name', [('name', 1), ('name', 4)],
            )

        Parameters
        ----------
        partitions : iter
            Iterable producing pre-partitioned data.  See example above for format.

        Returns
        -------
        defaultdict
            See `_partition()`.
        """

        out = defaultdict(list)

        try:

            for ptn in partitions:
                for key, values in ptn:
                    out[key] += values

            yield out

        finally:
            out = None
