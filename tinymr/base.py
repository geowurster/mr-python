"""
Base classes.  Subclass away!
"""


from itertools import chain

import six

from tinymr import _mrtools
from tinymr import errors
from tinymr import tools


class BaseMapReduce(object):

    """
    Base class for various MapReduce implementations.  Not all methods are
    used by every implementation.
    """

    def __enter__(self):

        """
        See `__exit__` for context manager usage.
        """

        # if self.closed:
        #     raise IOError("MapReduce task is closed - cannot reuse.")

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
    def jobs(self):

        """
        Default number of jobs to run in parallel for each phase.

        Returns
        -------
        int
        """

        return 1

    @property
    def map_jobs(self):

        """
        Number of jobs to run in parallel during the map phase.  Defaults
        to `jobs` property.

        Returns
        -------
        int
        """

        return self.jobs

    @property
    def sort_jobs(self):

        """
        Number of jobs to run in parallel during the sort phases.  Defaults
        to `jobs` property.

        Returns
        -------
        int
        """

        return self.jobs

    @property
    def reduce_jobs(self):

        """
        Number of jobs to run in parallel during the reduce phase.  If `None`,
        defaults to `jobs` property.

        Returns
        -------
        int
        """

        return self.jobs

    @property
    def chunksize(self):

        """
        Amount of data to process in each `job`.

        Returns
        -------
        int
        """

        return 1

    @property
    def map_chunksize(self):

        """
        Amount of data to process in each `job` during the map phase.
        Defaults to `chunksize`.
        """

        return self.chunksize

    @property
    def sort_chunksize(self):

        """
        Amount of data to process in each `job` during the sort phase.
        Defaults to `chunksize`.
        """

        return self.chunksize

    @property
    def reduce_chunksize(self):

        """
        Amount of data to process in each `job` during the reduce phase.
        Defaults to `chunksize`.
        """

        return self.chunksize

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
    def sort_output(self):

        """
        Pass data to `output()` sorted by key.

        Returns
        -------
        bool
        """

        return self.sort

    @property
    def sort_reduce(self):

        """
        Sort the output from each `reducer()` before executing the next or
        before being passed to `output()`.

        Define one property per reducer, so `reducer2()` would be `sort_reduce2`.

        Returns
        -------
        bool
        """

        return self.sort

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

    def output(self, pairs):

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

    @property
    def _reduce_jobs(self):

        reducers = tools.sorter(filter(
            lambda x: not x.startswith('_') and 'reducer' in x,
            dir(self)))

        for r in reducers:
            yield _mrtools.ReduceJob(
                reducer=getattr(self, r),
                sort=getattr(self, 'sort_{}'.format(r.replace('reducer', 'reduce'))),
                jobs=getattr(self, '{}_jobs'.format(r.replace('reducer', 'reduce'))),
                chunksize=getattr(self, '{}_jobs'.format(r.replace('reducer', 'reduce'))))

    def _map_combine_partition(self, stream):

        """
        Run `mapper()`, partition, `combiner()` (if implemented) and partition
        on a chunk of input data.  Data may be sorted between each phase
        according to the control properties.

        Produces a dictionary of partitioned data with sort keys intact.

        Parameters
        ----------
        stream : iter
            Input data passed to the MapReduce task.

        Returns
        -------
        dict
            {key: [(sort, data), (sort, data), ...]}
        """

        # Map, partition, and convert back to a `(key, [v, a, l, u, e, s])` stream
        mapped = chain(*(self.mapper(item) for item in stream))
        map_partitioned = tools.partition(mapped)
        map_partitioned = six.iteritems(map_partitioned)

        if self.sort_map:
            map_partitioned = _mrtools.sort_partitioned_values(map_partitioned)

        try:

            # The generators within this method get weird and don't break
            # properly when wrapped in this try/except
            # Instead we just kinda probe the `combiner()` to see if it exists
            # and hope it doesn't do any setup.
            self.combiner(None, None)
            has_combiner = True

        except errors.CombinerNotImplemented:
            has_combiner = False

        if has_combiner:
            map_partitioned = _mrtools.strip_sort_key(map_partitioned)
            combined = chain(*(self.combiner(k, v) for k, v in map_partitioned))
            combine_partitioned = tools.partition(combined)
            combine_partitioned = six.iteritems(combine_partitioned)

        # If we don't have a combiner then we don't need to partition either
        # because we're already dealing with partitioned output from the
        # map phase
        else:
            combine_partitioned = map_partitioned

        # If we don't have a combiner or if we're not sorting, then whatever
        # we got from the mapper is good enough
        if has_combiner and self.sort_combine:
            combine_partitioned = _mrtools.sort_partitioned_values(combine_partitioned)

        return dict(combine_partitioned)

    def _reduce_partition(self, stream, reducer, sort):

        reduced = chain(*(reducer(k, v) for k, v in stream))
        partitioned = tools.partition(reduced)
        partitioned = six.iteritems(partitioned)

        if sort:
            partitioned = _mrtools.sort_partitioned_values(partitioned)

        return tuple(partitioned)

    def _output_sorter(self, kv_stream):

        """
        Sort data by key before it enters `output()`.

        Parameters
        ----------
        kv_stream : iter
            Producing `(key, iter(values))`.

        Yields
        ------
        tuple
        """

        return ((k, v) for k, v in tools.sorter(kv_stream, key=lambda x: x[0]))
