"""
In-memory MapReduce - for those weird use cases ...
"""


from collections import defaultdict
from itertools import chain
import logging

from six import iteritems

from tinysort._backport_heapq import merge as heapq_merge
from tinymr import base
from tinymr import tools
from tinymr.tools import runner


def _sort_partition_values(values):

    """
    Handle the optional sort key.
    """

    return sorted(values, key=lambda x: x[:-1] or x[-1])


def _partition(stream, sort=False):

    """
    Partition a stream of `(key, [sort], value)` tuples into:

        {
            key: [([sort], value)]
        }
    """

    out = defaultdict(list)
    for keys in stream:
        out[keys[0]].append(keys[1:])

    if sort:
        out = {
            k: _sort_partition_values(values) for k, values in iteritems(out)}

    return out


class MemMRResults(object):

    """
    Wraps output results to avoid `repr()` confusion.
    """

    def __init__(self, stream):
        self._stream = iter(stream)

    def __iter__(self):
        return self._stream

    def __next__(self):
        return next(self._stream)

    next = __next__


class MemMapReduce(base.BaseMapReduce):

    """
    Base class for deriving in-memory MapReduce classes.
    """

    def _run_combine(self, kv_stream):

        """
        Wrap the combiner so it can be called as a function that produces a
        stream of `(key, sort, value)` tuples.
        """

        for k, values in kv_stream:
            for keys in self.combiner(k, (v[-1] for v in values)):
                yield keys

    def _run_map_job(self, stream):

        """
        Run map + combine phase and prep data for reduce phase.

        Order of operations, assuming everything is implemented and enabled:

            * mapper()
            * sort
            * partition
            * combine
            * partition
            * sort

        This method produces a dictionary with sorted values, so when merging
        data from other mappers it is advantageous to use `heapq.merge()` since
        the data has already been sorted within each map job.
        """

        mapped = chain(*(self.mapper(i) for i in stream))
        partitioned = _partition(mapped, sort=self.sort_map)

        if self._has_combiner:
            combined = self._run_combine(iteritems(partitioned))
            partitioned = _partition(combined, sort=self.sort_combine)

        return partitioned

    def _run_reduce_job(self, key_values_stream):

        """
        Run the reduce phase.

        Order of operations:

            * finish sorting the data from the map phase
            * reduce
            * partition
            * sort
        """

        partitioned = defaultdict(list)
        for key, values in key_values_stream:
            if self.sort_map or self.sort_combine:
                values = heapq_merge(*values)
            else:
                values = chain(*values)

            values = (v[-1] for v in values)

            for rkeys in self.reducer(key, values):
                partitioned[rkeys[0]].append(rkeys[1:])

        if self.sort_reduce:
            partitioned = {
                k: _sort_partition_values(v) for k, v in iteritems(partitioned)}

        return partitioned

    def __call__(self, stream, log_level=logging.NOTSET):

        """
        Run an in-memory MapReduce task.

        Parameters
        ----------
        stream : iter
            Input data.
        log_level : int, optional
            Each MapReduce task has its own `logger` property that is used for
            logging during execution.

        Returns
        -------
        MemMapReduce.output()
        """

        # Set up logging
        initial_log_level = self.logger.level
        self.logger.setLevel(log_level)

        # Chunk the input stream according to map_chunksize and start a
        # map / combine job.  The job runs the mapper, sorts, partitions,
        # combines, sorts, and produces a dictionary of {key, [(sort, value)]}
        # that are ready to be merged with the results from the other jobs.
        self.logger.debug("Running map")
        sliced = tools.slicer(stream, self.map_chunksize)
        with runner(self._run_map_job, sliced, self.map_jobs) as run:
            partitioned = defaultdict(list)
            for key, values in chain(*(iteritems(r) for r in run)):
                partitioned[key].append(values)
        self.logger.debug("Finished map with %s keys", len(partitioned))

        # User gets the class's __init__ for initializing both the task and the
        # map phase, so init_reduce() gives them a place to initialize the
        # entire reduce phase.
        self.logger.debug("Initializing reduce phase")
        self.init_reduce()

        # Run the reducer, which finished the map / combine sort and produces
        # a partitioned dictionary
        self.logger.debug("Running reduce")
        sliced = tools.slicer(iteritems(partitioned), self.reduce_chunksize)
        with runner(self._run_reduce_job, sliced, self.reduce_jobs) as run:
            partitioned = defaultdict(list)
            for key, values in chain(*(iteritems(r) for r in run)):
                partitioned[key].append(values)
        self.logger.debug("Finished reduce with %s keys", len(partitioned))

        # Sort output by key?
        if self.sort_output:
            out = sorted(iteritems(partitioned), key=lambda x: x[0])
        else:
            out = iteritems(partitioned)

        # Sort the reduced values
        if self.sort_reduce:
            out = ((k, heapq_merge(*v, key=lambda x: x[:-1] or x[-1]))
                   for k, v in out)
        else:
            out = ((k, chain(*v)) for k, v in out)

        # Strip off the sort keys
        out = ((k, tuple(v[-1] for v in values)) for k, values in out)

        try:
            return self.output(out)
        finally:
            # Set the log level back to the original value
            self.logger.setLevel(initial_log_level)
