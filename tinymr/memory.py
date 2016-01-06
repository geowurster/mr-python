"""
In-memory MapReduce - for those weird use cases ...
"""


from collections import defaultdict
from contextlib import contextmanager
from itertools import chain
import logging
import multiprocessing as mp

import six

import tinymr as mr
import tinymr.base
import tinymr.errors
import tinymr.tools


logger = logging.getLogger('tinymr')
logger.setLevel(logging.DEBUG)


class MRSerial(mr.base.MRBase):

    """
    For MapReduce operations that don't benefit from sorting or parallelism.

    The `mapper()` and `reducer()` must yield 2 element tuples.  The first
    element is used for partitioning and the second is data.
    """

    def __call__(self, stream):

        self._runtime_validate()

        with self._partition(self._map(stream)) as partitioned:

            sorted_data = self._sorter(six.iteritems(partitioned), fake=not self.sort_map)

        with self._partition(self._reduce(sorted_data)) as partitioned:

            sorted_data = self._sorter(six.iteritems(partitioned), fake=not self.sort_reduce)

            if self.sort_final_reduce:
                sorted_data = self._final_reducer_sorter(sorted_data)

            self.init_reduce()
            return self.final_reducer(sorted_data)


class MRParallel(mr.base.MRBase):

    """
    Parallelized map and reduce with an optional combine phase.
    """

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

    @contextmanager
    def _runner(self, func, iterable, jobs):

        if jobs == 1:
            pool = None
            proc = (func(i) for i in iterable)
        else:
            pool = mp.Pool(jobs)
            proc = pool.imap_unordered(func, iterable)

        try:
            yield proc
        finally:
            if pool is not None:
                pool.close()

    def _mpcp(self, data):

        """
        Map, partition, combine, partition
        """

        with self._partition(self._map(data)) as partitioned:

            sorted_data = self._sorter(six.iteritems(partitioned), fake=not self.sort_map)

        try:
            self.combiner(None, None)
            combined = self._combine(sorted_data)
        except mr.errors.CombinerNotImplemented:
            combined = chain(*(mr.tools.mapkey(k, values) for k, values in sorted_data))

        with self._partition(combined) as partitioned:

            return partitioned

    def _rp(self, data):

        """
        Reduce, partition.
        """

        with self._partition(self._reduce(data)) as partitioned:

            return partitioned

    def _parallel_sorter(self, data, fake=False):

            """
            Wrapper for `_sorter()` for use when processing in parallel.

            Parameters
            ----------
            data : iter
                Data for sorting.
            """

            return [(k, tuple(v)) for k, v in self._sorter(data)]

    @contextmanager
    def _merge_partitions(self, partitions, sort=True):

        out = defaultdict(list)

        try:
            for key, values in chain(*(six.iteritems(ptn) for ptn in partitions)):
                out[key] += list(values)

            if sort:
                sliced = mr.tools.slicer(six.iteritems(out), self.sort_chunksize)
                with self._runner(self._parallel_sorter, sliced, self.sort_jobs) as sorted_data:
                    yield chain(*sorted_data)

            else:
                yield [(k, tuple(v)) for k, v in self._sorter(six.iteritems(out), fake=not sort)]

        finally:
            out = None

    def __call__(self, stream):

        # TODO: Figure out how/if these with statements are correct, AFTER writing tests

        self._runtime_validate()

        sliced = mr.tools.slicer(stream, self.map_chunksize)

        # Map, partition, combine, partition
        with self._runner(self._mpcp, sliced, self.map_jobs) as mpcp:

            # Merge the data that was processed in parallel
            with self._merge_partitions(mpcp, sort=self.sort_combine) as partitioned:

                # Reduce, partition
                self.init_reduce()
                sliced = mr.tools.slicer(partitioned, self.reduce_chunksize)
                with self._runner(self._rp, sliced, self.reduce_jobs) as rp:

                    # Merge the partitioned data that was processed in parallel
                    with self._merge_partitions(rp, sort=self.sort_reduce) as partitioned:

                        if self.sort_final_reduce:
                            partitioned = self._final_reducer_sorter(partitioned)

                        return self.final_reducer(partitioned)
