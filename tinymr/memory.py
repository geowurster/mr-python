"""
In-memory MapReduce - for those weird use cases ...
"""


from contextlib import closing
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
        Default number of `multiprocessing` jobs to use.  Can set this instead
        of setting each property individually.

        Returns
        -------
        int
        """

        return 1

    @property
    def chunksize(self):

        """
        Default amount of data to process in each `multiprocessing` job.  Can
        set this instead of setting each property individually.

        Returns
        -------
        int
        """

        return 1

    @property
    def map_jobs(self):

        """
        Number of map operations to run in parallel.

        Returns
        -------
        int
        """

        return self.jobs

    @property
    def map_chunksize(self):

        """
        Maximum number of items to send to each map operation.  The input
        stream is sliced into chunks with `tinymr.tools.slicer()`.  If `None`
        and the input stream has a `__len__()`, the data is spread evenly
        across `map_jobs`, otherwise the default is `1`.

        Returns
        -------
        int or None
        """

        return None

    @property
    def reduce_jobs(self):

        """
        Number of reduce operations to run in parallel.

        Returns
        -------
        int
        """

        return self.jobs

    @property
    def reduce_chunksize(self):

        """
        Maximum number keys to send to each reduce operation.  If `None`, then
        all keys are spread evenly across `reduce_jobs`.

        Returns
        -------
        int or None
        """

        return None

    @property
    def sort_jobs(self):

        """
        Number of sort operations to run in parallel.

        Returns
        -------
        int
        """

        return self.jobs

    @property
    def sort_chunksize(self):

        """
        Maximum number keys to send to each sort operation.  If `None`, then
        all keys are spread evenly across `sort_jobs`.
        """

        return None

    def _get_default_chunksize(self, data):

        """
        Figure out how many chunks the data should be split into for parallel
        processing operations.
        """

        try:
            detected = len(data) % self.map_jobs
        except TypeError:
            detected = None

        return detected or self.chunksize

    def _mpcp(self, chunk_data):

        """
        Map, partition, combine, and partition a chunk of data.

        Data reaching this method has been split into chunks and is being
        processed in parallel.  Since we are already in a multiprocess we can
        partition the data we have access to so we don't have to partition
        everything in serial.

        Parameters
        ----------
        chunk_data : tuple
            First element is the chunk ID for logging purposes and the second
            is an iterable producing the data to process.

        Returns
        -------
        iter
            Partitioned data as `(key, tuple)` ready for `_merge_partitions()`.
        """

        # TODO: Could be extra smart about sorting data here.
        #   We're already inside a multiprocessing job so we could pre-sort
        #   combined data as well and then do a heapq sort if `self.sort_combine=True`.

        chunk, data = chunk_data

        logger.debug("Running map and partition for chunk %s", chunk)
        with self._partition(self._map(data)) as partitioned:

            logger.debug("Sorting data for chunk %s", chunk)
            sorted_data = self._sorter(six.iteritems(partitioned), fake=not self.sort_map)

            try:
                logger.debug("Combining data for chunk %s", chunk)
                combined = self._combine(sorted_data)
            except mr.errors.CombinerNotImplemented:
                logger.debug("No combiner implemented")
                combined = sorted_data

        logger.debug("Pre-partitioning data for chunk %s", chunk)
        with self._partition(combined) as partitioned:
            return tuple(six.iteritems(partitioned))

    def _rp(self, chunk_data):

        """
        Reduce and partition.

        We are inside a `multiprocessing` job so we can pre-partition the data
        and merge it later.
        """

        # TODO: Smarter sorting (see _mpcp)?

        chunk, data = chunk_data

        logger.debug("Reducing and partitioning data for chunk %s", chunk)
        with self._partition(self._reduce(data)) as partitioned:
            return tuple(six.iteritems(partitioned))

    def __call__(self, stream):

        # TODO: This can be simplified through abstraction to remove redundant code.  See sorting especially.

        # ==== Validate ==== #

        logger.debug("Validating task")
        self._runtime_validate()

        # ==== Map + Combine ==== #

        map_chunks = self.map_chunksize or self._get_default_chunksize(stream)
        logger.debug("Starting map + combine phase with chunksize %s", map_chunks)
        with closing(mp.Pool(self.map_jobs)) as pool:
            it = enumerate(mr.tools.slicer(stream, map_chunks))
            combined = tuple(chain(*pool.imap_unordered(self._mpcp, it)))

        # ==== Partition ==== #

        logger.debug("Merging partitions ...")
        with self._merge_partitions(combined) as partitioned:

            if self.sort_combine:

                sort_chunks = self.sort_chunksize or self._get_default_chunksize(partitioned)
                logger.debug("Set sort chunks to %s", sort_chunks)

                with closing(mp.Pool(self.sort_jobs)) as pool:
                    partitioned = six.iteritems(partitioned)
                    it = mr.tools.slicer(partitioned, sort_chunks)
                    logger.debug("Sorting data with %s chunks ...", sort_chunks)
                    sorted_data = tuple(chain(*pool.imap_unordered(self._parallel_sorter, it)))
            else:
                logger.debug("Skipping sort combine")
                sorted_data = tuple((k, tuple(v)) for k, v in self._sorter(
                    six.iteritems(partitioned), fake=not self.sort_combine))

        # ==== Reduce ==== #

        reduce_chunks = self.reduce_chunksize or self._get_default_chunksize(partitioned)
        logger.debug("Starting reduce phase with chunksize %s", reduce_chunks)
        with closing(mp.Pool(self.reduce_jobs)) as pool:
            it = enumerate(mr.tools.slicer(sorted_data, reduce_chunks))
            reduced = tuple(chain(*pool.imap_unordered(self._rp, it)))

        logger.debug("Merging partitions ...")
        with self._merge_partitions(reduced) as partitioned:

            if self.sort_combine:

                sort_chunks = self.sort_chunksize or self._get_default_chunksize(partitioned)
                logger.debug("Set sort chunks to %s", sort_chunks)

                with closing(mp.Pool(self.sort_jobs)) as pool:
                    partitioned = six.iteritems(partitioned)
                    it = mr.tools.slicer(partitioned, sort_chunks)
                    logger.debug("Sorting data with %s chunks ...", sort_chunks)
                    sorted_data = tuple(chain(*pool.imap_unordered(self._parallel_sorter, it)))
            else:
                logger.debug("Skipping sort reduce")
                sorted_data = self._sorter(
                    six.iteritems(partitioned), fake=not self.sort_combine)

        # ==== Final Reduce ==== #

        if self.sort_final_reduce:
            logger.debug("Sorting data by key for the final reducer ...")
            sorted_data = self._final_reducer_sorter(sorted_data)

        logger.debug("Initializing reduce ...")
        self.init_reduce()

        logger.debug("Starting final reducer ...")
        return self.final_reducer(sorted_data)
