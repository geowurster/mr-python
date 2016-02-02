"""
In-memory MapReduce - for those weird use cases ...
"""


import functools
import logging

from tinymr import _mrtools
from tinymr import base
from tinymr import tools
from tinymr.tools import runner


class MemMapReduce(base.BaseMapReduce):

    def __call__(self, stream, log_level=logging.NOTSET):

        original_log_level = self.logger.level
        self.logger.setLevel(log_level)

        sliced = tools.slicer(stream, self.map_chunksize)

        # Map, partition, combine, partition
        self.logger.info(
            "Running map, combine, and partition phase with %s jobs, chunksize "
            "%s, sort_map=%s, and sort_combine=%s",
            self.map_jobs, self.map_chunksize, self.sort_map, self.sort_combine)

        with runner(self._map_combine_partition, sliced, self.map_jobs) as mcp:
            partitioned = tools.merge_partitions(*mcp, sort=self.sort_combine)

        self.logger.info("Finished map with %s keys", len(partitioned))
        self.logger.info("Initializing reduce phase")
        self.init_reduce()

        # Run all partition jobs
        reducer_input = partitioned
        for rj in self._reduce_job_confs:

            self.logger.info("Running reduce job %s", rj)

            # Pin the reduce job so we can treat it like a lambda
            func = functools.partial(
                self._reduce_partition, reducer=rj.reducer, sort=rj.sort)

            reducer_input = _mrtools.strip_sort_key(reducer_input)
            sliced = tools.slicer(reducer_input, rj.chunksize)
            with runner(func, sliced, rj.jobs) as reduced:
                partitioned = tools.merge_partitions(*reduced, sort=rj.sort)

            self.logger.info(
                "Finished reduce job %s with %s keys", rj, len(partitioned))

        partitioned = _mrtools.strip_sort_key(partitioned)

        self.logger.info("Sorting output data by key")
        if self.sort_output:
            partitioned = self._output_sorter(partitioned)

        try:
            self.logger.info("Producing output dataset")
            return self.output(partitioned)
        finally:
            self.logger.setLevel(original_log_level)
