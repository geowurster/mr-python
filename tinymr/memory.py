"""
In-memory MapReduce - for those weird use cases ...
"""


import functools
import logging

from tinymr import _mrtools
from tinymr import base
from tinymr import tools
from tinymr.tools import runner


logger = logging.getLogger('tinymr')
logger.setLevel(logging.DEBUG)


class MapReduce(base.BaseMapReduce):

    def __call__(self, stream):

        sliced = tools.slicer(stream, self.map_chunksize)

        # Map, partition, combine, partition
        with runner(self._map_combine_partition, sliced, self.map_jobs) as mcp:
            partitioned = tools.merge_partitions(*mcp, sort=self.sort_combine)

        reducer_input = partitioned
        for rj in self._reduce_jobs:

            func = functools.partial(
                self._reduce_partition, reducer=rj.reducer, sort=rj.sort)

            reducer_input = _mrtools.strip_sort_key(reducer_input)
            sliced = tools.slicer(reducer_input, rj.chunksize)

            with runner(func, sliced, rj.jobs) as reduced:
                partitioned = tools.merge_partitions(*reduced, sort=rj.sort)

        partitioned = _mrtools.strip_sort_key(partitioned)

        if self.sort_output:
            partitioned = self._output_sorter(partitioned)

        return self.output(partitioned)
