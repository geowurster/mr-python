"""
In-memory MapReduce - for those weird use cases ...
"""


from itertools import chain
import multiprocessing as mp

import six

import tinymr as mr
import tinymr.base
import tinymr.tools


# class MRParallelNoSort(mr.base.MRBase):
#
#     @property
#     def jobs(self):
#
#         """
#         Number of tasks to execute in parallel.
#         """
#
#         return 1
#
#     @property
#     def map_size(self):
#
#         """
#         Number of items from the input data stream to hand to each mapper.
#         """
#
#         return 1
#
#     def __call__(self, stream):
#
#         stream = mr.tools.slicer(stream, self.map_size)
#
#         combined = chain(
#             *mp.Pool(self.jobs).imap_unordered(self._map_partition_combine, stream))
#
#         with self._partition_no_sort(combined) as partitioned:
#             partitioned = tuple(six.iteritems(partitioned))
#
#         reduced = tuple(mp.Pool(self.jobs).imap_unordered(self._imap_reducer, partitioned))
#
#         with self._partition_no_sort(reduced) as partitioned:
#             return self.final_reducer(six.iteritems(partitioned))
#
#     def _imap_reducer(self, pair):
#
#         """
#         Adapter to integrate `reducer()` into the `imap_unordered()` API.
#         """
#
#         return tuple(self.reducer(*pair))


class MRSerial(mr.base.MRBase):

    """
    For MapReduce operations that don't benefit from sorting or parallelism.

    The `mapper()` and `reducer()` must yield 2 element tuples.  The first
    element is used for partitioning and the second is data.
    """

    def __call__(self, stream):

        with self._partition(self._map(stream)) as partitioned:

            sorted_data = self._sorter(six.iteritems(partitioned), fake=self.sort_map)

        with self._partition(self._reduce(sorted_data)) as partitioned:

            sorted_data = self._sorter(six.iteritems(partitioned), fake=self.sort_reduce)

            if self.sort_final_reduce:
                sorted_data = self._final_reducer_sorter(sorted_data)

            return self.final_reducer(sorted_data)
