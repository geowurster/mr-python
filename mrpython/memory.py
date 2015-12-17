"""
In-memory MapReduce - for those weird use cases ...
"""


import six

import mrpython as mr
import mrpython.base


class MRSerial(mr.base.MRBase):

    """
    For MapReduce operations that don't benefit from parallelism.

    The `mapper()` and `reducer()` must yield 3 element tuples.  The first
    element is used for partitioning, the second sorting, and the third is
    data.
    """

    def __call__(self, stream):

        mapped = self._map(stream)

        with self._partition(mapped) as partitioned:
            sorted_data = self._sort(six.iteritems(partitioned))

        reduced = self._reduce(sorted_data)

        with self._partition(reduced) as partitioned:
            sorted_data = self._sort(six.iteritems(partitioned))

        return self.final_reducer(sorted_data)


class MRSerialNoSort(mr.base.MRBase):

    """
    For MapReduce operations that don't benefit from sorting or parallelism.

    The `mapper()` and `reducer()` must yield 2 element tuples.  The first
    element is used for partitioning and the second is data.
    """

    def __call__(self, stream):

        mapped = self._map(stream)

        with self._partition_no_sort(mapped) as partitioned:
            reduced = self._reduce(six.iteritems(partitioned))

        with self._partition_no_sort(reduced) as partitioned:
            return self.final_reducer(six.iteritems(partitioned))
