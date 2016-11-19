"""In-memory MapReduce.  Get weird."""


from collections import deque, defaultdict
import itertools as it
import operator as op
import sys

from tinymr import base
from tinymr import errors
from tinymr import tools
from tinymr import _compat


class MemMapReduce(base.BaseMapReduce):

    """In-memory MapReduce."""

    def _run_map(self, item):
        """Run ``mapper()`` in a parallel context.  Ensures the iterator that
        is likely produced by ``mapper()`` is expanded to a ``tuple()`` so it
        can be pickled.
        """
        return tuple(self.mapper(item))

    def _partition_sort_mapped(self, mapped):

        """Partition and sort data from the map phase.  Placing this in its
        own method lets the entire MapReduce task be a generator.

        Parameters
        ----------
        mapped : iter
            Data from the map phase like ``(partition, sort, data)``.

        Yields
        ------
        tuple
            ``(key, (value, value, ...)``
        """

        partitioned = defaultdict(deque)
        grouped = _compat.map(self._map_key_grouper, mapped)

        # Only sort when required
        if self.n_sort_keys == 0:
            for ptn, val in grouped:
                partitioned[ptn].append(val)
            partitioned_items = tools.popitems(partitioned)
        else:
            for ptn, srt, val in grouped:
                partitioned[ptn].append((srt, val))
            if self.n_partition_keys > 1:
                partitioned_items = it.starmap(
                    lambda _ptn, srt_val: (_ptn[0], srt_val),
                    partitioned.items())
            else:
                partitioned_items = tools.popitems(partitioned)

        for i in partitioned_items:
            yield i

    def _run_reduce(self, kv):
        """Run ``reducer()`` in a parallel context."""
        key, values = kv
        if self.n_sort_keys != 0:
            values = sorted(values, key=op.itemgetter(0))
            values = _compat.map(op.itemgetter(1), values)
        return tuple(self.reducer(key, values))

    def _partition_reduced(self, reduced):

        """Partition data from ``reducere()``.  Placing this in its own method
        lets the entire MapReduce task operate as a generator.

        Parameters
        ----------
        reduced : iter
            ``(key, value)``.
        """

        partitioned = defaultdict(deque)
        for k, v in reduced:
            partitioned[k].append(v)

        for i in tools.popitems(partitioned):
            yield i

    def _check_keys(
            self, checker, expected_key_count, phase, stream):

        first = next(stream)
        stream = it.chain([first], stream)
        if len(first) != expected_key_count:
            raise errors.KeyCountError(
                "Expected {expected} keys from the {phase} phase, not "
                "{actual} - first keys: {keys}".format(
                    phase=phase,
                    expected=expected_key_count,
                    actual=len(first),
                    keys=first))
        checker(first)
        for i in stream:
            yield i

    def __call__(self, stream):

        """Run the MapReduce task.

        Parameters
        ----------
        stream : iter
        """

        self.init_map()

        if self.map_jobs == 1:
            # Avoid the overhead (and debugging complexities) of
            # parallelized jobs
            mapped = _compat.map(self.mapper, stream)
        else:
            mapped = self._map_job_pool.imap_unordered(
                self._run_map,
                stream,
                self.map_chunksize)
        mapped = it.chain.from_iterable(mapped)

        # Parallelized jobs can be difficult to debug so the first set of
        # keys get a sniff check for some obvious potential problems.
        # Exceptions here prevent issues with multiprocessing getting confused
        # when a job fails.
        mapped = self._check_keys(
            checker=self.check_map_keys,
            expected_key_count=self.n_partition_keys + self.n_sort_keys + 1,
            phase='map',
            stream=mapped)

        # Partition and sort data from the map phase
        partitioned_items = self._partition_sort_mapped(mapped)

        # Reduce phase
        self.init_reduce()
        if self.reduce_jobs == 1:
            reduced = _compat.map(self._run_reduce, partitioned_items)
        else:
            reduced = self._reduce_job_pool.imap_unordered(
                self._run_reduce, partitioned_items, self.reduce_chunksize)
        reduced = it.chain.from_iterable(reduced)

        # Same as with the map phase, issue a more useful error
        reduced = self._check_keys(
            checker=self.check_reduce_keys,
            expected_key_count=2,
            phase='reduce',
            stream=reduced)

        partitioned_items = self._partition_reduced(reduced)

        return self.output(partitioned_items)


# Required to make ``MemMapReduce._run_map()`` pickleable.
if sys.version_info.major == 2:  # pragma: no cover

    import copy_reg

    def _reduce_method(m):
        if m.im_self is None:
            return getattr, (m.im_class, m.im_func.func_name)
        else:
            return getattr, (m.im_self, m.im_func.func_name)

    copy_reg.pickle(type(MemMapReduce._run_map), _reduce_method)
