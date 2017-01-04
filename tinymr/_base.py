"""All the MapReduce magic."""


from collections import defaultdict, deque
import itertools as it
from multiprocessing.dummy import Pool as DummyPool
from multiprocessing.pool import Pool
import operator as op

from tinymr import _compat
from tinymr.errors import ClosedTaskError, KeyCountError
from tinymr.tools import popitems


class _MRInternal(object):

    """A lot of the helper methods on ``tinymr.MapReduce()`` are not relevant
    to subclassers so they are hidden away.

    This class cannot be directly subclassed.  Use
    ``tinymr.mapreduce.MapReduce()``.
    """

    def _run_map(self, item):
        return tuple(self.mapper(item))

    def _run_reduce(self, kv):
        key, values = kv
        if self.n_sort_keys != 0:
            values = sorted(values, key=op.itemgetter(0))
            values = _compat.map(op.itemgetter(1), values)
        return tuple(self.reducer(key, values))

    @property
    def _ptn_key_idx(self):
        if self.n_partition_keys == 1:
            return 0
        else:
            return slice(0, self.n_partition_keys)

    @property
    def _sort_key_idx(self):
        # Ensure a lack of sort keys is properly handled down the line by
        # letting something fail spectacularly
        if self.n_sort_keys == 0:
            return None
        elif self.n_sort_keys == 1:
            # Given keys like: ('partition', 'sort', 'data')
            # the number of partition keys equals the index of the single
            # sort key
            return self.n_partition_keys
        else:
            start = self.n_partition_keys
            stop = start + self.n_sort_keys
            return slice(start, stop)

    @property
    def _map_key_grouper(self):
        getter_args = [self._ptn_key_idx, -1]
        if self.n_sort_keys > 0:
            getter_args.insert(1, self._sort_key_idx)
        return op.itemgetter(*getter_args)

    @property
    def _map_job_pool(self):
        if self.threaded_map:
            return DummyPool(self.map_jobs)
        else:
            return Pool(self.map_jobs)

    @property
    def _reduce_job_pool(self):
        if self.threaded_reduce:
            return DummyPool(self.reduce_jobs)
        else:
            return Pool(self.reduce_jobs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __call__(self, stream):

        if self.closed:
            raise ClosedTaskError("Task is closed.")

        self.init_map()

        if self.map_jobs == 1:
            # Avoid the overhead (and debugging complexities) of
            # parallelized jobs
            results = _compat.map(self.mapper, stream)
        else:
            results = self._map_job_pool.imap_unordered(
                self._run_map,
                stream,
                self.map_chunksize)
        results = it.chain.from_iterable(results)

        # Parallelized jobs can be difficult to debug so the first set of
        # keys get a sniff check for some obvious potential problems.
        # Exceptions here prevent issues with multiprocessing getting confused
        # when a job fails.
        first = next(results)
        results = it.chain([first], results)
        expected_key_count = self.n_partition_keys + self.n_sort_keys + 1
        if len(first) != expected_key_count:
            raise KeyCountError(
                "Expected {expected} keys from the map phase, not {actual} - "
                "first keys: {keys}".format(
                    expected=expected_key_count,
                    actual=len(first),
                    keys=first))
        self.check_map_keys(first)

        partitioned = defaultdict(deque)
        mapped = _compat.map(self._map_key_grouper, results)

        # Only sort when required
        if self.n_sort_keys == 0:
            for ptn, val in mapped:
                partitioned[ptn].append(val)
            partitioned_items = partitioned.items()
        else:
            for ptn, srt, val in mapped:
                partitioned[ptn].append((srt, val))
            if self.n_partition_keys > 1:
                partitioned_items = it.starmap(
                    lambda _ptn, srt_val: (_ptn[0], srt_val),
                    partitioned.items())
            else:
                partitioned_items = partitioned.items()

        # Reduce phase
        self.init_reduce()
        if self.reduce_jobs == 1:
            results = _compat.map(self._run_reduce, partitioned_items)
        else:
            results = self._reduce_job_pool.imap_unordered(
                self._run_reduce, partitioned_items, self.reduce_chunksize)
        results = it.chain.from_iterable(results)

        # Same as with the map phase, issue a more useful error
        first = next(results)
        results = it.chain([first], results)
        if len(first) != 2:
            raise KeyCountError(
                "Expected 2 keys from the reduce phase, not {} - first "
                "keys: {}".format(len(first), first))
        self.check_reduce_keys(first)

        partitioned = defaultdict(deque)
        for k, v in results:
            partitioned[k].append(v)

        return self.output(popitems(partitioned))
