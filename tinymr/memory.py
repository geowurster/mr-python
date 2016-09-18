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

    """In-memory MapReduce.  All data is held in memory."""

    def _run_map(self, item):
        return tuple(self.mapper(item))

    def _run_reduce(self, kv):
        key, values = kv
        if self.n_sort_keys != 0:
            values = sorted(values, key=op.itemgetter(0))
            values = _compat.map(op.itemgetter(1), values)
        return tuple(self.reducer(key, values))

    def __call__(self, stream):

        if self.map_jobs == 1:
            # We skip some function calls if we bypass _run_map()
            # For tasks like word count this can matter a lot
            results = _compat.map(self.mapper, stream)
        else:
            results = self._map_job_pool.imap_unordered(
                self._run_map,
                stream,
                self.map_chunksize)
        results = it.chain.from_iterable(results)

        # It's very difficult to debug multiprocessing operations so we
        # explicitly check the first set of keys and issue a much more
        # useful error if the key count is wrong.
        first = next(results)
        results = it.chain([first], results)
        expected_key_count = self.n_partition_keys + self.n_sort_keys + 1
        if len(first) != expected_key_count:
            raise errors.KeyCountError(
                "Expected {expected} keys from the map phase, not {actual} - "
                "first keys: {keys}".format(
                    expected=expected_key_count,
                    actual=len(first),
                    keys=first))

        partitioned = defaultdict(deque)
        mapped = _compat.map(self._map_key_grouper, results)
        if self.n_sort_keys == 0:
            for ptn, val in mapped:
                partitioned[ptn].append(val)
            partitioned_items = partitioned.items()
        else:
            for ptn, srt, val in mapped:
                partitioned[ptn].append((srt, val))
            if self.n_partition_keys > 1:
                partitioned_items = it.starmap(
                    lambda ptn, srt_val: (ptn[0], srt_val),
                    partitioned.items())
            else:
                partitioned_items = partitioned.items()

        self.init_reduce()
        if self.reduce_jobs == 1:
            results = _compat.map(self._run_reduce, partitioned_items)
        else:
            results = self._reduce_job_pool.imap_unordered(
                self._run_reduce, partitioned_items, self.reduce_chunksize)
        results = it.chain.from_iterable(results)

        # Same as with the map phase, we issue a more useful error
        first = next(results)
        results = it.chain([first], results)
        if len(first) != 2:
            raise errors.KeyCountError(
                "Expected 2 keys from the reduce phase, not {} - first "
                "keys: {}".format(len(first), first))

        partitioned = defaultdict(deque)
        for k, v in results:
            partitioned[k].append(v)

        return self.output(tools.popitems(partitioned))


# Required to make ``MemMapReduce._run_map()`` pickleable.
if sys.version_info.major == 2:  # pragma: no cover

    import copy_reg

    def _reduce_method(m):
        if m.im_self is None:
            return getattr, (m.im_class, m.im_func.func_name)
        else:
            return getattr, (m.im_self, m.im_func.func_name)

    copy_reg.pickle(type(MemMapReduce._run_map), _reduce_method)
