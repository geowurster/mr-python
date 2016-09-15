"""In-memory MapReduce.  Get weird."""


from collections import deque, defaultdict
import itertools as it
import operator as op

from tinymr import base
from tinymr import errors
from tinymr import tools


class MemMapReduce(base.BaseMapReduce):

    """In-memory MapReduce.  All data is held in memory."""

    def __call__(self, data):

        # Map phase
        mapped = it.chain.from_iterable(map(self.mapper, data))

        first = next(mapped)
        mapped = it.chain([first], mapped)

        n_expected_keys = self.n_partition_keys + self.n_sort_keys + 1
        if len(first) != n_expected_keys:
            raise errors.KeyCountError(
                "Expected mapper to generate {} keys but got {}: {}".format(
                    n_expected_keys, len(first), first))

        partitioned = defaultdict(deque)

        _grouper_args = [self._ptn_key_idx, -1]
        if self.n_sort_keys > 0:
            _grouper_args.insert(1, self._sort_key_idx)
        key_grouper = op.itemgetter(*_grouper_args)

        mapped = map(key_grouper, mapped)
        if self.n_sort_keys == 0:
            for k, v in mapped:
                partitioned[k].append(v)
            partitioned = partitioned.items()
        else:
            for k, *sv in mapped:
                partitioned[k].append(sv)
            partitioned = partitioned.items()
            partitioned = it.starmap(
                lambda k, v: (k, sorted(v, key=op.itemgetter(0))),
                partitioned)
            partitioned = it.starmap(
                lambda k, v: (k, tuple(map(op.itemgetter(1), v))),
                partitioned)
            if self.n_partition_keys > 1:
                partitioned = it.starmap(lambda k, v: (k[0], v), partitioned)

        # Reduce by key
        self.init_reduce()
        reduced = it.chain.from_iterable(it.starmap(self.reducer, partitioned))

        first = next(reduced)
        reduced = it.chain([first], reduced)

        if len(first) != 2:
            raise errors.KeyCountError(
                "Expected reducer to generate 2 keys but got {}:".format(
                    len(first)), first)

        # Partition by key
        partitioned = defaultdict(deque)
        for k, v in reduced:
            partitioned[k].append(v)

        return self.output(tools.popitems(partitioned))
