"""
In-memory MapReduce - for those weird use cases ...
"""


from collections import deque, defaultdict
import itertools as it
import operator as op

from tinymr import base
from tinymr import tools


class MemMapReduce(base.BaseMapReduce):

    """In-memory MapReduce.  All data is held in memory."""

    def __call__(self, data):

        # Map phase
        mapped = it.chain.from_iterable(map(self.mapper, data))

        first = next(mapped)
        mapped = it.chain([first], mapped)

        partitioned = defaultdict(deque)

        # If we only get 2 keys then there aren't any sort keys
        # There is a *significant* penalty for sorting unecessarily
        if len(first) == 2:
            for k, v in mapped:
                partitioned[k].append(v)
            partitioned = partitioned.items()
        else:
            for k, *v, data in mapped:
                partitioned[k].append((v, data))
            partitioned = partitioned.items()
            partitioned = (
                (k, sorted(v, key=op.itemgetter(0))) for k, v in partitioned)
            partitioned = (
                (k, tuple(map(op.itemgetter(-1), v))) for k, v in partitioned)

        # Reduce by key
        self.init_reduce()
        reduced = it.chain.from_iterable(it.starmap(self.reducer, partitioned))

        # Partition by key
        partitioned = defaultdict(deque)
        for k, v in reduced:
            partitioned[k].append(v)

        return self.output(tools.popitems(partitioned))
