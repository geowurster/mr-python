from collections import Counter
from multiprocessing import Pool as MPProcessPool
from multiprocessing.dummy import Pool as MPThreadPool
import operator as op
import sys

import pytest

from tinymr import MapReduce


class SerialPoolExecutor(object):

    def __init__(self, max_workers):
        pass

    def close(self):
        pass

    def map(self, func, sequence):
        return (func(s) for s in sequence)


if sys.version_info.major > 2:
    from functools import reduce
    from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
else:
    ProcessPoolExecutor = ThreadPoolExecutor = SerialPoolExecutor


POOLS = (
    None, SerialPoolExecutor,
    ProcessPoolExecutor, ThreadPoolExecutor,
    MPProcessPool, MPThreadPool)


class WordCountYieldYield(MapReduce):

    def mapper(self, item):
        line = item.lower().strip()
        for word in line.split():
            yield word, 1

    def reducer(self, key, values):
        yield key, sum(values)

    def output(self, items):
        return {k: next(iter(v)) for k, v in items.items()}


class WordCountYieldReturn(MapReduce):

    def mapper(self, item):
        line = item.lower().strip()
        for word in line.split():
            yield word, 1

    def reducer(self, key, values):
        return key, sum(values)


class WordCountReturnYield(WordCountYieldYield):

    def mapper(self, item):
        count = Counter(item.lower().strip().split())
        return 0, count

    def reducer(self, key, values):
        yield key, dict(reduce(op.iadd, values))

    def output(self, items):
        return items[0][0]


class WordCountReturnReturn(WordCountReturnYield):

    def reducer(self, key, values):
        return key, reduce(op.iadd, values)

    def output(self, items):
        return items[0]


@pytest.mark.parametrize("map_pool", POOLS)
@pytest.mark.parametrize("reduce_pool", POOLS)
@pytest.mark.parametrize("max_workers", (1, 2))
@pytest.mark.parametrize("wordcount", (
    WordCountYieldYield, WordCountYieldReturn,
    WordCountReturnReturn, WordCountReturnYield))
def test_mapreduce(
        text, text_word_count,
        wordcount, max_workers,
        map_pool, reduce_pool):

    """Tests combinations of several things:

        1. Mapper that yields.
        2. Mapper that returns.
        3. Reducer that yields.
        4. Reducer that returns.
        5. Custom ``output()`` method.
        6. Concurrent and serial map phase.
        7. Concurrent and serial reduce phase.
    """

    try:

        mapper_map = None
        if map_pool is not None:
            map_pool = map_pool(1)
            mapper_map = map_pool.map

        reducer_map = None
        if reduce_pool is not None:
            reduce_pool = reduce_pool(1)
            reducer_map = reduce_pool.map

        with wordcount() as wc:
            actual = wc(
                text.splitlines(),
                mapper_map=mapper_map,
                reducer_map=reducer_map)

    finally:
        getattr(map_pool, 'close', lambda: None)()
        getattr(reduce_pool, 'close', lambda: None)()

    assert actual == text_word_count
