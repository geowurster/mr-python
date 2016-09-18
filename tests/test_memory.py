"""Tests for ``tinymemory``."""


import itertools as it
from multiprocessing.pool import ThreadPool
import random

from tinymr import errors
from tinymr import memory
from tinymr import tools

import pytest


class _WordCount(memory.MemMapReduce):

    """Define outside a function so other tests can subclass to test
    concurrency and parallelism.
    """

    def mapper(self, item):
        return zip(item.lower().split(), it.repeat(1))

    def reducer(self, key, values):
        yield key, sum(values)

    def output(self, items):
        return tools.single_key_output(items)


@pytest.fixture(scope='module')
def wordcount():
    return _WordCount


def test_init_reduce(tiny_text, wordcount):

    class InitReduce(wordcount):
        def __init__(self):
            self.initialized_reduce = False
        def init_reduce(self):
            self.initialized_reduce = True

    wc = InitReduce()
    assert not wc.initialized_reduce
    tuple(wc(tiny_text.splitlines()))
    assert wc.initialized_reduce


def test_serial_sort():

    """Make sure enabling sorting actually sorts."""

    text = [
        'key2 sort2 data2',
        'key2 sort1 data1',
        'key3 sort2 data2',
        'key3 sort1 data1',
        'key1 sort2 data2',
        'key1 sort1 data1'
    ]

    class GroupSort(memory.MemMapReduce):

        n_sort_keys = 1

        def mapper(self, item):
            yield item.split()

        def reducer(self, key, values):
            return zip(it.repeat(key), values)

    gs = GroupSort()
    results = {k: tuple(v) for k, v in gs(text)}
    assert len(results) == 3
    assert results == {
        'key1': ('data1', 'data2'),
        'key2': ('data1', 'data2'),
        'key3': ('data1', 'data2')}


def test_serial_no_sort():

    """Make sure that disabling sorting actually disables sorting."""

    text = [
        '1 6',
        '1 5',
        '1 4',
        '1 3',
        '1 2',
        '1 1']

    class Grouper(memory.MemMapReduce):
        
        def mapper(self, item):
            yield item.split()
        
        def reducer(self, key, values):
            return zip(it.repeat(key), values)

    g = Grouper()
    results = {k: tuple(v) for k, v in g(text)}
    assert results == {'1': ('6', '5', '4', '3', '2', '1')}


class _WCParallelSort(memory.MemMapReduce):
    """Define out here so we can pickle it in multiprocessing."""

    # Make sure everything gets sent to a single map + combine
    chunksize = 10
    jobs = 4

    n_sort_keys = 1

    def mapper(self, item):
        yield item.split()

    def reducer(self, key, values):
        return zip(it.repeat(key), values)


def test_parallel_sort():

    """Process in parallel with sorting."""

    text = [
        'key2 sort2 data2',
        'key2 sort1 data1',
        'key3 sort2 data2',
        'key3 sort1 data1',
        'key1 sort2 data2',
        'key1 sort1 data1'
    ]

    wc = _WCParallelSort()
    results = {k: sorted(v) for k, v in wc(text)}
    assert results == {
        'key1': ['data1', 'data2'],
        'key2': ['data1', 'data2'],
        'key3': ['data1', 'data2']}


def test_composite_partition_sort():
    """Composite key with sorting."""
    class GroupSort(memory.MemMapReduce):

        n_partition_keys = 2
        n_sort_keys = 2

        def mapper(self, item):
            yield item

        def reducer(self, key, values):
            return zip(it.repeat(key), values)

    data = [
        ('p1', 'p2', 's1', 's2', 'd1'),
        ('p1', 'p2', 's3', 's4', 'd2'),
        ('p3', 'p4', 's1', 's2', 'd1'),
        ('p3', 'p4', 's3', 's4', 'd2'),
        ('p5', 'p6', 's1', 's2', 'd1'),
        ('p5', 'p6', 's3', 's4', 'd2')]
    random.shuffle(data)

    gs = GroupSort()
    results = {k: tuple(v) for k, v in gs(data)}
    assert results == {
        'p1': ('d1', 'd2'),
        'p3': ('d1', 'd2'),
        'p5': ('d1', 'd2'),
    }


def test_MemMapReduce_exceptions():
    class TooManyMapperKeys(memory.MemMapReduce):
        def mapper(self, item):
            yield 1, 2, 3

    tmmk = TooManyMapperKeys()
    with pytest.raises(errors.KeyCountError):
        tmmk([1])

    class TooManyReducerKeys(memory.MemMapReduce):
        def mapper(self, item):
            yield 1, 2
        def reducer(self, key, values):
            yield 1, 2, 3

    tmrk = TooManyReducerKeys()
    with pytest.raises(errors.KeyCountError):
        tmrk([1])


def test_run_map_method(wordcount):
    """``tinymr.memory.MemMapReduce._run_map()`` isn't always called."""
    wc = wordcount()
    expected = (
        ('key', 1),
        ('value', 1)
    )
    assert expected == wc._run_map('key value')


class _WCThreaded(_WordCount):
    threaded = True


def test_threaded(tiny_text, tiny_text_wc_output):
    wc = _WCThreaded()
    assert isinstance(wc._map_job_pool, ThreadPool)
    assert isinstance(wc._reduce_job_pool, ThreadPool)
    assert dict(wc(tiny_text.splitlines())) == dict(tiny_text_wc_output)


def test_context_manager(wordcount, tiny_text, tiny_text_wc_output):

    """Test context manager and ensure default implementation exists."""

    class WordCount(wordcount):

        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    with WordCount() as wc:
        assert not wc.closed
        assert dict(tiny_text_wc_output) == dict(wc(tiny_text.splitlines()))
    assert wc.closed


def test_MemMapReduce_properties(wordcount):

    wc = wordcount()
    assert wc.chunksize == 1
    assert not wc.threaded
    assert wc.close() is None

    class WordCount(wordcount):
        chunksize = 2

    wc = WordCount()
    assert wc.chunksize == 2
    assert wc.map_chunksize == 2
    assert wc.reduce_chunksize == 2
