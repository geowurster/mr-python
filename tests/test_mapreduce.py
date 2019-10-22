"""Tests for ``tinymemory``."""


import itertools as it
import random

import pytest

from tinymr import MapReduce
from tinymr.exceptions import KeyCountError


class _WordCount(MapReduce):

    """Define outside a function so other tests can subclass to test
    concurrency and parallelism.
    """

    def mapper(self, item):
        return zip(item.lower().split(), it.repeat(1))

    def reducer(self, key, values):
        return key, sum(values)


@pytest.fixture(scope='class')
def wordcount():
    return _WordCount


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

    class GroupSort(MapReduce):

        def mapper(self, item):
            yield item.split()

        def reducer(self, key, values):
            for v in values:
                yield key, v

    with GroupSort() as gs:
        results = gs(text)
    assert len(results) == 3
    assert results == {
        'key1': ['data1', 'data2'],
        'key2': ['data1', 'data2'],
        'key3': ['data1', 'data2']}


def test_serial_no_sort():

    """Make sure that disabling sorting actually disables sorting."""

    text = [
        '1 6',
        '1 5',
        '1 4',
        '1 3',
        '1 2',
        '1 1']

    class Grouper(MapReduce):

        def mapper(self, item):
            yield item.split()

        def reducer(self, key, values):
            for v in values:
                yield key, v

    with Grouper() as g:
        results = g(text)
    assert results == {'1': ['6', '5', '4', '3', '2', '1']}


class _WCParallelSort(MapReduce):
    """Define out here so we can pickle it in multiprocessing."""

    # Make sure everything gets sent to a single map + combine
    chunksize = 10
    jobs = 4

    def mapper(self, item):
        yield item.split()

    def reducer(self, key, values):
        for v in values:
            yield key, v


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

    with _WCParallelSort() as wc:
        results = wc(text)

    assert results == {
        'key1': ['data1', 'data2'],
        'key2': ['data1', 'data2'],
        'key3': ['data1', 'data2']}


def test_composite_partition_sort():
    """Composite key with sorting."""
    class GroupSort(MapReduce):

        def mapper(self, item):
            k1, k2, k3, k4, k5 = item
            yield (k1, k2), (k3, k4), k5

        def reducer(self, key, values):
            for v in values:
                yield key, v

    data = [
        ('p1', 'p2', 's1', 's2', 'd1'),
        ('p1', 'p2', 's3', 's4', 'd2'),
        ('p3', 'p4', 's1', 's2', 'd1'),
        ('p3', 'p4', 's3', 's4', 'd2'),
        ('p5', 'p6', 's1', 's2', 'd1'),
        ('p5', 'p6', 's3', 's4', 'd2')]
    random.shuffle(data)

    with GroupSort() as gs:
        results = gs(data)

    assert results == {
        ('p1', 'p2'): ['d1', 'd2'],
        ('p3', 'p4'): ['d1', 'd2'],
        ('p5', 'p6'): ['d1', 'd2'],
    }


def test_MemMapReduce_exceptions():

    class TooManyMapperKeys(MapReduce):

        def mapper(self, item):
            yield 1, 2, 3, 4

    with TooManyMapperKeys() as tmmk:
        with pytest.raises(KeyCountError):
            tmmk([1])

    class TooManyReducerKeys(MapReduce):

        def mapper(self, item):
            yield 1, 2

        def reducer(self, key, values):
            yield 1, 2, 3, 4

    with TooManyReducerKeys() as tmrk:
        with pytest.raises(KeyCountError):
            tmrk([1])


def test_context_manager(wordcount, tiny_text, tiny_text_wc_output):

    class WordCount(wordcount):

        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    with WordCount() as wc:
        assert not wc.closed
    assert wc.closed
