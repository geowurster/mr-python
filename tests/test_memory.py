"""Tests for ``tinymr.memory``."""


import tinymr as mr
import tinymr.memory


def test_MapReduce_init_reduce(
        tiny_text, tiny_text_wc_output, mr_wordcount_memory_no_sort):

    class WCInitReduce(mr_wordcount_memory_no_sort):

        def init_reduce(self):
            self.initialized = True

    with WCInitReduce() as wc:
        assert not hasattr(wc, 'initialized')
        actual = wc(tiny_text.splitlines())
        assert wc.initialized


def test_MapReduce_sort():

    """Make sure enabling sorting actually sorts."""

    text = [
        'key2 sort2 data2',
        'key2 sort1 data1',
        'key3 sort2 data2',
        'key3 sort1 data1',
        'key1 sort2 data2',
        'key1 sort1 data1'
    ]

    class WordCount(mr.memory.MemMapReduce):

        # Make sure everything gets sent to a single map + combine
        chunksize = 10

        def mapper(self, item):
            yield item.split()

        def reducer(self, key, values):

            d1, d2 = list(values)
            assert [d1, d2] == ['data1', 'data2']

            yield key, d2
            yield key, d1

    wc = WordCount()
    results = {k: sorted(v) for k, v in wc(text)}
    assert results == {
        'key1': ['data1', 'data2'],
        'key2': ['data1', 'data2'],
        'key3': ['data1', 'data2']}


def test_MapReduce_no_sort():

    """Make sure that disabling sorting actually disables sorting."""

    text = [
        'key2 sort2 data2',
        'key2 sort1 data1',
        'key3 sort2 data2',
        'key3 sort1 data1',
        'key1 sort2 data2',
        'key1 sort1 data1'
    ]

    class WordCount(mr.memory.MemMapReduce):

        # Make sure everything gets sent to a single map + combine
        chunksize = 10
        sort = False

        def mapper(self, item):
            yield item.split()

        def reducer(self, key, values):

            d2, d1 = list(values)
            assert [d2, d1] == ['data1', 'data2']

            yield key, d2
            yield key, d1

    wc = WordCount()
    results = {k: sorted(v) for k, v in wc(text)}
    assert results == {
        'key2': ['data1', 'data2'],
        'key3': ['data1', 'data2'],
        'key1': ['data1', 'data2']}


class _WCParallelSort(mr.memory.MemMapReduce):

    """Define out here so we can pickle it in multiprocessing."""

    # Make sure everything gets sent to a single map + combine
    chunksize = 10
    jobs = 4

    def mapper(self, item):
        yield item.split()

    def reducer(self, key, values):

        d1, d2 = list(values)
        assert [d1, d2] == ['data1', 'data2']

        yield key, d2
        yield key, d1


def test_MapReduce_parallel_sort():

    """
    Process in parallel with sorting.
    """

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
