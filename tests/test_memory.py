"""
Unittests for tinymr.memory
"""


import tinymr as mr
import tinymr.memory


# def test_MapReduce_no_sort(tiny_text, tiny_text_wc_output, mr_wordcount_memory_no_sort):
#
#     with mr_wordcount_memory_no_sort() as wc:
#         actual = wc(tiny_text.splitlines())
#     assert actual == tiny_text_wc_output


def test_MapReduce_init_reduce(tiny_text, tiny_text_wc_output, mr_wordcount_memory_no_sort):

    class WCInitReduce(mr_wordcount_memory_no_sort):

        def init_reduce(self):
            self.initialized = True

    with WCInitReduce() as wc:
        actual = wc(tiny_text.splitlines())


def test_MapReduce_sort():

    """
    Make sure enabling sorting actually sorts.
    """

    text = [
        'key2 sort2 data2',
        'key2 sort1 data1',
        'key3 sort2 data2',
        'key3 sort1 data1',
        'key1 sort2 data2',
        'key1 sort1 data1'
    ]

    class WordCount(mr.memory.MapReduce):

        # Make sure everything gets sent to a single map + combine
        chunksize = 10

        def mapper(self, item):
            yield item.split()

        def combiner(self, key, values):

            d1, d2 = list(values)
            assert [d1, d2] == ['data1', 'data2']

            yield key, 'sort2', d2
            yield key, 'sort1', d1

        def reducer(self, key, values):

            d1, d2 = list(values)
            assert [d1, d2] == ['data1', 'data2']

            yield key, 'sort2', d2
            yield key, 'sort1', d1

    wc = WordCount()

    for attr in ('jobs', 'map_jobs', 'sort_jobs', 'reduce_jobs'):
        assert getattr(wc, attr) == 1
    for attr in ('sort', 'sort_map', 'sort_combine', 'sort_reduce', 'sort_final_reduce'):
        assert getattr(wc, attr)
    for attr in ('chunksize', 'map_chunksize', 'reduce_chunksize', 'sort_chunksize'):
        assert getattr(wc, attr) == 10

    assert tuple(wc(text)) == (
        ('key1', ('data1', 'data2')),
        ('key2', ('data1', 'data2')),
        ('key3', ('data1', 'data2')))


def test_MapReduce_no_sort():

    """
    Make sure that disabling sorting actually disables sorting.
    """

    text = [
        'key2 sort2 data2',
        'key2 sort1 data1',
        'key3 sort2 data2',
        'key3 sort1 data1',
        'key1 sort2 data2',
        'key1 sort1 data1'
    ]

    class WordCount(mr.memory.MapReduce):

        # Make sure everything gets sent to a single map + combine
        chunksize = 10
        sort = False

        def mapper(self, item):
            yield item.split()

        def combiner(self, key, values):

            d2, d1 = list(values)
            assert [d2, d1] == ['data2', 'data1']

            yield key, 'sort2', d2
            yield key, 'sort1', d1

        def _final_reducer_sorter(self, kv_stream):
            raise Exception("Shouldn't hit this.")

        def reducer(self, key, values):

            d2, d1 = list(values)
            assert [d2, d1] == ['data2', 'data1']

            yield key, 'sort2', d2
            yield key, 'sort1', d1

    wc = WordCount()
    for attr in ('jobs', 'map_jobs', 'sort_jobs', 'reduce_jobs'):
        assert getattr(wc, attr) == 1
    for attr in ('sort', 'sort_map', 'sort_combine', 'sort_reduce', 'sort_final_reduce'):
        assert getattr(wc, attr) is False
    for attr in ('chunksize', 'map_chunksize', 'reduce_chunksize', 'sort_chunksize'):
        assert getattr(wc, attr) == 10

    # Can't really check key order here, so we're just going to
    assert dict(wc(text)) == {
        'key2': ('data2', 'data1'),
        'key3': ('data2', 'data1'),
        'key1': ('data2', 'data1')}


class _WCParallelSort(mr.memory.MapReduce):

    """
    Define out here so we can pickle it in multiprocessing
    """

    # Make sure everything gets sent to a single map + combine
    chunksize = 10
    jobs = 4

    def mapper(self, item):
        yield item.split()

    def combiner(self, key, values):

        d1, d2 = list(values)
        assert [d1, d2] == ['data1', 'data2']

        yield key, 'sort2', d2
        yield key, 'sort1', d1

    def reducer(self, key, values):

        d1, d2 = list(values)
        assert [d1, d2] == ['data1', 'data2']

        yield key, 'sort2', d2
        yield key, 'sort1', d1


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

    for attr in ('jobs', 'map_jobs', 'sort_jobs', 'reduce_jobs'):
        assert getattr(wc, attr) == 4
    for attr in ('sort', 'sort_map', 'sort_combine', 'sort_reduce', 'sort_final_reduce'):
        assert getattr(wc, attr)
    for attr in ('chunksize', 'map_chunksize', 'reduce_chunksize', 'sort_chunksize'):
        assert getattr(wc, attr) == 10

    assert tuple(wc(text)) == (
        ('key1', ('data1', 'data2')),
        ('key2', ('data1', 'data2')),
        ('key3', ('data1', 'data2')))
