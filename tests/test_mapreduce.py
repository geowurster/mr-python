"""Tests for ``tinymemory``."""


import itertools as it
import random

import pytest

from tinymr import MapReduce
from tinymr.errors import KeyCountError, ClosedTaskError
from tinymr.tools import single_key_output


class _WordCount(MapReduce):

    """Define outside a function so other tests can subclass to test
    concurrency and parallelism.
    """

    def mapper(self, item):
        return zip(item.lower().split(), it.repeat(1))

    def reducer(self, key, values):
        yield key, sum(values)

    def output(self, items):
        return single_key_output(items)


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

    class Grouper(MapReduce):

        def mapper(self, item):
            yield item.split()

        def reducer(self, key, values):
            return zip(it.repeat(key), values)

    g = Grouper()
    results = {k: tuple(v) for k, v in g(text)}
    assert results == {'1': ('6', '5', '4', '3', '2', '1')}


class _WCParallelSort(MapReduce):
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
    class GroupSort(MapReduce):

        n_sort_keys = 2

        def mapper(self, item):
            # Use first two elements for partitioning
            item = list(item)
            k1 = item.pop(0)
            k2 = item.pop(0)
            yield [(k1, k2)] + item

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
        ('p1', 'p2'): ('d1', 'd2'),
        ('p3', 'p4'): ('d1', 'd2'),
        ('p5', 'p6'): ('d1', 'd2'),
    }


def test_MemMapReduce_exceptions():
    class TooManyMapperKeys(MapReduce):
        def mapper(self, item):
            yield 1, 2, 3

    tmmk = TooManyMapperKeys()
    with pytest.raises(KeyCountError):
        tmmk([1])

    class TooManyReducerKeys(MapReduce):
        def mapper(self, item):
            yield 1, 2
        def reducer(self, key, values):
            yield 1, 2, 3

    tmrk = TooManyReducerKeys()
    with pytest.raises(KeyCountError):
        tmrk([1])


def test_run_map_method(wordcount):
    """``tinymr.MapReduce._run_map()`` isn't always called."""
    wc = wordcount()
    expected = (
        ('key', 1),
        ('value', 1)
    )
    assert expected == wc._run_map('key value')


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


@pytest.mark.parametrize(
    'method_name', ['check_map_keys', 'check_reduce_keys'])
def test_MemMapReduce_check_keys(wordcount, tiny_text, method_name):

    """Tests both ``MR.check_map_keys()`` and ``MR.check_reduce_keys()``."""

    class WordCount(wordcount):

        def checker(self, keys):
            # Its possible something like a ValueError, KeyError, or subclass
            # of KeyError will be raised accidentally if something breaks in
            # the code around where this check actually happens, but a
            # NameError is much less likely, and even less likely to go
            # unnoticed.
            raise NameError(keys)

    wc = WordCount()
    setattr(wc, method_name, wc.checker)

    with pytest.raises(NameError):
        wc(tiny_text)


def test_closed(wordcount):

    """An instance of a task that has been closed should raise an exception
    if it is used twice.
    """

    wc = wordcount()
    assert not wc.closed
    wc.close()
    assert wc.closed
    with pytest.raises(ClosedTaskError):
        wc('')

    with wordcount() as wc:
        assert not wc.closed
    assert wc.closed
    with pytest.raises(ClosedTaskError):
        wc('')


def test_not_implemented_methods():

    mr = MapReduce()
    with pytest.raises(NotImplementedError):
        mr.mapper(None)
    with pytest.raises(NotImplementedError):
        mr.reducer(None, None)


def test_default_methods():

    mr = MapReduce()

    expected = [(i, tuple(range(i))) for i in range(1, 10)]
    assert list(mr.output(expected)) == expected

    assert mr._sort_key_idx is None

    with pytest.raises(NotImplementedError):
        mr([None])


def test_set_properties():

    """All of the MapReduce level configuration happens in properties, but
    since the entire ``MapReduce.__init__()`` is handed over to the user,
    each of these properties needs to have a getter _and_ setter.
    """

    # Collect all the public properties
    props = []
    for attr in dir(MapReduce):
        obj = getattr(MapReduce, attr)
        if not attr.startswith('_') and isinstance(obj, property):
            props.append(attr)

    mr = MapReduce()
    for p in props:

        # Make sure the attribute isn't already set for some reason but
        # cache the value so it can be replaced once this property is tested
        original = getattr(mr, p)
        assert original != 'whatever', \
            "Property '{}' has already been set?".format(p)

        # This calls the setter
        try:
            setattr(mr, p, 'whatever')

        # The default error message isn't very helpful for debugging tests.
        except AttributeError:
            raise AttributeError(
                "Can't set property '{}' on '{}'".format(
                    p, mr.__class__.__name__))

        assert getattr(mr, p) == 'whatever'

        # Some properties default to other properties, for instance 'map_jobs'
        # and 'reduce_jobs' both default to 'jobs'.  If 'jobs' is tested
        # first then 'map_jobs' and 'reduce_jobs' will inherit its new
        # value.  By explicitly resetting the property's value to the
        # original state this test is a little more sensitive to human
        # errors, like if 'jobs.setter' actually points to 'chunksize'.
        setattr(mr, p, original)
