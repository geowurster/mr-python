"""
Unittests for mrpython.tools
"""


import pickle
from multiprocessing.pool import IMapUnorderedIterator
from types import GeneratorType

import pytest
import six

from mrpython import tools


def test_slicer_even():
    it = tools.slicer(six.moves.xrange(100), 10)
    for idx, actual in enumerate(it):

        assert isinstance(actual, tuple)
        assert len(actual) == 10

        # Verify that the values are correct
        assert actual == tuple((10 * idx) + i for i in range(len(actual)))

    assert idx == 9


def test_slicer_odd():

    it = tools.slicer(range(5), 2)
    assert next(it) == (0, 1)
    assert next(it) == (2, 3)
    assert next(it) == (4, )
    with pytest.raises(StopIteration):
        next(it)


def _func(v):

    """
    Can't pickle local functions.
    """

    return v + 1


def test_runner():

    input = list(range(10))
    expected = tuple(i + 1 for i in input)

    j1 = tools.runner(_func, input, 1)
    assert isinstance(j1, GeneratorType)
    assert tuple(j1) == expected

    j2 = tools.runner(_func, input, 2)
    assert isinstance(j2, IMapUnorderedIterator)
    assert tuple(sorted(j2)) == expected

    with pytest.raises(ValueError):
        tools.runner(None, None, -1)


class TestDefaultUnorderedDict:

    def setup_method(self, method):
        self.d = tools.DefaultOrderedDict(list)

    def test_repr(self):
        assert self.d.__class__.__name__ in repr(self.d)

    def test_present_key(self):
        self.d['key'] = 'value'
        assert self.d['key'] == 'value'

    def test_missing_key(self):
        assert self.d['missing'] == []

    def test_bool_true(self):
        self.d[None] = 'word'
        assert self.d

    def test_bool_false(self):
        assert not self.d

    def test_not_present(self):
        d = tools

    def test_exceptions(self):
        with pytest.raises(TypeError):
            tools.DefaultOrderedDict(None)

    def test_copy(self):
        self.d['key1'] = 'v1'
        self.d['key2'] = 'v2'
        c = self.d.copy()
        assert isinstance(c, tools.DefaultOrderedDict)
        assert self.d['key1'] == 'v1'
        assert self.d['key2'] == 'v2'
        self.d['key1'] = None
        assert c['key1'] == 'v1'
        assert len(c) == 2
        assert list(c.keys()) == ['key1', 'key2']
        assert list(c.values()) == ['v1', 'v2']
        assert c.default_factory is list

    def test_sorted_keys(self):

        """
        Verify that keys maintain their insert position.
        """

        # Set values
        it = list(range(10))
        for i in it:
            self.d[i] = i + 1

        # Check values
        for k, v in self.d.items():
            assert k + 1 == v

        # Check sorting
        assert list(self.d.keys()) == it
        assert sorted(self.d.keys()) == it

    def test_unsorted_keys(self):

        """
        Verify that unsorted keys remain the the same unsorted order.
        """

        for i in range(5):
            self.d[i] = i + 1
        for i in reversed(range(30, 35)):
            self.d[i] = i + 1

        assert list(self.d.keys()) == [0, 1, 2, 3, 4, 34, 33, 32, 31, 30]
        assert len(self.d.keys()) == 10


def test_mapkey():

    actual = tools.mapkey('key', range(5))
    expected = [('key', 0), ('key', 1), ('key', 2), ('key', 3), ('key', 4)]

    assert not isinstance(actual, (list, tuple))  # Make sure we get an iterator
    assert list(actual) == expected
