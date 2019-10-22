"""Unittests for ``tinymr.tools``."""


import pytest

from tinymr import tools


def test_slicer_even():
    it = tools.slicer(range(100), 10)
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


def test_popitems():

    d = {k: str(k) for k in range(10)}

    for k, v in tools.popitems(d):
        assert k < 10
        assert v == str(k)
    assert not d


def test_poplist():
    l = list(range(10))
    for v in tools.poplist(l):
        assert v < 10
        assert v not in l
    assert not l


def test_single_key_output():

    data = {
        'key1': ('v1',),
        'key2': ('v2',),
        'key3': ('v3',)
    }
    expected = {k: next(iter(v)) for k, v in data.items()}
    assert dict(tools.single_key_output(data.items())) == expected
