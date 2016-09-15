"""Tests for ``tinymr.base``."""


import pytest

from tinymr import base


def test_not_implemented_methods():

    mr = base.BaseMapReduce()
    with pytest.raises(NotImplementedError):
        mr.mapper(None)
    with pytest.raises(NotImplementedError):
        mr.reducer(None, None)


def test_default_methods():

    mr = base.BaseMapReduce()

    expected = [(i, tuple(range(i))) for i in range(1, 10)]
    assert list(mr.output(expected)) == expected

    assert mr._sort_key_idx is None

    with pytest.raises(NotImplementedError):
        mr(None)
