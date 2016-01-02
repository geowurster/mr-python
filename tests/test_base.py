"""
Unittests for tinymr.base
"""


import pytest

from tinymr import base


def test_not_implemented_methods():

    mr = base.MRBase()
    with pytest.raises(NotImplementedError):
        mr.mapper(None)
    with pytest.raises(NotImplementedError):
        mr.combiner(None, None)
    with pytest.raises(NotImplementedError):
        mr.reducer(None, None)


def test_default_settings():

    mr = base.MRBase()
    assert mr.sort_map
    assert mr.sort_combine
    assert mr.sort_reduce
    assert mr.sort_final_reduce


def test_default_methods():

    mr = base.MRBase()
    expected = [(i, tuple(range(i))) for i in range(1, 10)]
    assert list(mr.final_reducer(expected)) == expected
