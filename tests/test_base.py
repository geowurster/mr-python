"""
Unittests for tinymr.base
"""


import pytest

from tinymr import base
from tinymr import errors


def test_not_implemented_methods():

    mr = base.MRBase()
    with pytest.raises(NotImplementedError):
        mr.mapper(None)
    with pytest.raises(errors.CombinerNotImplemented):
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


def test_context_manager():

    class MR(base.MRBase):

        def __init__(self):
            self._closed = False

        def close(self):
            self._closed = True

    with MR() as mr:
        assert not mr.closed
    assert mr.closed


def test_no_context_manager():

    class MR(base.MRBase):

        def close(self):
            self._closed = True

    mr = MR()
    assert not mr.closed
    mr.close()
    assert mr.closed
    assert not MR._closed
    assert not MR().closed


def test_cant_reuse_tasks():

    class MR(base.MRBase):
        pass

    with MR() as mr:
        pass

    assert mr.closed
    with pytest.raises(IOError):
        with mr as c:
            pass
