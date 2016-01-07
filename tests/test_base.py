"""
Unittests for tinymr.base
"""


import pytest

from tinymr import base
from tinymr import errors


def test_not_implemented_methods():

    mr = base.BaseMapReduce()
    with pytest.raises(NotImplementedError):
        mr.mapper(None)
    with pytest.raises(errors.CombinerNotImplemented):
        mr.combiner(None, None)
    with pytest.raises(NotImplementedError):
        mr.reducer(None, None)


def test_default_settings():

    mr = base.BaseMapReduce()
    assert mr.sort_map
    assert mr.sort_combine
    assert mr.sort_reduce
    assert mr.sort_final_reduce


def test_default_methods():

    mr = base.BaseMapReduce()
    expected = [(i, tuple(range(i))) for i in range(1, 10)]
    assert list(mr.final_reducer(expected)) == expected


def test_context_manager():

    class MapReduce(base.BaseMapReduce):

        def __init__(self):
            self._closed = False

        def close(self):
            self._closed = True

    with MapReduce() as mr:
        assert not mr.closed
    assert mr.closed


def test_no_context_manager():

    class MapReduce(base.BaseMapReduce):

        def close(self):
            self._closed = True

    mr = MapReduce()
    assert not mr.closed
    mr.close()
    assert mr.closed
    assert not MapReduce._closed
    assert not MapReduce().closed


def test_cant_reuse_tasks():

    class MapReduce(base.BaseMapReduce):
        pass

    with MapReduce() as mr:
        pass

    assert mr.closed
    with pytest.raises(IOError):
        with mr as c:
            pass


def test_runtime_validate():

    class MapReduce(base.BaseMapReduce):
        closed = True

    with pytest.raises(errors.ClosedTask):
        MapReduce()._runtime_validate()
