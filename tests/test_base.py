"""
Unittests for tinymr.base
"""


import pytest

from tinymr import _mrtools
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
    assert mr.sort_output


def test_default_methods():

    mr = base.BaseMapReduce()
    expected = [(i, tuple(range(i))) for i in range(1, 10)]
    assert list(mr.output(expected)) == expected

# TODO: Reimplement
# def test_reduce_job_confs():
#     # Make sure attributes are coming from the correct location
#     class MR(base.BaseMapReduce):
#
#         jobs = 4
#         reduce2_chunksize = 10
#         reduce10_jobs = 2
#         sort = False
#         sort_reduce2 = True
#
#         # Define out of order to test sorting
#         def reducer10(self, key, values):
#             pass
#
#         def reducer(self, key, values):
#             pass
#
#         def reducer2(self, key, values):
#             pass
#
#     mr = MR()
#
#     rj = _mrtools.ReduceJobConf(
#         reducer=mr.reducer,
#         sort=False,
#         jobs=4,
#         chunksize=1)
#     rj2 = _mrtools.ReduceJobConf(
#         reducer=mr.reducer2,
#         sort=True,
#         jobs=4,
#         chunksize=10)
#     rj10 = _mrtools.ReduceJobConf(
#         reducer=mr.reducer10,
#         sort=False,
#         jobs=2,
#         chunksize=1)
#
#     assert mr._reduce_job_confs == [rj, rj2, rj10]


# def test_context_manager():
#
#     class MapReduce(base.BaseMapReduce):
#
#         def __init__(self):
#             self._closed = False
#
#         def close(self):
#             self._closed = True
#
#     with MapReduce() as mr:
#         assert not mr.closed
#     assert mr.closed


# def test_no_context_manager():
#
#     class MapReduce(base.BaseMapReduce):
#
#         def close(self):
#             self._closed = True
#
#     mr = MapReduce()
#     assert not mr.closed
#     mr.close()
#     assert mr.closed
#     assert not MapReduce._closed
#     assert not MapReduce().closed


# def test_cannot_reuse_tasks():
#
#     class MapReduce(base.BaseMapReduce):
#         pass
#
#     with MapReduce() as mr:
#         pass
#
#     # assert mr.closed
#     with pytest.raises(IOError):
#         with mr as c:
#             pass


# def test_runtime_validate():
#
#     class MapReduce(base.BaseMapReduce):
#         closed = True
#
#     with pytest.raises(errors.ClosedTask):
#         MapReduce()._runtime_validate()
