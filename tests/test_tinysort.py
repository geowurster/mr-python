"""
Unittests for tinymr.tinysort
"""


import io
import os

import pytest

from tinymr._backport_heapq import merge as heapq_merge
from tinymr import serialize
from tinymr import tinysort


def test_delete_files(tmpdir):

    base = tmpdir.mkdir('test_delete_files')
    paths = [str(base.join(str(i))) for i in range(5)]

    for p in paths:
        assert not os.path.exists(p)
        with open(p, 'w') as f:
            pass
        assert os.path.exists(p)

    with tinysort.delete_files(*paths) as pths:
        for p in pths:
            assert os.path.exists(p)

    for p in pths:
        assert not os.path.exists(p)

    # Run it again to make sure we don't get an exception
    with tinysort.delete_files(*paths) as pths:
        pass


def test_batch_open(tmpdir):

    base = tmpdir.mkdir('test_delete_files')
    paths = [str(base.join(str(i))) for i in range(5)]

    for p in paths:
        assert not os.path.exists(p)

    with tinysort.batch_open(*paths, mode='w') as handles:
        # There is no io.IOBase in Python 2.
        types = (io.IOBase, globals()['__builtins__'].get('file', io.IOBase))
        for h in handles:
            assert isinstance(h, types)


def test_sort_into(tmpdir):

    path = str(tmpdir.mkdir('test_sort_into').join('data'))

    # We cheat a bit to get newline characters into the output file
    values = tuple((str(i) + os.linesep for i in range(10)))

    with open(path, 'w') as f:
        tinysort.sort_into(reversed(values), f)

    with open(path) as f:
        for e, a in zip(values, f):
            assert e == a


def test_sort_into_files():

    # Use an odd number so one chunk only has 1 value
    values = tuple(range(9))

    results = tinysort.sort_into_files(reversed(values), chunksize=2)
    assert len(results) == 5

    with tinysort.delete_files(*results) as paths:
        for p in paths:
            with serialize.Pickle().open(p) as f:
                lines = [int(l) for l in list(f)]

                # Input values are reversed, so the odd chunk is 0, not 9
                if len(lines) == 1:
                    assert lines[0] == 0
                elif len(lines) == 2:
                    assert lines[0] + 1 == lines[1]

                else:
                    raise ValueError("Unexpected condition")

@pytest.fixture(scope='function')
def unsorted_files(tmpdir):

    """
    Returns a tuple where the first element is a list of temporary files
    containing unsorted data the the second is the serializer needed to
    read them.

        ([p1, p2, ...], tinymr.serialize.Serializer())
    """

    base = tmpdir.mkdir('sort_files_into_files')

    values_210 = str(base.join('210'))
    values_543 = str(base.join('543'))
    values_765 = str(base.join('876'))
    values_98 = str(base.join('9'))
    paths = (values_210, values_543, values_765, values_98)
    slz = serialize.Pickle()

    for p in paths:
        nums = os.path.basename(p)
        with slz.open(p, 'w') as f:
            for n in nums:
                n = int(n)
                f.write((n, n))

    return paths, slz


def test_sort_files_into_files(unsorted_files):

    out, slz = unsorted_files

    result = tinysort.sort_files_into_files(
        *out,
        reader=slz,
        chunksize=2,
        key=lambda x: x[0])

    with tinysort.delete_files(*result) as opaths:

        with tinysort.batch_open(
                *opaths, opener=serialize.Pickle().open) as handles:

            assert len(opaths) == len(handles) == 7
            actual = list(heapq_merge(*handles, key=lambda x: x[0]))
            expected = [(i, i) for i in range(10)]
            assert actual == expected


def test_sort_files(unsorted_files):
    paths, slz = unsorted_files
    result = list(tinysort.sort_files(*paths, reader=slz, key=lambda x: x[0]))
    assert result == [(i, i) for i in range(10)]
