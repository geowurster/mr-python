"""
Unittests for tinymr.tools
"""


from collections import defaultdict
from multiprocessing.pool import IMapUnorderedIterator
import os
from types import GeneratorType

import pytest
import six

from tinymr import errors
from tinymr import tools


def _icount_lines(path, minimum=1):

    """
    Count lines by opening the file and iterating over the file.
    """

    count = 0
    with open(path) as f:
        for l in f:
            count += 1
    assert count >= minimum
    return count


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


def test_runner_1job():

    input = list(range(10))
    expected = tuple(i + 1 for i in input)

    j1 = tools.runner(_func, input, 1)
    assert isinstance(j1, tools.runner)
    assert isinstance(iter(j1), GeneratorType)
    assert tuple(j1) == expected


def test_runner_2job():

    input = list(range(10))
    expected = tuple(i + 1 for i in input)

    # Also tests context manager
    with tools.runner(_func, input, 2) as j2:
        assert not j2._closed
        assert isinstance(j2, tools.runner)
        assert isinstance(iter(j2), IMapUnorderedIterator)
        assert tuple(sorted(j2)) == expected
    assert j2._closed


def test_runner_next():

    input = list(range(10))
    expected = list(i + 1 for i in input)

    r = tools.runner(_func, input, 1)
    assert next(r) == _func(input[0])

    # Multiple jobs - have to pretty much run the whole thing and sort to compare
    results = []
    with tools.runner(_func, input, 2) as proc:
        for i in input:
            results.append(next(proc))

    assert sorted(results) == expected


def test_runner_attrs_and_exceptions():

    # repr
    r = tools.runner(_func, range(10), 2)
    assert repr(r).startswith(r.__class__.__name__)
    assert 'jobs=2' in repr(r)
    assert 'iterable={}'.format(repr(range(10))) in repr(r)

    # Bad values
    with pytest.raises(ValueError):
        tools.runner(None, None, -1)


def test_mapkey():

    actual = tools.mapkey('key', range(5))
    expected = [('key', 0), ('key', 1), ('key', 2), ('key', 3), ('key', 4)]

    assert not isinstance(actual, (list, tuple))  # Make sure we get an iterator
    assert list(actual) == expected


def test_sorter():

    items = [1, 6, 3, 5, 9, 10]
    assert sorted(items) == tools.sorter(items)


# Python 2 isn't very forgiving when it comes to sorting.
# Make sure a useful error is raised for unorderable types
if six.PY3:
    def test_sorter_unorderable():
        # Unorderable types
        with pytest.raises(errors.UnorderableKeys):
            tools.sorter(['2', 1])


def test_sorter_exceptions():

    if not six.PY2:
        with pytest.raises(errors.UnorderableKeys):
            tools.sorter(['1', 1])

    def _k(v):
        raise TypeError('bad')

    with pytest.raises(TypeError):
        tools.sorter([2, 1], key=_k)


def test_Orderable():

    on = tools.Orderable(None)
    for v in (-1, 0, 1):
        assert on < v
        assert on <= v
        assert not on > v
        assert not on >= v
        assert on != v
        assert on.obj is None

    on = tools.Orderable(None, lt=False, le=False, gt=True, ge=True)
    for v in (-1, 0, 1):
        assert on > v
        assert on >= v
        assert not on < v
        assert not on <= v
        assert on != v
        assert on.obj is None

    # Actually perform equality test
    on = tools.Orderable(None, eq=None)
    assert on == on
    assert not on is False
    assert not on == 67

    # Never equal to a type
    on = tools.Orderable(None, eq=False)
    assert not on == on
    assert not on == on
    assert not on == 'True'
    assert not on == 21

    # Always equal to any type
    on = tools.Orderable(None, eq=True)
    assert on == on
    assert on == 'False'
    assert on == 10


def test_OrderableNone():

    assert isinstance(tools.OrderableNone, tools._OrderableNone)
    assert tools.OrderableNone.obj is None


def test_partition():

    data = [
        (1, 2),
        (1, 1),
        (2, 1),
        (3, 1),
        ('ptn', 'sort', 'data')]

    expected = {
        1: [(2,), (1,)],
        2: [(1,)],
        3: [(1,)],
        'ptn': [('sort', 'data')]}

    ptn = tools.partition(data)

    assert isinstance(ptn, dict)
    assert not isinstance(ptn, defaultdict)
    assert ptn == expected


def test_merge_partitions():

    dptn = {
        1: [(1, 2), (1, 1)],
        2: [(2, 1)],
        3: [(3, 1)],
        'ptn': [('ptn', 'sort', 'data')]}

    expected = {
        1: [(1, 2), (1, 1), (1, 2), (1, 1)],
        2: [(2, 1), (2, 1)],
        3: [(3, 1), (3, 1)],
        'ptn': [('ptn', 'sort', 'data'), ('ptn', 'sort', 'data')]}

    actual = tools.merge_partitions(dptn, dptn)
    assert expected == actual


def test_count_lines_exception(linecount_file):

    """
    Make sure known exceptions in `count_lines()` are raised.
    """

    path = linecount_file()
    with pytest.raises(ValueError):
        tools.count_lines(path, linesep='too many chars')


@pytest.mark.parametrize("linesep", ["\n", "\r\n"])
def test_count_lines_small(linesep, linecount_file):

    """
    Count lines of a file that fits in the buffer.
    """

    path = linecount_file(linesep)
    buff = os.stat(path).st_size + 2
    assert _icount_lines(path) == tools.count_lines(
        path, linesep=linesep, buffer=buff)


@pytest.mark.parametrize("linesep", ["\n", "\r\n"])
def test_count_lines_buffered(linesep, linecount_file):

    """
    Use the buffered method to count lines
    """

    path = linecount_file(linesep)
    buff = os.stat(path).st_size // 4
    assert _icount_lines(path) == tools.count_lines(
        path, linesep=linesep, buffer=buff)


def test_count_lines_split_buffer(tmpdir):

    """
    Explicitly test a scenario where the `linesep` character is 2 bytes long
    and is split across blocks.
    """

    path = str(tmpdir.mkdir('test_count_lines').join('split_buffer'))
    with open(path, 'wb') as f:
        f.write(b'\r\nhey some words')
    assert tools.count_lines(path, buffer=1, linesep='\r\n') == 1


def test_count_lines_literal_linesep(tmpdir):

    """
    Explicitly test a scenario where the input file contains a literal '\n'.
    """

    path = str(tmpdir.mkdir('test_count_lines').join('literal_linesep'))
    with open(path, 'w') as f:
        f.write('first line with stuff' + os.linesep)
        f.write('before \{} after'.format(os.linesep) + os.linesep)
    assert tools.count_lines(path) == 3


def test_count_lines_empty(tmpdir):

    """
    Completely empty file.
    """

    path = str(tmpdir.mkdir('test_count_lines').join('empty'))
    with open(path, 'w') as f:
        pass
    assert tools.count_lines(path) == 0


def test_count_lines_only_linesep(tmpdir):

    """
    File only contains a `linesep`.
    """

    path = str(tmpdir.mkdir('test_count_lines').join('only_linesep'))
    with open(path, 'w') as f:
        f.write(os.linesep)
    assert tools.count_lines(path) == 1


def test_count_lines_trailing_linesep(tmpdir):

    """
    Last line has a trailing `linesep`.
    """

    path = str(tmpdir.mkdir('test_count_lines').join('trailing_linesep'))
    with open(path, 'w') as f:
        f.write('line1' + os.linesep)
        f.write('line2' + os.linesep)
        f.write('line3' + os.linesep)
    assert tools.count_lines(path) == 3
