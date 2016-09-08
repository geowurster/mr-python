"""Unittests for ``tinymr.tools``."""


import os

import pytest

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


def test_mapkey():

    actual = tools.mapkey('key', range(5))
    expected = [('key', 0), ('key', 1), ('key', 2), ('key', 3), ('key', 4)]

    assert not isinstance(actual, (list, tuple))  # Make sure we get an iterator
    assert list(actual) == expected


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
