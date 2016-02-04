"""
Unittests for tinymr._mrtools
"""


import pytest
import six

from tinymr import _mrtools
from tinymr import errors


def test_sorter():

    items = [1, 6, 3, 5, 9, 10]
    assert sorted(items) == _mrtools.sorter(items)


# Python 2 isn't very forgiving when it comes to sorting.
# Make sure a useful error is raised for unorderable types
if six.PY3:
    def test_sorter_unorderable():
        # Unorderable types
        with pytest.raises(errors.UnorderableKeys):
            _mrtools.sorter(['2', 1])


def test_sorter_exceptions():

    if not six.PY2:
        with pytest.raises(errors.UnorderableKeys):
            _mrtools.sorter(['1', 1])

    def _k(v):
        raise TypeError('bad')

    with pytest.raises(TypeError):
        _mrtools.sorter([2, 1], key=_k)
