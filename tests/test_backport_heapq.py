"""
Unittests for tinymr._backport_heapq
"""


import os

import pytest
import six


@pytest.mark.skipif(
    os.environ.get('TRAVIS', 'true').lower() == 'true',
    reason="Not running on Travis")
def test_diff():

    """
    Make sure there haven't been any changes to the heapq module.
    """

    url = 'https://raw.githubusercontent.com/python/cpython/master/Lib/heapq.py'
    response = six.moves.urllib.request.urlopen(url)
    actual = response.read().strip()
    if not six.PY2:
        actual = actual.decode('utf-8')

    with open(os.path.join('tests', '_heapq_master.py')) as f:
        expected = f.read().strip()

    assert expected == actual
