"""
Test fixtures
"""


from collections import OrderedDict
import os

import pytest

import tinymr as mr
import tinymr.memory


@pytest.fixture(scope='function')
def tiny_text():
    return os.linesep.join([
        "word something else",
        "else something word",
        "mr python could be cool 1"
    ])


@pytest.fixture(scope='function')
def tiny_text_wc_output():
    return OrderedDict((
        ('1', 1),
        ('be', 1),
        ('cool', 1),
        ('could', 1),
        ('else', 2),
        ('mr', 1),
        ('python', 1),
        ('something', 2),
        ('word', 2),
    ))


@pytest.fixture(scope='function')
def linecount_file(tmpdir):

    """
    Return a function that copies the license and writes it to a tempfile.
    Can optionally change the `linesep` character.
    """

    def _linecount_file(linesep=os.linesep):
        path = str(tmpdir.mkdir('test').join('count-lines.txt'))
        with open('LICENSE.txt') as src, open(path, 'w') as dst:
            for line in src:
                dst.write(line.strip() + linesep)
        return path
    return _linecount_file
