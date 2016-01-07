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
def mr_wordcount_memory_no_sort():

    class WordCount(mr.memory.MapReduce):

        def mapper(self, item):
            for word in item.split():
                yield word, 1

        def reducer(self, key, values):
            yield key, sum(values)

        def final_reducer(self, pairs):
            return {k: tuple(v)[0] for k, v in pairs}

    return WordCount
