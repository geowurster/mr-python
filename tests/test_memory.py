"""
Unittests for mrpython.memory
"""


import pytest

import mrpython as mr
import mrpython.memory


# TODO: There has to be a way to get this fixture's output with pytest instead of pasting
TINY_TEXT_MR_OUTPUT = {
    'word': 2,
    'something': 2,
    'else': 2,
    'mr': 1,
    'python': 1,
    'could': 1,
    'be': 1,
    'cool': 1,
    '1': 1
}


@pytest.mark.parametrize("jobs,expected", [
    (1, TINY_TEXT_MR_OUTPUT),
    (1, TINY_TEXT_MR_OUTPUT)])  # TODO: Test with 2 jobs
def test_MRSerialNoSort(jobs, tiny_text, expected):

    class WordCount(mr.memory.MRSerialNoSort):

        def mapper(self, item):
            for word in item.split():
                yield word, 1

        def reducer(self, key, values):
            yield key, sum(values)

        def final_reducer(self, pairs):
            return {k: v[0] for k, v in pairs}

    actual = WordCount()(tiny_text)
    assert actual == expected


@pytest.mark.parametrize("jobs,expected", [
    (1, TINY_TEXT_MR_OUTPUT),
    (1, TINY_TEXT_MR_OUTPUT)])  # TODO: Test with 2 jobs
def test_MRSerial(jobs, tiny_text, expected):

    class WordCount(mr.memory.MRSerial):

        def mapper(self, item):
            for word in item.split():
                yield word, word, 1

        def reducer(self, key, values):
            yield key, key, sum(values)

        def final_reducer(self, pairs):
            return {k: tuple(v)[0] for k, v in pairs}

    actual = WordCount()(tiny_text)
    assert actual == expected
