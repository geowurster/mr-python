"""
Unittests for mrpython.memory
"""


import pytest

from mrpython import MRMemory


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
def test_MRMemory(jobs, tiny_text, expected):

    class WordCount(MRMemory):

        def mapper(self, item):
            for word in item.split():
                yield word, 1

        def reducer(self, key, values):
            return sum(values)

    actual = dict(WordCount()(tiny_text, jobs=jobs))
    assert actual == expected
