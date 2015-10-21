"""
Unittests for mrpython.memory
"""


import pytest

from mrpython import mr_memory
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



def test_mr_memory(tiny_text, tiny_text_mr_output):

    def mapper(line):
        for word in line.split():
            yield word, 1

    def reducer(word, frequency):
        return sum(frequency)

    actual = mr_memory(tiny_text, mapper, reducer)
    assert actual == tiny_text_mr_output


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


# def test_MRMemory_parallel(tiny_text, tiny_text_mr_output):
#
#     class WordCount(MRMemory):
#
#         def mapper(self, item):
#             for word in item.split():
#                 yield word, 1
#
#         def reducer(self, key, values):
#             return sum(values)
#
#     actual = WordCount()(tiny_text, jobs=2)
#     assert dict(actual) == tiny_text_mr_output
