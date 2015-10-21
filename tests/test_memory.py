"""
Unittests for mrpython.memory
"""


from mrpython import mr_memory
from mrpython import MRMemory


def test_mr_memory():
    text = [
        "word something else",
        "else something word",
        "mr python could be cool 1"
    ]

    def mapper(line):
        for word in line.split():
            yield word, 1

    def reducer(word, frequency):
        return sum(frequency)

    actual = mr_memory(text, mapper, reducer)
    expected = {
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
    assert actual == expected


def test_MRMemory():

    text = [
        "word something else",
        "else something word",
        "mr python could be cool 1"
    ]

    class WordCount(MRMemory):

        def mapper(self, item):
            for word in item.split():
                yield word, 1

        def reducer(self, key, values):
            return sum(values)

    actual = dict(WordCount()(text))
    expected = {
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
    assert actual == expected
