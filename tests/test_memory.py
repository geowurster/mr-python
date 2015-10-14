"""
Unittests for mrpython.memory
"""


from mrpython import mr_memory


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
