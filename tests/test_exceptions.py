import pytest

from tinymr import ElementCountError, MapReduce


@pytest.mark.parametrize("bad", ((1,), (1, 2, 3, 4)))
def test_malformed_mapper(bad):

    class WordCount(MapReduce):

        def mapper(self, item):
            yield bad

        def reducer(self, key, values):
            return key, values

    wc = WordCount()
    with pytest.raises(ElementCountError):
        wc([None])


@pytest.mark.parametrize("bad", ((1,), (1, 2, 3, 4)))
def test_malformed_reducer(bad):

    class WordCount(MapReduce):

        def mapper(self, item):
            yield 0, item

        def reducer(self, key, values):
            return bad

    wc = WordCount()
    with pytest.raises(ElementCountError):
        wc([None])
