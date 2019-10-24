"""Tests for ``tinymr``."""


from tinymr import MapReduce


def test_context_manager():
    class WordCount(MapReduce):

        def __init__(self):
            self.closed = False

        def mapper(self, item):
            return None, item

        def reducer(self, key, values):
            return key, values

        def close(self):
            self.closed = True

    with WordCount() as wc:
        assert not wc.closed

    assert wc.closed
