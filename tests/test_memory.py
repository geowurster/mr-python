"""
Unittests for tinymr.memory
"""


import tinymr as mr
import tinymr.memory


def test_MRSerial_no_sort(tiny_text, tiny_text_wc_output):

    class WordCount(mr.memory.MRSerial):

        def mapper(self, item):
            for word in item.split():
                yield word, word, 1

        def reducer(self, key, values):
            yield key, key, sum(values)

        def final_reducer(self, pairs):
            return {k: tuple(v)[0] for k, v in pairs}

    text = tiny_text.splitlines()
    actual = WordCount()(text)
    assert actual == tiny_text_wc_output
