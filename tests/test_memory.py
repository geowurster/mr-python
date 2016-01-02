"""
Unittests for tinymr.memory
"""


import tinymr as mr
import tinymr.memory


def test_MRSerial_no_sort(tiny_text, tiny_text_wc_output, mr_wordcount_memory_no_sort):

    with mr_wordcount_memory_no_sort() as wc:
        actual = wc(tiny_text.splitlines())
    assert actual == tiny_text_wc_output


def test_MRSerial_init_reduce(tiny_text, tiny_text_wc_output, mr_wordcount_memory_no_sort):

    class WCInitReduce(mr_wordcount_memory_no_sort):

        def init_reduce(self):
            self.initialized = True

    with WCInitReduce() as wc:
        actual = wc(tiny_text.splitlines())
