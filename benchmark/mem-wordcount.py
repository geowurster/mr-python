"""Compare an in-memory tinymr implementation to a more straightforward one
using Python's builtins.

Pass a file as the first argument to perform tests on that file.
"""


from collections import Counter
import itertools as it
import operator as op
import sys

from tinymr import tools
from tinymr.bench import timer
from tinymr.memory import MemMapReduce


def builtin_mr(lines):

    """About as efficient as you can get.

    Parameters
    ----------
    lines : iter
        Iterator producing lines from a text file.

    Returns
    -------
    collections.Counter
    """

    words = map(op.methodcaller('lower'), lines)
    words = map(op.methodcaller('split'), words)
    concatenated = it.chain.from_iterable(words)
    return Counter(concatenated)


class WordCount(MemMapReduce):

    def mapper(self, item):
        return zip(item.lower().split(), it.repeat(1))

    def reducer(self, key, values):
        yield key, sum(values)

    def output(self, items):
        return tools.single_key_output(items)


if __name__ == '__main__':

    if len(sys.argv) == 1:
        print(__doc__.strip())
        exit(1)

    infile = sys.argv[1]
    print("Running wordcount tests on: {}".format(infile))

    print("Running tinymr ...")
    with open(infile) as f, timer() as elapsed, WordCount() as wc:
        tuple(wc(f))
    print(elapsed.seconds)

    print("Running builtin ...")
    with open(infile) as f, timer() as elapsed:
        builtin_mr(f)
    print(elapsed.seconds)
