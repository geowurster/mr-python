======
tinymr
======

Experimental Pythonic MapReduce.

.. image:: https://travis-ci.org/geowurster/tinymr.svg?branch=master
    :target: https://travis-ci.org/geowurster/tinymr?branch=master

.. image:: https://coveralls.io/repos/geowurster/tinymr/badge.svg?branch=master
    :target: https://coveralls.io/r/geowurster/tinymr?branch=master

Inspired by `Spotify's luigi framework <http://www.github.com/Spotify/luigi>`_,
specifically its interface for Hadoop's streaming jar.


The Word Count Example
======================

This is pretty naive, only performs some really basic word normalization, and
does not handle punctuation.  The input is a stream of text read line
by line, and the output is a dictionary where keys are words and values are
the number of times each word appeared in the text stream.

Here's a very fast and efficient word count example using Python's builtins:

.. code-block:: python

    import itertools as it
    import operator as op

    def builtin_mr(lines):

        words = map(op.methodcaller('lower'), lines)
        words = map(op.methodcaller('split'), words)
        concatenated = it.chain.from_iterable(words)
        return Counter(concatenated)

Currently the only MapReduce implementation is in-memory and serial:

.. code-block:: python

    from tinymr.memory import MemMapReduce
    from tinymr.tools import single_key_output

    class WordCount(MemMapReduce):

        def mapper(self, item):
            return zip(item.lower().split(), it.repeat(1))

        def reducer(self, key, values):
            yield key, sum(values)

        def output(self, items):
            """See ``single_key_output()``'s docstring for more info."""
            return single_key_output(items)

    with open('LICENSE.txt') as f:
        wc = WC()
        results = wc(f)

Truncated output:

.. code-block:: json

    {
        "a": 1,
        "above": 2,
        "advised": 1,
        "all": 1,
        "and": 8,
        "andor": 1
    }


Developing
==========

.. code-block:: console

    $ git clone https://github.com/geowurster/tinymr.git
    $ cd tinymr
    $ pip install -e .\[dev\]
    $ py.test --cov tinymr --cov-report term-missing


License
=======

See ``LICENSE.txt``


Changelog
=========

See ``CHANGES.md``
