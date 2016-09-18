======
tinymr
======

Experimental Pythonic MapReduce / an exploration in parallelism.

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

    from collections import Counter
    import itertools as it
    import operator as op

    def builtin_mr(lines):
        words = map(op.methodcaller('lower'), lines)
        words = map(op.methodcaller('split'), words)
        concatenated = it.chain.from_iterable(words)
        return Counter(concatenated)

    with open('LICENSE.txt') as f:
        builtin_mr(f)

Currently the only MapReduce implementation is in-memory:

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


Composite Keys
--------------

``tinymr`` transacts in ``tuples`` for several reasons: they're cheap to
create, easy to read, and provide a uniform API for a (hopefully eventual)
MapReduce implementation processing data that doesn't fit into memory.

The trade off is that the key layout must be known before data is processed
in order to partition and sort data properly and probably a bit of a
performance hit when they keys are transformed internally.


.. code-block:: python

    from tinymr.memory import MemMapReduce

    class CompositeKey(MemMapReduce):

        n_partition_keys = 2
        n_sort_keys = 2

        def mapper(self, item):
            yield partition1, partition2, sort1, sort2, data


Combine Phase
-------------

Some MapReduce implementations use a combiner to reduce the amount of data
coming out of each mapper.  Parallel and threaded in-memory tasks would
benefit from a combine phase to reduce the amount of data passing through
``pickle``, which is expensive.  The cost is an extra partition + sort phase
that I have tried implementing many times, the first of which probably made
it into the commit history, and the rest weren't good enough.  This is
probably more useful for MapReduce implementations that include intermediary
disk I/O so I'll try tackling it again if ``tinymr`` makes it that far.  My
gut instinct is that its just not worth it for in-memory tasks, and the code
required to do it at a reasonable speed is difficult to read and un-Pythonic.
See the `Roadmap`_ for more info.


Roadmap
-------

Ideally ``tinymr`` will:

1. Have MapReduce implementations that run in-memory and with intermediary disk I/O.
2. Let the user decide if the map and/or reduce happens in-memory or on disk.  Key handling is such that the API is the same and a flags should suffice.
3. Support a combine phase, likely entirely in-memory.
4. Provide the tools necessary for optimizing tasks.


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
