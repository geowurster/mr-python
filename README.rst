================
MapReduce Python
================

Experimental Pythonic MapReduce inspired by `Spotify's luigi framework <http://www.github.com/Spotify/luigi>`_.

.. image:: https://travis-ci.org/geowurster/mr-python.svg?branch=master
    :target: https://travis-ci.org/geowurster/mr-python?branch=master

.. image:: https://coveralls.io/repos/geowurster/mr-python/badge.svg?branch=master
    :target: https://coveralls.io/r/geowurster/mr-python?branch=master


Canonical Word Count Example
============================

Currently there are two MapReduce implementations, one that includes sorting and
one that does not.  The example below would not benefit from sorting so we can
take advantage of the inherent optimization of not sorting.  The API is the same
but ``mrpython.memory.MRSerial()`` sorts after partitioning and again between the
``reducer()`` and ``final_reducer()``.

.. code-block:: python

    import json
    import re
    import sys

    from mrpython.memory import MRSerialNoSort


    class WordCount(MRSerialNoSort):

        def __init__(self):
            self.pattern = re.compile('[\W_]+')

        def mapper(self, item):
            for word in item.split():
                word = self.pattern.sub('', word)
                if word:
                    yield word.lower(), 1

        def reducer(self, key, values):
            yield key, sum(values)

        def final_reducer(self, pairs):
            return {k: v[0] for k, v in pairs}


    wc = WordCount()
    with open('LICENSE.txt') as f:
        out = wc(f)
        print(json.dumps(out, indent=4, sort_keys=True))

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

    $ git clone https://github.com/geowurster/mr-python.git
    $ cd mr-python
    $ pip install -e .\[dev\]
    $ py.test tests --cov mrpython --cov-report term-missing


License
=======

See ``LICENSE.txt``


Changelog
=========

See ``CHANGES.md``
