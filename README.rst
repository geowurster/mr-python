================
MapReduce Python
================

Experimental Pythonic MapReduce.

.. image:: https://travis-ci.org/geowurster/mr-python.svg?branch=master
    :target: https://travis-ci.org/geowurster/mr-python?branch=master

.. image:: https://coveralls.io/repos/geowurster/mr-python/badge.svg?branch=master
    :target: https://coveralls.io/r/geowurster/mr-python?branch=master


Canonical Word Count Example
============================

.. code-block:: python

    import re
    from mrpython import MRMemory


    class WordCount(MRMemory):

        def __init__(self):
            self.p = re.compile('[^a-zA-Z]+')

        def scrub_word(self, word):
            return self.p.sub('', word)

        def mapper(self, item):
            for word in item.split():
                word = self.scrub_word(word).strip().lower()
                yield word, 1

        def reducer(self, key, values):
            return sum(values)

    mr = WordCount()
    with open('LICENSE.txt') as f:
        for word, count in mr(f):
            print("{word}: {count}".format(word=word,  count=count))


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
