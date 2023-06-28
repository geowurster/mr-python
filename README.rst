======
tinymr
======

Experimental in-memory MapReduce.


Word Count Example
==================

As written, this only emits the 10 most common words and their counts:

.. code-block:: python

    from tinymr import MapReduce


    class WordCount(MapReduce):

        def __init__(self, most_common):

            """
            Parameters
            ----------
            most_common : int
                Only emit N most common words.
            """

            self.most_common = most_common

        def mapper(self, item):

            """Instances of individual words in a single line of text."""

            line = item.lower()
            for word in line.split():
                yield word, 1

        def reducer(self, key, values):

            """Count word frequence across the entire dataset."""

            return key, sum(values)

        def output(self, items):

            """Order results based on frequency descending."""

            ordered = sorted(
                items.items(),
                key=lambda x: tuple(reversed(x)),
                reverse=True)

            return ordered[:self.most_common]


    with open("LICENSE.txt") as f, WordCount(10) as mapreduce:
        for word, count in mapreduce(f):
            print(word, count)

Output:

.. code-block:: console

    the 13
    of 12
    or 11
    and 8
    in 6
    this 5
    copyright 5
    any 4
    provided 3
    not 3


Developing
==========

.. code-block:: console

    $ git clone https://github.com/geowurster/tinymr.git
    $ cd tinymr
    $ python3 -m venv venv
    (venv) $ pip install --upgrade pip setuptools -e ".[test]"
    (venv) $ pytest --cov tinymr --cov-report term-missing
    (venv) $ pycodestyle


License
=======

See ``LICENSE.txt``.


Changelog
=========

See ``CHANGES.md``.
