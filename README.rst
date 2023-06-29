######
tinymr
######

In-memory MapReduce. A personal project.


******************
Word Count Example
******************

Classic. Unavoidable. As written, this only emits the 10 most common words
and their counts:

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

            """Count word frequency across the entire dataset."""

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

.. code-block::

    # Set up workspace
    $ git clone https://github.com/geowurster/tinymr.git
    $ cd tinymr
    $ python3 -m venv venv
    (venv) $ pip install --upgrade pip setuptools -e ".[test]"

    # Run tests
    (venv) $ pytest --cov tinymr --cov-report term-missing

    # Run linters
    (venv) $ pycodestyle
    (venv) $ pydocstyle


Last Working State
==================

.. code-block::

    (venv) $ python3 --version
    Python 3.11.4

    (venv) $ python3 -c "import platform; print(platform.platform())"
    macOS-12.6.7-arm64-arm-64bit

    (venv) $ pip freeze
    coverage==7.2.7
    iniconfig==2.0.0
    packaging==23.1
    pluggy==1.2.0
    pycodestyle==2.10.0
    pydocstyle==6.3.0
    pytest==7.4.0
    pytest-cov==4.1.0
    snowballstemmer==2.2.0
