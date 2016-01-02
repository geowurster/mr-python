================
MapReduce Python
================

.. image:: https://travis-ci.org/geowurster/tinymr.svg?branch=master
    :target: https://travis-ci.org/geowurster/tinymr?branch=master

.. image:: https://coveralls.io/repos/geowurster/tinymr/badge.svg?branch=master
    :target: https://coveralls.io/r/geowurster/tinymr?branch=master

Experimental Pythonic MapReduce inspired by `Spotify's luigi framework <http://www.github.com/Spotify/luigi>`_.


Canonical Word Count Example
============================

.. code-block:: python

    from collections import Counter
    import json
    import re
    import sys

    import tinymr as mr
    import tinymr as memory


    class WordCount(mr.memory.MRSerial):

        """
        The go-to MapReduce example.

        Don't worry, a better example is on its way:
        https://github.com/geowurster/tinymr/issues/11
        """

        # Counting word occurrence does not benefit from sorting post-map or
        # post-reduce and our `final_reducer()` doesn't care about key order
        # so we can disable sorting for a speed boost.
        sort_map = False
        sort_reduce = False
        sort_final_reduce = False

        def __init__(self):

            """
            Stash a regex to strip off punctuation so we can grab it later.
            """

            self.pattern = re.compile('[\W_]+')

        def mapper(self, line):

            """
            Take a line of text from the input file and figure out how many
            times each word appears.

            An alternative, simpler implementation would be:

                def mapper(self, item):
                    for word in item.split():
                        word = self.pattern.sub('', word)
                        if word:
                            yield word.lower(), 1

            This simpler example is more straightforward, but holds more data
            in-memory.  The implementation below does more work per line but
            potentially has a smaller memory footprint.  Like anything
            MapReduce the implementation benefits greatly from knowing a lot
            about the input data.
            """

            # Normalize all words to lowercase
            line = line.lower().strip()

            # Strip off punctuation
            line = [self.pattern.sub('', word) for word in line]

            # Filter out empty strings
            line = filter(lambda x: x != '', line)

            # Return an iterable of `(word, count)` pairs
            return Counter(line).items()

        def reducer(self, key, values):

            """
            Just sum the number of times each word appears in the entire
            dataset.

            At this phase `key` is a word and `values` is an iterable producing
            all of the values for that word from the map phase.  Something like:

                key = 'the'
                values = (1, 1, 2, 2, 1)

            The word `the` appeared once on 3 lines and twice on two lines for
            a total of `7` occurrences, so we yield:

                ('the', 7)
            """

            yield key, sum(values)

        def final_reducer(self, pairs):

            """
            Normally this phase is where the final dataset is written to disk,
            but since we're operating in-memory we just want to re-structure as
            a dictionary.

            `pairs` is an iterator producing `(key, iter(values))` tuples from
            the reduce phase, and since we know that we only produced one key
            from each reduce we want to extract it for easier access later.
            """

            return {k: tuple(v)[0] for k, v in pairs}


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

Word Count Workflow
-------------------

Internally, the workflow looks like this:

**Input data**:

.. code-block:: console

    $ head -10 LICENSE.txt

    New BSD License

    Copyright (c) 2015, Kevin D. Wurster
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice, this
      list of conditions and the following disclaimer.

**Map**

Count occurrences of each word in every line.

.. code-block:: python

    # Input line
    line = 'Copyright (c) 2015, Kevin D. Wurster'

    # Sanitized words
    words = ['Copyright', 'c', '2015', 'Kevin', 'D', 'Wurster']

    # Return tuples with word as the first element and count as the second
    pairs = [('Copyright', 1), ('c', 1), ('2015', 1), ('Kevin', 1), ('D', 1), ('Wurster', 1)]

**Partition**

Organize all of the ``(word, count)`` pairs by ``word``.  The ``word`` keys are
kept at this point in case the data is sorted.  Sorting grabs the second to last
key, so the data could be partitioned on one key and sorted on another with
``(word, sort, count)``.  The second to last key is used for sorting so the keys
that appear below match the ``word`` only because a ``sort`` key was not given.

Words that appear in the input text on multiple lines have multiple
``(word, count)`` pairs.  A ``count`` of ``2`` would indicate a word that
appeared twice on a single line, but our input data does not have this
condition.

.. code-block:: python

    {
        'use': [('use', 1)],
        'new': [('new', 1)],
        'above': [('above', 1)],
        'redistributions': [('redistributions', 1)],
        'source': [('source', 1), ('source', 1)],
        'without': [('without', 1)],
        'notice': [('notice', 1)],
        'redistribution': [('redistribution', 1)],
        'bsd': [('bsd', 1)],
        'that': [('that', 1)],
        'permitted': [('permitted', 1)],
        'forms': [('forms', 1)],
        'rights': [('rights', 1)],
        'must': [('must', 1)],
        'list': [('list', 1)],
        'are': [('are', 1), ('are', 1)],
        'with': [('with', 1)],
        'd': [('d', 1)],
        'license': [('license', 1)],
        'binary': [('binary', 1)],
        'reserved': [('reserved', 1)],
        'or': [('or', 1)],
        'the': [('the', 1), ('the', 1), ('the', 1)],
        'and': [('and', 1), ('and', 1), ('and', 1)],
        'all': [('all', 1)],
        'met': [('met', 1)],
        'this': [('this', 1)],
        'provided': [('provided', 1)],
        'of': [('of', 1), ('of', 1)],
        'c': [('c', 1)],
        'wurster': [('wurster', 1)],
        'code': [('code', 1)],
        'disclaimer': [('disclaimer', 1)],
        'modification': [('modification', 1)],
        'copyright': [('copyright', 1), ('copyright', 1)],
        'retain': [('retain', 1)], 'kevin': [('kevin', 1)],
        'conditions': [('conditions', 1), ('conditions', 1)],
        'following': [('following', 1), ('following', 1)],
        'in': [('in', 1)], '2015': [('2015', 1)]
    }

**Reduce**

Sum ``count`` for each ``word``.

.. code-block:: python

    # The ``reducer()`` receives a key and an iterator of values
    key = 'the'
    values = (1, 1, 1)
    yield key, sum(values)

**Partition**

The reducer does not _have_ to produces the same key it was given, so the data
is partitioned by key again, which is superfluous for this wordcount example.
Again the keys are kept in case the data is sorted and only match ``word``
because an optional ``sort`` key was not given.

.. code-block:: python

    {
        'following': [('following', 2)],
        '2015': [('2015', 1)],
        'reserved': [('reserved', 1)],
        'permitted': [('permitted', 1)],
        'forms': [('forms', 1)],
        'are': [('are', 2)],
        'license': [('license', 1)],
        'c': [('c', 1)],
        'kevin': [('kevin', 1)],
        'without': [('without', 1)],
        'redistribution': [('redistribution', 1)],
        'copyright': [('copyright', 2)],
        'met': [('met', 1)],
        'use': [('use', 1)],
        'the': [('the', 3)],
        'rights': [('rights', 1)],
        'that': [('that', 1)],
        'or': [('or', 1)],
        'this': [('this', 1)],
        'with': [('with', 1)],
        'source': [('source', 2)],
        'new': [('new', 1)],
        'binary': [('binary', 1)],
        'wurster': [('wurster', 1)],
        'list': [('list', 1)],
        'must': [('must', 1)],
        'of': [('of', 2)],
        'retain': [('retain', 1)],
        'modification': [('modification', 1)],
        'and': [('and', 3)],
        'above': [('above', 1)],
        'all': [('all', 1)],
        'redistributions': [('redistributions', 1)],
        'bsd': [('bsd', 1)],
        'in': [('in', 1)],
        'conditions': [('conditions', 2)],
        'disclaimer': [('disclaimer', 1)],
        'd': [('d', 1)],
        'code': [('code', 1)],
        'provided': [('provided', 1)],
        'notice': [('notice', 1)]
    }

**Final Reduce**

The default implementation is to return ``(key, iter(values))`` pairs from the
``final_reducer()``, which would look something like:

.. code-block:: python

    values = [
        ('the', (3,)),
        ('in', (1,),
    ]

But a dictionary is much more useful, and we know that we only got a single
value for each ``word`` in the reduce phase, so we can extract that value
and produce a dictionary.

.. code-block:: python

    return {k: tuple(v)[0] for k, v in values}

The ``tuple()`` call is included because the data in the ``value`` key is
_always_ an iterable object but _may_ be an iterator.  Calling ``tuple()``
expands the iterable and lets us get the first element.


Developing
==========

.. code-block:: console

    $ git clone https://github.com/geowurster/tinymr.git
    $ cd tinymr
    $ pip install -e .\[dev\]
    $ py.test tests --cov tinymr --cov-report term-missing


License
=======

See ``LICENSE.txt``


Changelog
=========

See ``CHANGES.md``
