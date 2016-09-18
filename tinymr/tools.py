"""Tools for working with data in a MapReduce context."""


import itertools as it
import io
import os

from tinymr import _compat


def slicer(iterable, chunksize):

    """
    Read an iterator in chunks.

    Example:

        >>> for p in slicer(range(5), 2):
        ...     print(p)
        (0, 1)
        (2, 3)
        (4,)

    Parameters
    ----------
    iterable : iter
        Input stream.
    chunksize : int
        Number of records to include in each chunk.  The last chunk will be
        incomplete unless the number of items in the stream is evenly
        divisible by `size`.

    Yields
    ------
    tuple
    """

    iterable = iter(iterable)
    while True:
        v = tuple(it.islice(iterable, chunksize))
        if v:
            yield v
        else:
            raise StopIteration


def mapkey(key, values):

    """
    Given a key and a series of values, create a series of `(key, value)`
    tuples.

    Example:

        >>> for pair in mapkey('key', range(5)):
        ...     print(pair)
        ('key', 0)
        ('key', 1)
        ('key', 2)
        ('key', 3)
        ('key', 4)

    Parameters
    ----------
    key : object
        Object to use as the first element of each output tuples.

    Returns
    -------
    iter
    """

    return _compat.zip(it.repeat(key), values)


def count_lines(
        path,
        buffer=io.DEFAULT_BUFFER_SIZE,
        linesep=os.linesep,
        encoding='utf-8'):

    """
    Quickly count the number of lines in a text file.  Useful for computing an
    optimal `chunksize`.

    Comparable to `$ wc -l` for files larger than ``~100 MB``, and significantly
    faster as the file gets smaller (ignoring Python interpreter startup and
    imports).  For reference just looping over all the lines in a 1.2 GB file
    takes ~6 or 7 seconds, but `count_lines()` takes ~1.5.

    For reference just looping over all the lines in the ``1.2 GB`` file takes
    ``~6 to 7 sec``.

    Speed is achieved by reading the file in blocks and counting the occurrence
    of `linesep`.  For `linesep` strings that are larger than 1 byte we
    check to make sure a `linesep` was not split across blocks.

    Scott Persinger on StackOverflow gets credit for the core logic.
    http://stackoverflow.com/questions/845058/how-to-get-line-count-cheaply-in-python

    Parameters
    ----------
    path : str
        Path to input text file.
    buffer : int, optional
        Buffer size in bytes.
    linesep : str, optional
        Newline character.  Cannot be longer than 2 bytes.
    encoding : str, optional
        Encoding of newline character so it can be converted to `bytes()`.

    Returns
    -------
    int
    """

    nl = bytearray(linesep.encode(encoding))
    size = os.stat(path).st_size

    if len(nl) > 2:
        raise ValueError(
            "Cannot handle linesep characters larger than 2 bytes.")

    # File is small enough to just process in one go
    elif size < buffer:
        with open(path, 'rb', buffering=buffer) as f:
            return getattr(f, 'raw', f).read(buffer).count(nl)  # No raw in PY2

    # Process in chunks
    # Number of chunks is pre-determined so that the last chunk can be
    # read into a fresh array to avoid double-counting
    else:

        buff = bytearray(buffer)
        blocks = size // buffer
        lines = 0

        with open(path, 'rb') as f:

            # Optimize the loops a bit in case we are working with a REALLY
            # big file
            readinto = getattr(f, 'raw', f).readinto  # no raw in PY2
            count = buff.count

            for i in range(blocks):
                readinto(buff)
                lines += count(nl)

                # linesep is something like \r\n, which means it could be split
                # across blocks, so we need an additional check
                if buff[-1] == nl[0]:
                    lines += 1

            # The last bit of data in the file is smaller than a block
            # We can't just read this into the constant buffer because
            # it the remaining bytes would still be populated by the
            # previous block, which could produce duplicate counts.
            lines += getattr(f, 'raw', f).read().count(nl)  # No raw in PY2

        return lines


def popitems(dictionary):

    """Like ``dict.popitem()`` but iterates over all ``(key, value)`` pairs,
    emptying the input ``dictionary``.  Useful for maintaining a lower memory
    footprint at the expense of some additional function calls.

    Parameters
    ----------
    dictionary : dict
        ``dict()`` to process.

    Yields
    ------
    tuple
        ``(key, value)``
    """

    while True:
        try:
            yield dictionary.popitem()
        except KeyError:
            raise StopIteration


def poplist(l):

    """Like ``list.pop(0)`` but iterates over all items in the input list and
    emptying it in the process.  Iterates from beginning to end.

    Parameters
    ----------
    l : list
        ``list()`` to process.

    Yields
    ------
    object
    """

    while True:
        try:
            yield l.pop(0)
        except IndexError:
            raise StopIteration


def single_key_output(items):

    """Override ``MapReduce.output()`` with a custom method that passes
    ``items`` to this method when dealing with outputs that only have a single
    value for every key.  For the standard word count example this would
    change the output from:

        (word1, (sum,)
        (word2, (sum,)
        (word3, (sum,)

    to:

        (word1, sum)
        (word2, sum)
        (word3, sum)

    The result is that the output can be passed directly to ``dict()``, if it
    fits in memory for more straightforwad key -> value lookups, rather than
    doing: ``next(iter(output[key]))``.

    Parameters
    ----------
    items : iter
        Stream of ``(key, values)`` pairs where ``values`` is also an iterable.

    Yields
    ------
    tuple
        The equivalent of ``(key, next(iter(values)))``.
    """

    for key, value in items:
        yield key, next(iter(value))
