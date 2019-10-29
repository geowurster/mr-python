"""Tools for working with data in a MapReduce context."""


import itertools as it
import sys


if sys.version_info.major == 2:
    from collections import Iterator
else:
    from collections.abc import Iterator


__all__ = ["slicer"]


def slicer(iterator, chunksize, container=tuple):

    """Read an iterator in chunks.

    Example:

        >>> for p in slicer(range(5), 2):
        ...     print(p)
        (0, 1)
        (2, 3)
        (4,)

    Parameters
    ----------
    iterator : iter
        Input stream.
    chunksize : int
        Number of records to include in each chunk. The last chunk will be
        incomplete unless the number of items in the stream is evenly
        divisible by `size`.
    container : callable, optional
        Place sliced items into this container before yielding like:
        ``container([1, 2, 3])``.

    Yields
    ------
    tuple
    """

    if not isinstance(iterator, Iterator):
        raise TypeError("Object is not an iterator: {}".format(iterator))

    while True:
        v = container(it.islice(iterator, chunksize))
        if v:
            yield v
        else:
            break
