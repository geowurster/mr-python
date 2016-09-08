"""Base classes.  Subclass away!"""


import abc


class BaseMapReduce(object):

    """Base class for various MapReduce implementations."""

    @abc.abstractmethod
    def mapper(self, item):
        """Apply keys to each input item."""
        raise NotImplementedError

    @abc.abstractmethod
    def reducer(self, key, values):
        """Process the data for a single key."""
        raise NotImplementedError

    def output(self, items):

        """Intercept the output post-reduce phase for one final transform.
        Data looks like:

            (key3, values)
            (key1, values)
            (key2, values)

        Data is not guaranteed to be ordered by key.  Values are guaranteed to
        be iterable but not of any specific type.
        """

        return items

    def __init__(self):
        """Subclass and use for pre-map phase setup."""
        pass

    def init_reduce(self):
        """Subclass and use for pre-reduce phase setup."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Subclass for custom closing operations.  Remember that a MapReduce
        task's ``__init__`` can be used for pre-map setup.
        """
        pass

    # @abc.abstractmethod
    # def combiner(self, key, values):
    #     """Mini reduce operation for the data from one map operation."""
    #     raise NotImplementedError

    # @property
    # @functools.lru_cache()
    # def _has_combiner(self):
    #     class _ProbeCombiner(object):
    #         pass
    #     try:
    #         self.combiner(_ProbeCombiner, _ProbeCombiner)
    #         return True
    #     except NotImplementedError:
    #         return False
