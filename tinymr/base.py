"""Base classes.  Subclass away!"""


import abc
import functools


class _MRInternal(object):

    """For internal use only.  You want ``BaseMapReduce()``.

    This class provides a place to accumulate methods and properties that
    are only useful inside MapReduce implementations.
    """

    @abc.abstractmethod
    def __call__(self, *args, **kwargs):
        """Responsible for doing all the work.  This is subclassed by the
        various ``tinymr`` MapReduce implementations.
        """
        raise NotImplementedError

    @property
    @functools.lru_cache()
    def _ptn_key_idx(self):
        if self.n_partition_keys == 1:
            return 0
        else:
            return slice(0, self.n_partition_keys)
    
    @property
    @functools.lru_cache()
    def _sort_key_idx(self):
        # Ensure a lack of sort keys is properly handled down the line by
        # letting something fail spectacularly
        if self.n_sort_keys == 0:
            return None
        elif self.n_sort_keys == 1:
            return self.n_partition_keys + 1
        else:
            start = self.n_partition_keys
            stop = start + self.n_sort_keys
            return slice(start, stop)

    # @property
    # @functools.lru_cache()
    # def _has_combiner(self):
    #     """Attempt to figure out if the ``combiner()`` is implemented.
    #
    #     Returns
    #     -------
    #     bool
    #     """
    #
    #     class _ProbeCombiner(object):
    #         pass
    #
    #     # Probably implemented as a generator
    #     try:
    #         self.combiner(_ProbeCombiner, _ProbeCombiner)
    #         return True
    #     # Definitely not implemented
    #     except NotImplementedError:
    #         return False
    #     # Might not be implemented or might not be broken.  Just assume it is
    #     # implemented and let the combine phase fail if its not
    #     except Exception:
    #         return True


class BaseMapReduce(_MRInternal):

    """Base class for various MapReduce implementations."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # @property
    # def jobs(self):
    #     return 1
    #
    # @property
    # def map_jobs(self):
    #     return self.jobs
    #
    # @property
    # def reduce_jobs(self):
    #     return self.jobs
    #
    # @property
    # def chunksize(self):
    #     return 1
    #
    # @property
    # def map_chunksize(self):
    #     return self.chunksize
    #
    # @property
    # def reduce_chunksize(self):
    #     return self.chunksize

    @property
    def n_partition_keys(self):
        return 1

    @property
    def n_sort_keys(self):
        return 0

    @abc.abstractmethod
    def mapper(self, item):
        """Apply keys to each input item."""
        raise NotImplementedError

    # @abc.abstractmethod
    # def combiner(self, key, values):
    #     """Mini reduce operation for the data from one map operation.
    #
    #     This method will be probed once with the call below to determine
    #     if the combiner is implemented, so it would be unwise to modify the
    #     class state from within the combiner, but this is a bad idea for a
    #     bunch of other reasons so just don't do it.
    #
    #         ``combiner(_ProbeCombiner, _ProbeCombiner)``
    #
    #     This will create one of the following situations:
    #
    #         1. A ``NotImplementedError`` is raised - the combiner is
    #            definitely not implemented.
    #         2. Nothing - the combiner is probably implemented as a generator.
    #         3. Any other exception is raised - the combiner is probably
    #            implemented, but is either not a generator or not functional,
    #            the former being more likely.
    #
    #     Ultimately the combine phase will fail with an exception if the
    #     ``combiner()`` is not properly implemented.  We just need to know
    #     upfront in order to determine the order of operations.
    #     """
    #
    #     raise NotImplementedError

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

    def init_reduce(self):
        """Called immediately before the reduce phase outside of any parallel
        context.
        """
        pass

    def close(self):
        """Only automatically called only when using the MapReduce task as a
        context manager.
        """
        pass
