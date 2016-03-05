"""
Base classes.  Subclass away!
"""


import logging

from tinymr import errors


class BaseMapReduce(object):

    """
    Base class for various MapReduce implementations.  Not all methods are
    used by every implementation.
    """

    def mapper(self, item):
        raise NotImplementedError

    def combiner(self, key, values):

        """
        Not used by every MR implementation.  Receives the sorted output from
        a single `mapper()` and acts as an initial `reducer()` to cut the data
        volumn.

        See `reducer()` for more information.
        """

        raise errors.CombinerNotImplemented()

    def reducer(self, key, values):

        """
        Receives sorted data for a single key for processing and yields 2 or 3
        element tuples, depending on the sort strategy.

        Parameters
        ----------
        key : object
            The key for this group of data.
        values : iter
            Data to process.

        Yields
        ------
        tuple
            3 elements: `(partition, [sort], data)`.
        """

        raise NotImplementedError

    def init_reduce(self):

        """
        Called immediately prior to the reduce phase and gives the user an
        opportunity to make adjustments now that the entire dataset has been
        observed in the map phase.
        """

        pass

    def output(self, results):

        """
        Receives `(key, value)` pairs from each reducer.  The output of this
        method is handed back to the parent process, so do whatever you want!

        Default implementation is to yield a generator of `(key, values)` pairs
        where `key` is a reducer key and `values` is the output from that
        reducer as a tuple - this is always an iterable object but MAY be a
        generator.

        Properties
        ----------
        pairs : iter
            `(key, data)` pairs from each reducer.

        Returns
        -------
        object
        """

        return results

    @property
    def logger(self):

        """
        Each MapReduce task gets its own logger with a name like
        `tinymr-ClassName`.
        """

        return logging.getLogger('tinymr-{}'.format(self.__class__.__name__))

    @property
    def jobs(self):

        """
        Default number of jobs to run in parallel for each phase.

        Returns
        -------
        int
        """

        return 1

    @property
    def map_jobs(self):

        """
        Number of jobs to run in parallel during the map phase.  Defaults
        to `jobs` property.

        Returns
        -------
        int
        """

        return self.jobs

    @property
    def reduce_jobs(self):

        """
        Number of jobs to run in parallel during the reduce phase.  If `None`,
        defaults to `jobs` property.

        Returns
        -------
        int
        """

        return self.jobs

    @property
    def chunksize(self):

        """
        Amount of data to process in each `job`.

        Returns
        -------
        int
        """

        return 1

    @property
    def map_chunksize(self):

        """
        Amount of data to process in each `job` during the map phase.
        Defaults to `chunksize`.
        """

        return self.chunksize

    @property
    def reduce_chunksize(self):

        """
        Amount of data to process in each `job` during the reduce phase.
        Defaults to `chunksize`.
        """

        return self.chunksize

    @property
    def sort(self):

        """
        Disable all sorting.  Setting individual properties overrides
        this setting for individual phases.

        Returns
        -------
        bool
        """

        return True

    @property
    def sort_map(self):

        """
        Sort the output from the map phase before the combine phase.

        Returns
        -------
        bool
        """

        return self.sort

    @property
    def sort_combine(self):

        """
        Sort the output from the combine phase before the partition phase.

        Returns
        -------
        bool
        """

        return self.sort

    @property
    def sort_reduce(self):

        """
        Sort the reduced values before passing to `output()`.

        Returns
        -------
        bool
        """

        return self.sort

    @property
    def sort_output(self):

        """
        Pass data to `output()` sorted by key.

        Returns
        -------
        bool
        """

        return self.sort

    @property
    def _has_combiner(self):

        """
        Indicates whether a task has implemented a `combiner()`.
        """

        try:
            self.combiner(None, None)
            return True
        except errors.CombinerNotImplemented:
            return False

    def close(self):

        """
        Allows the user an opportunity to destroy any connections or data
        structures created in an `init` step.  See `__exit__` for more
        information.
        """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()
