"""Base classes.  Subclass away!"""


import abc
from multiprocessing.dummy import Pool as DummyPool
from multiprocessing.pool import Pool
import operator as op


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
    def _ptn_key_idx(self):
        if self.n_partition_keys == 1:
            return 0
        else:
            return slice(0, self.n_partition_keys)

    @property
    def _sort_key_idx(self):
        # Ensure a lack of sort keys is properly handled down the line by
        # letting something fail spectacularly
        if self.n_sort_keys == 0:
            return None
        elif self.n_sort_keys == 1:
            # Given keys like: ('partition', 'sort', 'data')
            # the number of partition keys equals the index of the single
            # sort key
            return self.n_partition_keys
        else:
            start = self.n_partition_keys
            stop = start + self.n_sort_keys
            return slice(start, stop)

    @property
    def _map_key_grouper(self):
        getter_args = [self._ptn_key_idx, -1]
        if self.n_sort_keys > 0:
            getter_args.insert(1, self._sort_key_idx)
        return op.itemgetter(*getter_args)

    @property
    def _map_job_pool(self):
        if self.threaded_map:
            return DummyPool(self.map_jobs)
        else:
            return Pool(self.map_jobs)

    @property
    def _reduce_job_pool(self):
        if self.threaded_reduce:
            return DummyPool(self.reduce_jobs)
        else:
            return Pool(self.reduce_jobs)


class BaseMapReduce(_MRInternal):

    """Base class for various MapReduce implementations."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def jobs(self):
        return getattr(self, '_mr_jobs', 1)

    @jobs.setter
    def jobs(self, value):
        self._mr_jobs = value

    @property
    def map_jobs(self):
        return getattr(self, '_mr_map_jobs', self.jobs)

    @map_jobs.setter
    def map_jobs(self, value):
        self._mr_map_jobs = value

    @property
    def reduce_jobs(self):
        return getattr(self, '_mr_reduce_jobs', self.jobs)

    @reduce_jobs.setter
    def reduce_jobs(self, value):
        self._mr_reduce_jobs = value

    @property
    def chunksize(self):
        """Default chunksize for map and reduce phases.  See ``map_chunksize``
        and ``reduce_chunksize``.
        """
        return getattr(self, '_mr_chunksize', 1)

    @chunksize.setter
    def chunksize(self, value):
        self._mr_chunksize = value

    @property
    def map_chunksize(self):
        """Pass items in groups of N to each map job when running with
        running with multiple jobs.
        """
        return getattr(self, '_mr_map_chunksize', self.chunksize)

    @map_chunksize.setter
    def map_chunksize(self, value):
        self._mr_map_chunksize = value

    @property
    def reduce_chunksize(self):
        """Pass items in groups of N to each reduce job when running with
        multiple jobs.
        """
        return getattr(self, '_mr_reduce_chunksize', self.chunksize)

    @reduce_chunksize.setter
    def reduce_chunksize(self, value):
        self._mr_reduce_chunksize = value

    @property
    def n_partition_keys(self):
        """Grab the first N keys for partitioning."""
        return getattr(self, '_mr_n_partition_keys', 1)

    @n_partition_keys.setter
    def n_partition_keys(self, value):
        self._mr_n_partition_keys = value

    @property
    def n_sort_keys(self):
        """Grab N keys after the partition keys when sorting."""
        return getattr(self, '_mr_n_sort_keys', 0)

    @n_sort_keys.setter
    def n_sort_keys(self, value):
        self._mr_n_sort_keys = value

    @property
    def threaded(self):
        """Use threads instead of processes when running multiple jobs."""
        return getattr(self, '_mr_threaded', False)

    @threaded.setter
    def threaded(self, value):
        self._mr_threaded = value

    @property
    def threaded_map(self):
        """When running multiple jobs, use threads for the map phase instead
        of processes.
        """
        return getattr(self, '_mr_threaded_map', self.threaded)

    @threaded_map.setter
    def threaded_map(self, value):
        self._mr_threaded_map = value

    @property
    def threaded_reduce(self):
        """When running multiple jobs, use threads for the reduce phase
        instead of processes.
        """
        return getattr(self, '_mr_threaded_reduce', self.threaded)

    @threaded_reduce.setter
    def threaded_reduce(self, value):
        self._mr_threaded_reduce = value

    @property
    def closed(self):
        return getattr(self, '_mr_closed', False)

    @closed.setter
    def closed(self, value):
        self._mr_closed = value

    def init_map(self):
        """Called immediately before the map phase."""
        pass

    @abc.abstractmethod
    def mapper(self, item):
        """Apply keys to each input item."""
        raise NotImplementedError

    def check_map_keys(self, keys):
        """Provides an opportunity to check the first set of keys
        produced by the map phase.
        """
        pass

    def init_reduce(self):
        """Called immediately before the reduce phase."""
        pass

    @abc.abstractmethod
    def reducer(self, key, values):
        """Process the data for a single key."""
        raise NotImplementedError

    def check_reduce_keys(self, keys):
        """Provides an opportunity to check the first set of keys
        produced by the map phase.
        """
        pass

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

    def close(self):
        """Only automatically called only when using the MapReduce task as a
        context manager.

        Be sure to set ``self.closed = True`` when overriding this method,
        otherwise a task can be used twice.  The assumption is that using an
        instance of a task after  ``MapReduce.close()`` or
        ``MapReduce.__exit__()`` is called will raise an exception.
        """
        self.closed = True
