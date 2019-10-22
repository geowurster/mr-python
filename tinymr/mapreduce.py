"""In-memory MapReduce.  Get weird."""


import abc
import sys

from tinymr._base import _MRInternal


class MapReduce(_MRInternal):

    """In-memory MapReduce.  Subclassers must implement ``mapper()`` and
    ``reducer()``.
    """

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

    @property
    def jobs(self):
        return getattr(self, '_mr_jobs', 1)

    @jobs.setter
    def jobs(self, value):
        self._mr_jobs = value

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

    def check_map_keys(self, keys):
        """Provides an opportunity to check the first set of keys
        produced by the map phase.
        """
        pass

    def check_reduce_keys(self, keys):
        """Provides an opportunity to check the first set of keys
        produced by the map phase.
        """
        pass

    def close(self):
        """Only automatically called only when using the MapReduce task as a
        context manager.

        Be sure to set ``self.closed = True`` when overriding this method,
        otherwise a task can be used twice.  The assumption is that using an
        instance of a task after  ``MapReduce.close()`` or
        ``MapReduce.__exit__()`` is called will raise an exception.
        """
        self.closed = True


# Required to make ``MemMapReduce._run_map()`` pickleable.
if sys.version_info.major == 2:  # pragma: no cover

    import copy_reg

    def _reduce_method(m):
        if m.im_self is None:
            return getattr, (m.im_class, m.im_func.func_name)
        else:
            return getattr, (m.im_self, m.im_func.func_name)

    copy_reg.pickle(type(MapReduce._run_map), _reduce_method)
