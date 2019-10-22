"""In-memory MapReduce.  Get weird."""


import abc

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
    def n_sort_keys(self):
        """Grab N keys after the partition keys when sorting."""
        return getattr(self, '_mr_n_sort_keys', 0)

    @n_sort_keys.setter
    def n_sort_keys(self, value):
        self._mr_n_sort_keys = value

    @property
    def closed(self):
        return getattr(self, '_mr_closed', False)

    @closed.setter
    def closed(self, value):
        self._mr_closed = value

    def close(self):
        """Only automatically called only when using the MapReduce task as a
        context manager.

        Be sure to set ``self.closed = True`` when overriding this method,
        otherwise a task can be used twice.  The assumption is that using an
        instance of a task after  ``MapReduce.close()`` or
        ``MapReduce.__exit__()`` is called will raise an exception.
        """
        self.closed = True
