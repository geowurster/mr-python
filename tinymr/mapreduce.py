"""In-memory MapReduce. Get weird.

See :obj:`MapReduce` for an example.
"""


import abc
from collections import defaultdict
from inspect import isgeneratorfunction
import itertools as it
from functools import partial
import operator as op
import sys

from .exceptions import ElementCountError


__all__ = ["MapReduce"]


class MapReduce(object):

    """Subclass this base class and implement :meth:`mapper` and :meth:`reducer`
    methods to produce an object that can run a map reduce task.

    Output can be customized by overriding :meth:`output`, and sorting can
    be controlled via these properties: :attr:`sort_map_with_value`,
    :attr:`sort_map_reverse`, :attr:`sort_reduce_with_value`, and
    :attr:`sort_reduce_reverse`.

    Subclassers are also given complete control over :meth:`__init__`, and can
    implement a context manager with :meth:`__enter__` and :meth:`__exit__`.

    See :meth:`__call__` for how to execute the :meth:`mapper` and/or the
    :meth:`reducer` concurrently.

    Example Word Count Task
    -----------------------

    This is not necessarily the fastest or best way to count words, but it
    is the easiest to read. The ``mapper()`` takes a line of text, split it
    into words, and emits tuples where the first element is the word and the
    second is a 1. The ``reducer()`` receives the word and a bunch of 1's, one
    for each instance of the word across all of the input text. The
    ``reducer()`` counts the 1's and emits a tuple where the first element
    is the word and the second is a count of all instances of that word across
    the entire input text.

        from tinymr import MapReduce

        class WordCount(MapReduce):

            def mapper(self, item):
                line = item.lower()
                for word in line.split():
                    yield word, 1

            def reducer(self, key, values):
                return word, sum(values)

    The task can be invoked like this:

        with WordCount() as mr, open('data.txt') as f:
            results = mr(f)

    The output of this task is a dictionary mapping keys to values (the default
    implementation) and would look something like:

        {
            "word": 345,
            "the": 4,
            "another": 71
        }

    See :meth:`mapper` and :meth:`reducer` for information about how to sort
    data and :meth:`output` for information about how to customize what the
    task returns.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def mapper(self, item):

        """Receives a single item from the input stream and produces one or
        more output ``tuple``s with 2 or 3 elements. The first element is
        always used for partitioning and the last is always passed to the
        ``reducer()``:

            (partition, value)

        but if the middle element is present then the data is sorted according
        to that value prior to being passed to ``reducer()``.

            (partition, sort, value)

        All elements emitted by the ``mapper()`` must be of the same length,
        however for performance reasons only the first is checked.

        This mapper looks for lines in a file containing the word
        ``fox`` and passes those lines to the ``reducer()`` in sorted order.

            def mapper(self, path):

                with open(path) as f:

                    for idx, line in enumerate(f):
                        line = line.lower()

                        if "fox" in line:
                            yield item, idx, line

        A ``mapper()`` can either ``return`` a single value or ``yield`` many.
        This ``mapper()`` just indicates if the word "fox" appears in a text
        file:

            def mapper(self, path):

                with open(path) in f:
                    text = f.read()
                    contains_fox = "fox" in text

                    return item, contains_fox

        and this ``mapper()`` does the same but by streaming the file and
        checking each line:

            def mapper(self, item):

                with open(item) as f:
                    for line in f:
                        if "fox" in line.lower():
                            yield item, 1
                            break

        Normally only the 2nd key enables sorting, but the value itself can
        be integrated into sorting with the :attr:`sort_map_with_value`
        attribute.

        If ``mapper()`` emits 3 elements and :attr:`sort_with_map_value` is
        enabled, then the results with be sorted based on the sort element AND
        the value element. Results can be sorted in reverse with
        :attr:`sort_map_reverse`, which can be configured similarly to
        :attr:`sort_map_with_value`.

        Parameters
        ----------
        item : object
            A single item from the input data stream.

        Returns
        -------
        A ``tuple`` containing 2 or 3 elements. Can also ``yield`` multiple
        ``tuple``s.
        """

    @abc.abstractmethod
    def reducer(self, key, values):

        """Receives values corresponding to a single partition. May or may
        not be sorted depending on the ``mapper()`` implementation and
        :attr:`sort_map_with_value`.

        Outputs a ``tuple`` with 2 or 3 keys that are subjected to the same
        sorting rules as :meth:`mapper`.

        Like :meth:`mapper`, ``reducer()`` can ``return`` a single value or
        ``yield`` multiple. For :meth:`mapper` this has no impact aside from
        making some implementations easier, but for ``reducer()`` this impacts
        how the data is passed on to :meth:`output`. Using the word count
        example, this ``reducer()`` returns a single value:

            def reducer(self, key, values):
                return key, sum(values)

        whereas this ``reducer()`` yields a single value:

            def reducer(self, key, values):
                yield key, sum(values)

        The difference is that for the former :meth:`output` receives a
        dictionary that looks like:

            {
                "word": 345,
                "the": 4,
                "another": 71
            }

        however for the latter :meth:`output` receives this:

            {
                "word": [345],
                "the": [4],
                "another": [71]
            }

        The difference is that ``yield``ing values gives :meth:`output` a list
        of values for each key. A ``reducer()`` that ``return``s a single value
        only has an output key containing a single value, however one that
        ``yield``s multiple values produces an output key containing multiple
        values.

        Output from ``reducer()`` can be sorted similar to :meth:`mapper`
        before being passed to :meth:`output` based on on the number of
        elements, :attr:`sort_reduce_with_value`, and :attr:`sort_reduve_reverse`.

        Parameters
        ----------
        key : object
            The partition key, which is the first element in the output from
            :meth:`mapper`.
        values : list
            List of all values emitted by :meth:`mapper`. May or may not be
            sorted. See :meth:`mapper` for information about sorting.

        Returns
        -------
        A ``tuple`` with 2 or 3 elements. Can also ``yield`` multiple
        ``tuple``s.
        """

    def output(self, mapping):

        """Catch and optionally modify task output before returning to caller.

        Parameters
        ----------
        mapping : dict
            A mapping between the first element produced by each :meth:`reducer`
            call and its corresponding values. See :meth:`reducer` for an
            explanation about when the dictionary values can be a ``list``.

        Returns
        -------
        Anything! The default implementation just passes on the input ``dict``
        unaltered.
        """

        return mapping

    @property
    def sort_map_with_value(self):

        """Part of controlling :meth:`mapper` sorting behavior. If
        :meth:`mapper`'s output does not include a sort element then this
        flag causes the sort phase to sort on the actual value. If
        :meth:`mapper`'s output does include a sort element then the sort phase
        sorts on that element and the actual value.

        Returns
        -------
        bool
        """

        return False

    @property
    def sort_map_reverse(self):

        """Indicates if the output of :meth:`mapper` should be sorted
        descending instead of ascending. Ignored if not sorting. See
        :meth:`mapper` for more information.

        Returns
        -------
        bool
        """

        return False

    @property
    def sort_reduce_with_value(self):

        """Like :attr:`sort_map_with_value` but for the output of
        :meth:`reducer`. See :meth:`mapper` and :meth:`reducer` for
        more information.

        Returns
        -------
        bool
        """

        return False

    @property
    def sort_reduce_reverse(self):

        """Like :attr:`sort_map_reverse` but for the output of :meth:`reducer`.
        See :meth:`mapper`, :meth:`reducer`, and :meth:`sort_map_reverse` for
        more information.

        Returns
        -------
        bool
        """

        return False

    def close(self):

        """A ``MapReduce`` task can also be structured as a context manager.
        This is part of the default implementation.
        """

    def __enter__(self):
        return self
    __enter__.__doc__ = close.__doc__

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    __exit__.__doc__ = close.__doc__

    def __partition_and_sort(
            self, sequence, sort_with_value, reverse):

        """Given the output from :meth:`mapper` or :meth:`reducer`, partition,
        sort if necessary, remove any data that was only used for sorting.

        Parameters
        ----------
        sequence : iterable
            Of ``tuple``s. Output from :meth:`mapper` or :meth:`reducer`.
        sort_with_value : bool
            Indicates if data should be sorted based on the value element in
            addition to any sort elements that may be present.
        reverse : bool
            Indicates if data should be sorted descending instead of ascending.

        Returns
        -------
        dict
            Where keys are partitions and values are ready to be passed to
            :meth:`reduce` or :meth:`output`. All extra sorting information
            has been removed.
        """

        sequence = iter(sequence)
        first = next(sequence)
        sequence = it.chain([first], sequence)

        if len(first) not in (2, 3):
            raise ElementCountError(
                "Expected data of size 2 or 3, not {}. Example: {}".format(
                    len(first), first))

        has_sort_element = len(first) == 3
        need_sort = has_sort_element or sort_with_value

        if has_sort_element:
            sequence = map(op.itemgetter(0, slice(1, 3)), sequence)

        if not need_sort:
            getval = None
            sortkey = None

        elif not has_sort_element and sort_with_value:
            getval = lambda x: x
            sortkey = None

        else:
            getval = op.itemgetter(1)
            if sort_with_value:
                sortkey = None
            else:
                sortkey = op.itemgetter(0)

        partitioned = defaultdict(list)
        for ptn, vals in sequence:
            partitioned[ptn].append(vals)

        if need_sort:
            partitioned = {
                p: (v.sort(key=sortkey, reverse=reverse), list(map(getval, v)))[1]
                for p, v in iteritems(partitioned)
            }

        return partitioned

    def __call__(self, sequence, mapper_map=None, reducer_map=None):

        """Given a sequence of input data, execute the map reduce task in
        several phases:

            1. Map (:meth:`mapper`).
            2. Partition and optionally sort.
            3. Reduce (:meth:`reducer()`).
            4. Partition and optionally sort.
            5. Construct output (:meth:`output`).

        Optionally the map and/or reduce phases can be executed concurrently
        by passing a parallelized ``map()`` function to ``mapper_map`` and
        ``reducer_map``. For example, this ``WordCount`` implementation
        runs eacn :meth:`mapper` in a separate thread but runs the reducer
        serially:

            from concurrent.futures import ThreadPoolExecutor

            class WordCount(MapReduce):

                def mapper(self, item):
                    with open(item) as f:
                        for line in f:
                            for word in line.split():
                                yield word, 1

                def reducer(self, key, values):
                    return key, sum(values)

            with WordCount() as mr, ThreadPoolExecutor(4) as pool:

                paths = ["file1.txt", "file2.txt"]

                results = mr(paths, mapper_map=pool.map)

        Passing the same function to ``reducer_map`` would cause each
        :meth:`reducer` to be executed in its own thread.

        Returns
        -------
        object
            See :meth:`output`.
        """

        # If 'mapper()' is a generator and it will be executed in some job
        # pool, wrap it in a function that expands the returned generator
        # so that the pool can serialize results and send back. Be sure to
        # wrap properly to preserve any docstring present on the method.
        mapper = self.mapper
        if mapper_map is not None and isgeneratorfunction(self.mapper):
            mapper = partial(_wrap_mapper, mapper=self.mapper)

        # Same as 'mapper()' but for 'reducer()'.
        reducer = self.reducer
        if reducer_map is not None:
            reducer = partial(_wrap_reducer, reducer=self.reducer)

        # Run map phase. If 'mapper()' is a generator flatten everything to
        # a single sequence.
        mapper_map = mapper_map or map
        mapped = mapper_map(mapper, sequence)
        if isgeneratorfunction(self.mapper):
            mapped = it.chain.from_iterable(mapped)

        # Partition and sort (if necessary).
        partitioned = self.__partition_and_sort(
            mapped,
            sort_with_value=self.sort_map_with_value,
            reverse=self.sort_map_reverse)

        # Run reducer. Be sure not to hold on to a pointer to the partitioned
        # dictionary. Instead replace it with a pointer to a generator.
        reducer_map = reducer_map or it.starmap
        partitioned = iteritems(partitioned)
        reduced = reducer_map(reducer, partitioned)

        # If reducer is a generator expand to a single sequence.
        if isgeneratorfunction(self.reducer):
            reduced = it.chain.from_iterable(reduced)

        # Partition and sort (if necessary).
        partitioned = self.__partition_and_sort(
            reduced,
            sort_with_value=self.sort_reduce_with_value,
            reverse=self.sort_reduce_reverse)

        # The reducer can yield several values or it can return a single value.
        # When the operating under the latter condition extract that value and
        # pass that on as the single output value.
        if not isgeneratorfunction(self.reducer):
            partitioned = {k: next(iter(v)) for k, v in iteritems(partitioned)}

        # Be sure not to pass a 'defualtdict()' as output.
        return self.output(dict(partitioned))


def _wrap_mapper(item, mapper):

    """Used in some cases when running concurrently. Expands generator
    produced by :meth:`MapReduce.mapper` so that results can be serialized
    and returned by a worker.

    Parameters
    ----------
    item : object
        See :meth:`MapReduce.mapper`.
    mapper : callable
        A :meth:`MapReduce.mapper`.

    Returns
    -------
    tuple
    """

    return tuple(mapper(item))


def _wrap_reducer(key_value, reducer):

    """Like :func:`_wrap_mapper` but for :meth:`MapReduce.reducer`.

    Parameters
    ----------
    key_value : tuple
        Arguments for :meth:`MapReduce.reducer`. First element is the key and
        second is values.
    reducer : callable
        A :meth:`MapReduce.reducer`.
    """

    return tuple(reducer(*key_value))


if sys.version_info.major == 2:

    import copy_reg

    map = it.imap

    def iteritems(d):
        return d.iteritems()

    # Required for pickling.
    def _reduce_method(m):
        if m.im_self is None:
            return getattr, (m.im_class, m.im_func.func_name)
        else:
            return getattr, (m.im_self, m.im_func.func_name)

    copy_reg.pickle(type(MapReduce.mapper), _reduce_method)

else:

    def iteritems(d):
        return d.items()
