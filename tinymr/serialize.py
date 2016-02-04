"""
Serialization and de-serialization keys.

This module is pretty rough, but the objects are primarily used internally.
Either way, don't get to attached.
"""


try:
    import cPickle as pickle
except ImportError:
    import pickle

import json
import os

from tinymr import base
from tinymr import tools


def str2type(string):

    """
    Convert a string to its native Python type.
    """

    # String might be an integer
    if string.isdigit():

        # Don't convert strings with leading 0's to an int - they probably aren't ints
        if len(string) == 1 or (string[0] != '0' and len(string) > 1):
            return int(string)
        else:
            return string

    # String might be a float
    elif '.' in string:

        # But not if it starts with a 0
        if len(string) > 1 and string[0] != '0':
            try:
                return float(string)
            except ValueError:
                return string
        else:
            return string

    else:

        # None, True, or False?
        string_lower = string.lower()
        if string_lower == 'none':
            return None
        elif string_lower == 'true':
            return True
        elif string_lower == 'false':
            return False
        elif string_lower == 'orderablenone':
            return tools.OrderableNone

        # It's really just a string
        else:
            return string


def dump_pickle(stream, *args, **kwargs):

    """
    Pickle a stream of data one item at a time.  Primarily used to serialize
    keys.

    Parameters
    ----------
    stream : iter
        Data to process.  Usually tuples.
    args : *args, optional
        *args for `pickle.dumps()`.
    kwargs : **kwargs, optional
        **kwargs for `pickle.dumps()`.

    Yields
    ------
    str
    """

    for key in stream:
        yield pickle.dumps(key, *args, **kwargs)


def load_pickle(stream, **kwargs):

    """
    Unpickle a file or stream of data.  Primarily used to de-serialize keys.

    Parameters
    ----------
    stream : file or iter
        If a `file` is given it is un-pickled with `pickle.Unpickler()`.  If
        an iterator is given each item is passed through `pickle.loads()`.
    kwargs : **kwargs, optional
        Not used - included to homogenize the serialization API.

    Yields
    ------
    object
    """

    if hasattr(stream, 'read') and 'b' in getattr(stream, 'mode', ''):
        up = pickle.Unpickler(stream)
        while True:
            try:
                yield up.load()
            except EOFError:
                break
    else:
        for key in stream:
            yield pickle.loads(key)


def dump_text(stream, delimiter='\t', serializer=str):

    """
    Convert keys to delimited text.

        >>> from tinymr.serialize import dump_text
        >>> keys = [
        ...     ('partition', 'sort', 'data')
        ...     (1, 1.23, None)]
        >>> print(list(dump_ext(keys)))
        [
            'partition\tsort\tdata\t',
            '1\t1.23\tNone'
        ]

    Parameters
    ----------
    stream : iter
        One set of keys per iteration.
    delimiter : str, optional
        Output text delimiter.
    serializer : function, optional
        Function to serialize each key to a string.

    Yields
    ------
    str
    """

    for keys in stream:
        yield delimiter.join((serializer(k) for k in keys))


def load_text(stream, delimiter='\t', deserializer=str2type):

    """
    Parse delimited text to a stream of key tuples.

        >>> import os
        >>> from tinymr.serialize import load_text
        >>> text = os.linesep.join([
        ...     'partition\tsort\tdata\t',
        ...     '1\t1.23\tNone'])
        >>> keys = [
        ...     ('partition', 'sort', 'data')
        ...     (1, 1.23, None)]
        >>> print(list(load_ext(text)))
        [
            ('partition', 'sort', 'data',
            ('1', '1.23', None)
        ]

    Parameters
    ----------
    stream
    """

    for line in stream:
        yield tuple((deserializer(k) for k in line.strip().split(delimiter)))


def load_json(stream, json_lib=json, **kwargs):

    """
    Load a series of JSON encoded strings.

    Parameters
    ----------
    stream : iter
        Strings to load.
    json_lib : module, optional
        JSON library to use for loading.  Must match the builtin `json`
        module's API.
    kwargs : **kwargs, optional
        Arguments for `json.loads()`.

    Yields
    ------
    dict or list
    """

    for item in stream:
        yield json_lib.loads(item, **kwargs)


def dump_json(stream, json_lib=json, **kwargs):

    """
    Dump objects to JSON.

    Parameters
    ----------
    stream : iter
        JSON encodable objects.
    json_lib : module, optional
        JSON library to use for loading.  Must match the builtin `json`
        module's API.
    kwargs : **kwargs, optional
        Arguments for `json.dumps()`.

    Yields
    ------
    str
    """

    for item in stream:
        yield json_lib.dumps(item, **kwargs)


class Pickle(base.BaseSerializer):

    """
    Serializer for reading/writing data with `pickle`.  Mostly for internal
    use until the API stabilizes.
    """

    @property
    def _read_mode(self):
        return 'rb'

    @property
    def _write_mode(self):
        return 'wb'

    @property
    def _loader(self):
        return load_pickle

    @property
    def _dumper(self):
        return dump_pickle

    def _writefile(self, stream, path, mode=None, **kwargs):

        """
        Write data to disk with `pickle`.
        """

        mode = mode or self._write_mode
        with open(path, mode) as f:
            for item in self._dumper(stream):
                f.write(item)

        return path


class Text(base.BaseSerializer):

    """
    Serializer for reading/writing delimited text.  Mostly for internal use
    until the API stabilizes.
    """

    @property
    def _loader(self):
        return load_text

    @property
    def _dumper(self):
        return dump_text

    def _writefile(self, stream, path, mode=None, **kwargs):

        """
        Write a data stream to disk as delimited text.
        """

        mode = mode or self._write_mode
        with open(path, mode) as f:
            for item in self._dumper(stream, **kwargs):
                f.write(item + os.linesep)

        return path


class NewlineJSON(base.BaseSerializer):

    @property
    def _dumper(self):
        return dump_json

    @property
    def _loader(self):
        return load_json

    def _readfile(self, path, mode=None, **kwargs):
        mode = mode or self._read_mode
        with open(path, mode) as f:
            for item in self._loader(f, **kwargs):
                yield item

    def _writefile(self, stream, path, mode=None, **kwargs):
        mode = mode or self._write_mode
        with open(path, mode) as f:
            for item in self._dumper(stream, **kwargs):
                f.write(item + os.linesep)
