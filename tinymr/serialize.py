"""
Serialization and de-serialization keys.
"""


try:
    import cPickle as _pickle
except ImportError:
    import pickle as _pickle


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

        # It's really just a string
        else:
            return string


def pickle(stream, *args, **kwargs):

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
    bytes
    """

    for key in stream:
        yield _pickle.dumps(key, *args, **kwargs)


def unpickle(stream):

    """
    Unpickle a file or stream of data.  Primarily used to de-serialize keys.

    Parameters
    ----------
    stream : file or iter
        If a `file` is given it is un-pickled with `pickle.Unpickler()`.  If
        an iterator is given each item is passed through `pickle.loads()`.

    Yields
    ------
    object
    """

    if isinstance(stream, file):
        up = _pickle.Unpickler(stream)
        while True:
            try:
                yield up.load()
            except EOFError:
                break
    else:
        for key in stream:
            yield _pickle.loads(key)


def text(stream, delimiter='\t', serializer=str):

    """
    Convert keys to delimited text.

    Example:

        >>> from tinymr.serialize import text
        >>> keys = [
        ...     ('partition', 'sort', 'data')
        ...     (1, 1.23, None)]
        >>> print(list(text(keys)))
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

    Parameters
    ----------
    stream
    """

    pass