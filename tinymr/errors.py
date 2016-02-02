"""
tinymr specific exceptions.
"""


class MRException(Exception):

    """
    Base exception for tinymr.
    """


class UnorderableKeys(MRException):

    """
    Encountered keys during a sort operation that could not be ordered.  This
    could mean that some keys are `str`, some are `int`, some are `None`, etc.
    """


class CombinerNotImplemented(MRException):

    """
    MapReduce task does not implement a `combiner()`.
    """


class ClosedTask(MRException):

    """
    Cannot re-use closed MapReduce tasks.
    """


# Instantiated exceptions to make sure we get a clear message
_UnorderableKeys = UnorderableKeys(UnorderableKeys.__doc__.strip())
_CombinerNotImplemented = CombinerNotImplemented(
    CombinerNotImplemented.__doc__.strip())
_ClosedTask = ClosedTask(ClosedTask.__doc__.strip())
