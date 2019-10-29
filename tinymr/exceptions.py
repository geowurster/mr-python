"""Custom exceptions."""


__all__ = ["ElementCountError"]


class ElementCountError(Exception):

    """The number of expected elements does not match the number of actual
     elements.
     """
