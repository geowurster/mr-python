"""``tinymr`` errors"""


class KeyCountError(KeyError):
    """The number of expected keys does not match the number of actual keys."""


class ClosedTaskError(ValueError):
    """Raised when attempting to use a closed MapReduce task twice."""
