"""Benchmarking tools."""


import datetime


class TimerError(Exception):
    """Base exceptions for ``timer()``."""


class TimerNotStarted(TimerError):
    """Raise when the start time is accessed but the timer was not started."""


class TimerNotStopped(TimerError):
    """Raise when the timer is still running but the end time is requested."""


class timer(object):
    
    """Context manager for determining ellapsed time.  All times are in UTC.
    Time tracking starts as soon as the context manager enters, or
    ``start_timer()`` is called, so be consider when and where to instantiate.
    """

    def __init__(self):
        self._start = None
        self._stop = None

    def __enter__(self):
        """Start the timer."""
        self.start_timer()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop the timer."""
        self.stop_timer()

    @property
    def start(self):
        """Start ``datetime.datetime`` in UTC."""
        s = self._start  # dot lookup isn't free
        if s is None:
            raise TimerNotStarted(
                "Timer session has not started.  Start time is ambiguous.")
        return s

    @property
    def stop(self):
        """Get the end ``datetime.datetime`` in UTC."""
        s = self._stop  # dot lookup isn't free
        if s is None:
            raise TimerNotStopped(
                "Timer session has not ended.  End time is ambiguous.")
        return s

    def start_timer(self):
        """Reset the last ``stop`` value and record a new start time."""
        self._stop = None
        self._start = self.now
        
    def stop_timer(self):

        """
        Stop timer and record end time, which can be accessed from the ``stop``
        property.

        Raises
        ------
        RuntimeError
            If this method is called before ``start_timer()``.
        """

        s = datetime.datetime.utcnow()
        if self._start is None:
            raise TimerNotStarted(
                "Invalid timer session.  Start time not recorded.")
        else:
            self._stop = s


    @property
    def now(self):

        """Current UTC time.

        Returns
        -------
        datetime.datetime
            ``datetime.datetime.utcnow()``.
        """

        return datetime.datetime.utcnow()

    @property
    def delta(self):

        """Returns the ``datetime.timedelta`` from ``start`` and ``stop``.  If
        timer is still running ``now`` is used as the end time.  Time is UTC.

        Returns
        -------
        datetime.timedelta
        """

        try:
            stop = self.stop
        except TimerNotStopped:
            stop = self.now
        return stop - self.start

    @property
    def seconds(self):

        """Number of seconds between ``start`` and ``stop`` times.  If timer is
        still running ``now()`` is used as the end time.  Time is UTC.

        Returns
        -------
        float
        """

        return self.delta.total_seconds()


def random_keys(total, n, func):
    """Generate a random set of test keys.

    For example:

        >>> import functools
        >>> import random
        >>> func = functools.partial(random.randint, 0, 100)
        >>> for keys in random_keys(5, 3, func):
        ...    print(keys)

    produces:

        (38, 18, 60)
        (54, 26, 52)
        (39, 89, 91)
        (56, 19, 62)
        (62, 14, 40)

    A total of 5 groups of keys, each with 3 values between 0 and 100.

    Parameters
    ----------
    total : int
        The total number of groups of keys.
    n : int
        The number of keys per group.
    func : callable
        Function to generate a random value.

    Yields
    ------
    tuple
        Of length ``n``.
    """

    for _ in range(total):
        yield tuple(func() for _ in range(n))
