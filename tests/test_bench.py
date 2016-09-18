"""Tests for ``tinymr.bench``."""


import datetime
import time

import pytest

from tinymr import bench


def test_timer():

    wait = 0.2
    tolerance = 0.006
    start = datetime.datetime.utcnow()

    with bench.timer() as elapsed:
        time.sleep(wait)
        mid = elapsed.delta

    end = datetime.datetime.utcnow()

    assert (elapsed.seconds - wait) < tolerance
    assert (start - elapsed.start).total_seconds() < tolerance
    assert (end - elapsed.stop).total_seconds() < tolerance
    assert elapsed.seconds - mid.total_seconds() < tolerance


def test_timer_exceptions():
    with pytest.raises(bench.TimerNotStopped):
        with bench.timer() as elapsed:
            elapsed.stop


def test_timer_no_context_manager():
    elapsed = bench.timer()
    with pytest.raises(bench.TimerNotStarted):
        elapsed.start
    with pytest.raises(bench.TimerNotStopped):
        elapsed.stop
    with pytest.raises(bench.TimerNotStarted):
        elapsed.stop_timer()


def test_random_keys():
    expected = [
        (1, 1, 1),
        (1, 1, 1),
        (1, 1, 1)]
    actual = bench.random_keys(3, 3, lambda: 1)
    assert expected == list(actual)
