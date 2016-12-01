"""Tests for ``tinymr.base``."""


import pytest

from tinymr import base


def test_not_implemented_methods():

    mr = base.BaseMapReduce()
    with pytest.raises(NotImplementedError):
        mr.mapper(None)
    with pytest.raises(NotImplementedError):
        mr.reducer(None, None)


def test_default_methods():

    mr = base.BaseMapReduce()

    expected = [(i, tuple(range(i))) for i in range(1, 10)]
    assert list(mr.output(expected)) == expected

    assert mr._sort_key_idx is None

    with pytest.raises(NotImplementedError):
        mr(None)


def test_set_properties():

    """All of the MapReduce level configuration happens in properties, but
    since the entire ``MapReduce.__init__()`` is handed over to the user,
    each of these properties needs to have a getter _and_ setter.
    """

    # Collect all the public properties
    props = []
    for attr in dir(base.BaseMapReduce):
        obj = getattr(base.BaseMapReduce, attr)
        if not attr.startswith('_') and isinstance(obj, property):
            props.append(attr)

    mr = base.BaseMapReduce()
    for p in props:

        # Make sure the attribute isn't already set for some reason but
        # cache the value so it can be replaced once this property is tested
        original = getattr(mr, p)
        assert original != 'whatever', \
            "Property '{}' has already been set?".format(p)

        # This calls the setter
        try:
            setattr(mr, p, 'whatever')

        # The default error message isn't very helpful for debugging tests.
        except AttributeError:
            raise AttributeError(
                "Can't set property '{}' on '{}'".format(
                    p, mr.__class__.__name__))

        assert getattr(mr, p) == 'whatever'

        # Some properties default to other properties, for instance 'map_jobs'
        # and 'reduce_jobs' both default to 'jobs'.  If 'jobs' is tested
        # first then 'map_jobs' and 'reduce_jobs' will inherit its new
        # value.  By explicitly resetting the property's value to the
        # original state this test is a little more sensitive to human
        # errors, like if 'jobs.setter' actually points to 'chunksize'.
        setattr(mr, p, original)
