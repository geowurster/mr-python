"""
Unittests for tinymr.serialize
"""


import os
import pickle

import pytest

from tinymr import serialize
from tinymr import tools


def test_str2type():
    values = [
        ('1', 1),
        ('0', 0),
        ('01', '01'),
        ('1.23', 1.23),
        ('01.23', '01.23'),
        ('words.dot', 'words.dot'),
        ('hey look at me', 'hey look at me'),
        ('none', None),
        ('true', True),
        ('false', False)]

    for v, e in values:
        assert serialize.str2type(v) == e


def test_pickle_roundtrip():
    data = [
        (1, 2),
        (3, 4)]

    serialized = list(serialize.dump_pickle(data))
    for item in serialized:
        assert isinstance(item, bytes)

    for expected, actual in zip(data, serialize.load_pickle(serialized)):
        assert expected == actual


def test_pickle_from_file(tmpdir):

    path = str(tmpdir.mkdir('test_pickle_from_file').join('data'))

    data = [
        (1, 2, None),
        (3, 4, tools.OrderableNone)]

    with open(path, 'wb+') as f:
        for line in serialize.dump_pickle(data):
            f.write(line)

        f.seek(0)

        for e, a in zip(data, serialize.load_pickle(f)):
            assert e == a


def test_text():
    data = [

        (1, 2, None),
        (3, 4, tools.OrderableNone)]

    expected = [
        '1\t2\tNone',
        '3\t4\tOrderableNone']

    for e, a in zip(expected, serialize.dump_text(data)):
        assert e == a


def test_text_roundtrip(tmpdir):

    path = str(tmpdir.mkdir('test_text_roundtrip').join('data'))

    data = [
        (1, 2, None),
        (3, 4, tools.OrderableNone)]

    out = serialize.dump_text(data)
    for e, a in zip(data, serialize.load_text(out)):
        assert e == a

    with open(path, 'w+') as f:
        for line in serialize.dump_text(data):
            f.write(line + os.linesep)

        f.seek(0)

        for e, a in zip(data, serialize.load_text(f)):
            assert e == a


def test_from_Pickler(tmpdir):

    path = str(tmpdir.mkdir('test_from_Pickler').join('data'))

    data = [
        (1, 2),
        (3, tools.OrderableNone)]

    with open(path, 'wb') as f:
        for item in serialize.dump_pickle(data):
            f.write(item)

    with open(path, 'rb') as f:
        loaded = list(serialize.load_pickle(f))
        assert len(loaded) > 1
        for e, a in zip(data, loaded):
            assert e == a


def test_dump_load_json():

    data = [
        {'key1': 'value1', 'key2': 'value2'},
        {'key3': 'value3', 'key4': 'value4'}]

    actual = serialize.dump_json(data)
    actual = serialize.load_json(actual)

    for e, a in zip(data, actual):
        assert e == a


@pytest.mark.parametrize('cls', [serialize.Pickle, serialize.Text])
def test_serialization_classes(cls, tmpdir):

    path = str(tmpdir.mkdir('test_serialization_classes').join('data'))

    data = [
        (1, 2),
        (3, 4)]

    slz = cls()

    assert isinstance(pickle.loads(pickle.dumps(slz)), type(slz))
    assert repr(slz).startswith(slz.__class__.__name__)

    with slz.open(path, 'w') as dst:
        for obj in data:
            dst.write(obj)
    assert dst._f.closed

    with slz.open(path) as src:
        for e, a in zip(data, src):
            assert e == a

    assert dst._f.closed

    if isinstance(cls, serialize.Pickle):
        with pytest.raises(ValueError):
            slz.open(path, 'a')


# def test_Text(tmpdir):
#
#     path = str(tmpdir.mkdir('test_Text').join('data'))
#
#     data = [
#         (1, 2, None),
#         ('hey', tools.OrderableNone, 1.23)]
#
#     slz = serialize.Text()
#
#     assert isinstance(pickle.loads(pickle.dumps(slz)), )
#     assert repr(slz).startswith('Text(')
#
#     with slz.open(path, 'w') as dst:
#         for obj in data:
#             dst.write(obj)
#
#     with slz.open(path) as src:
#         for e, a in zip(data, src):
#             assert e == a
