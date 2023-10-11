import operator as op
import random

import pytest

from tinymr import MapReduce


@pytest.mark.parametrize("reverse", (True, False))
def test_sort_mapper_value(reverse):

    data = [2, 3, 1]
    expected = sorted(data, reverse=reverse)

    class SortMapValue(MapReduce):

        sort_map_with_value = True
        sort_map_reverse = reverse

        def mapper(self, item):
            return None, item

        def reducer(self, key, values):
            return key, values == expected

        def output(self, items):
            return items[None]

    mr = SortMapValue()
    assert mr(data) is True


@pytest.mark.parametrize("reverse", (True, False))
def test_sort_reducer_value(reverse):

    data = [2, 3, 1]
    expected = sorted(data, reverse=reverse)

    class SortReduceValue(MapReduce):

        sort_reduce_with_value = True
        sort_reduce_reverse = reverse

        def mapper(self, item):
            return None, item

        def reducer(self, key, values):
            # Verify data has not been sorted
            assert values == data, "Data has been sorted!"
            for i in values:
                yield key, i

        def output(self, items):
            return items[None] == expected

    mr = SortReduceValue()
    assert mr(data) is True


@pytest.mark.parametrize("reverse", (True, False))
def test_mapper_sort_element(reverse):

    data = [
        (3, 'a'),
        (2, 'b'),
        (1, 'c')
    ]
    expected = sorted(data, key=op.itemgetter(0), reverse=reverse)
    expected = [i[1] for i in expected]

    class SortMap(MapReduce):

        sort_map_reverse = reverse

        def mapper(self, item):
            idx, letter = item
            return None, idx, letter

        def reducer(self, key, values):
            return key, values == expected

        def output(self, items):
            return items[None]

    mr = SortMap()
    actual = mr(data)

    assert actual is True


@pytest.mark.parametrize("reverse", (True, False))
def test_reducer_sort_element(reverse):

    data = [
        (3, 'a'),
        (2, 'b'),
        (1, 'c')
    ]
    expected = sorted(data, key=op.itemgetter(0), reverse=reverse)
    expected = [i[1] for i in expected]

    class SortReduce(MapReduce):

        sort_reduce_reverse = reverse

        def mapper(self, item):
            return None, item

        def reducer(self, key, values):
            # Verify data has not been sorted
            assert values == data, "Data has been sorted!"
            for idx, letter in values:
                yield key, idx, letter

        def output(self, items):
            return items[None]

    mr = SortReduce()
    actual = mr(data)

    assert expected == actual


@pytest.mark.parametrize("reverse", (True, False))
def test_complex_sort(reverse):

    data = [
        (2018, 11, 7),
        (2018, 12, 21),
        (2019, 1, 2),
        (2019, 2, 25)
    ]

    expected = sorted(data, key=op.itemgetter(0, 1), reverse=reverse)
    expected = [i[2] for i in expected]

    class ComplexSort(MapReduce):

        sort_map_with_value = True
        sort_reduce_with_value = True
        sort_map_reverse = reverse
        sort_reduce_reverse = reverse

        def __init__(self):
            self.day_ym = {d: (y, m) for y, m, d in data}

        def mapper(self, item):
            year, month, day = item
            return None, (year, month), day

        def reducer(self, key, values):
            values = list(values)
            assert values == expected

            for day in values:
                year, month = self.day_ym[day]
                yield None, (year, month), day

        def output(self, items):
            return items[None]

    random.shuffle(data)

    mr = ComplexSort()
    actual = mr(data)

    assert expected == actual
