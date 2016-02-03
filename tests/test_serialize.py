"""
Unittests for tinymr.serialize
"""


from tinymr import serialize


def test_str2type():
    values = [
        ('1', 1),
        ('0', 0),
        ('01', '01'),
        ('1.23', 1.23),
        ('01.23', '01.23'),
        ('none', None),
        ('true', True),
        ('false', False)]

    for v, e in values:
        assert serialize.str2type(v) == e
