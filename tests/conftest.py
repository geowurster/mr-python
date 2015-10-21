"""
Test fixtures
"""


import pytest


@pytest.fixture(scope='function')
def tiny_text():
    return [
        "word something else",
        "else something word",
        "mr python could be cool 1"
    ]


@pytest.fixture(scope='function')
def tiny_text_mr_output():
    return {
        'word': 2,
        'something': 2,
        'else': 2,
        'mr': 1,
        'python': 1,
        'could': 1,
        'be': 1,
        'cool': 1,
        '1': 1
    }
