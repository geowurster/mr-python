"""
Test fixtures
"""


from collections import OrderedDict
import os

import pytest


@pytest.fixture(scope='function')
def tiny_text():
    return os.linesep.join([
        "word something else",
        "else something word",
        "mr python could be cool 1"
    ])


@pytest.fixture(scope='function')
def tiny_text_wc_output():
    return OrderedDict((
        ('1', 1),
        ('be', 1),
        ('cool', 1),
        ('could', 1),
        ('else', 2),
        ('mr', 1),
        ('python', 1),
        ('something', 2),
        ('word', 2),
    ))
