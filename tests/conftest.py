"""``pytest`` fixtures."""


from collections import Counter
import os

import pytest


@pytest.fixture(scope='function')
def text():
    return os.linesep.join([
        "word something else",
        "else something word",
        "mr python could be cool 1"
    ])


@pytest.fixture(scope='function')
def text_word_count(text):
    words = text.lower().strip().split()
    return dict(Counter(words))
