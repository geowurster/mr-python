"""Begrudgingly support Python 2"""


import itertools as it
import sys


if sys.version_info.major == 2:
    zip = it.izip
    map = it.imap
else:
    zip = zip
    map = map
