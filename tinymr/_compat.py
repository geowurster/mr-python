"""Python 2 ... I guess ..."""


import itertools as it
import sys


if sys.version_info.major == 2:  # pragma: no cover
    zip = it.izip
    map = it.imap
else:  # pragma: no cover
    zip = zip
    map = map
