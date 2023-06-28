"""In-memory MapReduce. Get weird.

See :obj:`tinymr.MapReduce` for an example.
"""


from __future__ import absolute_import

from .mapreduce import MapReduce
from .tools import slicer


__all__ = ["MapReduce", "slicer"]


__version__ = "0.2"
