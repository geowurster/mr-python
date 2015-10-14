#!/usr/bin/env python


"""
Setup script for mr-python
"""


from itertools import chain
import os

from setuptools import find_packages
from setuptools import setup


with open('README.rst') as f:
    readme = f.read().strip()


version = None
author = None
email = None
source = None
with open(os.path.join('mrpython', '__init__.py')) as f:
    for line in f:
        if line.strip().startswith('__version__'):
            version = line.split('=')[1].strip().replace('"', '').replace("'", '')
        elif line.strip().startswith('__author__'):
            author = line.split('=')[1].strip().replace('"', '').replace("'", '')
        elif line.strip().startswith('__email__'):
            email = line.split('=')[1].strip().replace('"', '').replace("'", '')
        elif line.strip().startswith('__source__'):
            source = line.split('=')[1].strip().replace('"', '').replace("'", '')
        elif None not in (version, author, email, source):
            break


setup(
    name='mr-python',
    author=author,
    author_email=email,
    classifiers=[
        'Topic :: Utilities',
        'Intended Audience :: Developers',
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: BSD License',
        'Topic :: Text Processing',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: PyPy'
    ],
    description="Experimental Pythonic MapReduce.",
    include_package_data=True,
    install_requires=[
        'six'
    ],
    extras_require={
        'dev': [
            'pytest',
            'pytest-cov',
            'coveralls',
        ],
    },
    keywords='experimental map reduce mapreduce hadoop',
    license="New BSD",
    long_description=readme,
    packages=find_packages(exclude=['tests']),
    url=source,
    version=version,
    zip_safe=True
)
