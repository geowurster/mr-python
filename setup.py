#!/usr/bin/env python3


"""
Setup script for tinymr
"""


import os

from setuptools import find_packages
from setuptools import setup


with open('README.rst') as f:
    readme = f.read().strip()


with open(os.path.join('tinymr', '__init__.py')) as f:
    for line in f:
        if '__version__' in line:
            version = line.split("=")[1].strip()
            version = version.strip('"')
            version = version.strip("'")
            break
    else:
        raise RuntimeError("Could not find '__version__'")


extras_require = {
    'test': [
        'pytest>=3',
        'pytest-cov',
    ],
}


setup(
    name='tinymr',
    author="Kevin Wurster",
    author_email="wursterk@gmail.com",
    classifiers=[
        'Intended Audience :: Developers',
        'Development Status :: 7 - Inactive',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3'
    ],
    description="Pythonic in-memory MapReduce.",
    include_package_data=True,
    extras_require=extras_require,
    keywords='experimental memory map reduce mapreduce',
    license="New BSD",
    long_description=readme,
    packages=find_packages(exclude=['tests']),
    url="https://github.com/geowurster/tinymr",
    version=version,
    zip_safe=True
)
