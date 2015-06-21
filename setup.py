#!/usr/bin/env python

import versioneer
from setuptools import setup

long_description = """\

aiopg8000
------

**NOTE:** **[aiopg8000](https://github.com/realazthat/aiopg8000)** is a fork of \
**[pg8000](https://github.com/mfenniak/pg8000)** to support asyncio.

[pg8000](https://github.com/mfenniak/pg8000) is a Pure-Python interface to the PostgreSQL database engine.  It is \
one of many PostgreSQL interfaces for the Python programming language. pg8000 \
is somewhat distinctive in that it is written entirely in Python and does not \
rely on any external libraries (such as a compiled python module, or \
PostgreSQL's libpq library). pg8000 supports the standard Python DB-API \
version 2.0.

pg8000's name comes from the belief that it is probably about the 8000th \
PostgreSQL interface for Python."""

cmdclass = dict(versioneer.get_cmdclass())
version = versioneer.get_version()

try:
    from sphinx.setup_command import BuildDoc
    cmdclass['build_sphinx'] = BuildDoc
except ImportError:
    pass

setup(
    name="aiopg8000",
    version=version,
    cmdclass=cmdclass,
    description="PostgreSQL interface library, for asyncio",
    long_description=long_description,
    author="Mathieu Fenniak, Azriel Fasten",
    author_email="biziqe@mathieu.fenniak.net, azriel.fasten@gmail.com",
    url="https://github.com/realazthat/aiopg8000",
    license="BSD",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: Implementation",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: Jython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Operating System :: OS Independent",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords="postgresql dbapi asyncio",
    packages=("aiopg8000",),
    command_options={
        'build_sphinx': {
            'version': ('setup.py', version),
            'release': ('setup.py', version)}},
)
