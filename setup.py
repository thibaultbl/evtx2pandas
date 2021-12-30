import os
from setuptools import setup


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="evtx2pandas",
    version="0.0.4",
    author="Thibault Blanc",
    description=("Convert EVTX to pandas DataFrame."),
    license="GNU General Public License",
    keywords="evtx pandas Event logs",
    url="https://github.com/thibaultbl/evtx2pandas",
    packages=["evtx2pandas"],
    long_description=read('README.rst'),
    install_requires=['pytest>=6.2.5', 'pandas>=1.3.5', 'evtx>=0.6.11', 'dask>=2021.12.0'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)",
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Scientific/Engineering',
    ],
)