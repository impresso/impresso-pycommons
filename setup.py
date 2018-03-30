"""Config for Pypi."""

import os
from setuptools import setup, find_packages

VERSION = "0.1.2"


DESCRIPTION = "Python module with bits of code (objects, functions)\
    highly reusable within impresso."

setup(
    name='impresso_commons',
    author='Matteo Romanello, Maud Ehrmann',
    author_email='matteo.romanello@epfl.ch, maud.ehrmann@epfl.ch',
    url='https://github.com/impresso/impresso-pycommons',
    version=VERSION,
    packages=find_packages(),
    long_description=DESCRIPTION, install_requires=['dask']
    # install_requires=[]
)
