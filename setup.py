"""Config for Pypi."""

from setuptools import setup, find_packages
from impresso_commons import __version__

VERSION = __version__


DESCRIPTION = "Python module with bits of code (objects, functions)\
    highly reusable within impresso."

setup(
    name='impresso_commons',
    author='Matteo Romanello, Maud Ehrmann',
    author_email='matteo.romanello@epfl.ch, maud.ehrmann@epfl.ch',
    url='https://github.com/impresso/impresso-pycommons',
    version=VERSION,
    packages=find_packages(),
    long_description=DESCRIPTION,
    install_requires=[
        'dask',
        'boto',
        'bs4'
    ]
    # install_requires=[]
)
