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
    package_data={
        'impresso_commons': [
            'data/',
        ]
    },
    entry_points={
        'console_scripts': [
            'impresso-partitioner = impresso_commons.utils.daskutils:main',
            'impresso-rebuilder = impresso_commons.text.rebuilder:main'
        ]
    },
    long_description=DESCRIPTION,
    install_requires=[
        'dask[complete]',
        'distributed',
        'boto',
        'boto3',
        'bs4',
        'docopt',
        'deprecated',
        #'DKPro_PyCAS',
        'opencv-python>=3.4,<4',
        'numpy',
        'smart_open',
        'jsonlines',
        's3fs'
    ]
)

# TODO: add dkpro-pycas
