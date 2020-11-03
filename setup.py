"""Config for Pypi."""

from setuptools import setup, find_packages
from impresso_commons import __version__

VERSION = __version__


DESCRIPTION = "Python module with bits of code (objects, functions)\
    highly reusable within impresso."

setup(
    name='impresso_pycommons',
    author='Matteo Romanello, Maud Ehrmann',
    author_email='matteo.romanello@epfl.ch, maud.ehrmann@epfl.ch',
    url='https://github.com/impresso/impresso-pycommons',
    version=VERSION,
    packages=find_packages(),
    package_data={
        'impresso_commons': [
            'data/xmi/*.xml',
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
        'dkpro-cassis',
        'scikit-build',
        'cmake',
        'opencv-python==3.4.7.28',
        'numpy',
        'smart_open',
        'jsonlines',
        's3fs==0.4.2',
        'dask_k8'
    ]
)

