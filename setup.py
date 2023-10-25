"""Config for Pypi."""

import pathlib
from setuptools import setup, find_packages
from impresso_commons import __version__

VERSION = __version__

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()


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
            'data/config/*.json'
        ]
    },
    entry_points={
        'console_scripts': [
            'impresso-partitioner = impresso_commons.utils.daskutils:main',
            'impresso-rebuilder = impresso_commons.text.rebuilder:main'
        ]
    },
    python_requires='>=3.6',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],
    long_description=README,
    long_description_content_type='text/markdown',
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
        'numpy',
        'smart_open',
        'jsonlines',
        's3fs==0.4.2',
        'dask_k8'
    ]
)
