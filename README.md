# impresso-pycommons
Python module with bits of code (objects, functions) highly reusable within impresso.

* `impresso_commons.path`: contains objects and functions related to parsing impresso's canonical directory structure.

* `impresso_commons.images`: contains objects and functions related to working with images such as conversions of image box coordinates, etc.

## Installation

With `pip`:

    pip install https://<github_user>:<github_pwd>@github.com/impresso/impresso-pycommons/archive/master.zip

With `pipenv`:

    pipenv install -e git+https://github.com/impresso/impresso-pycommons.git#egg=impresso_commons

With `pipenv` for a specific branch:

    pipenv install -e git+ssh://git@github.com/impresso/impresso-pycommons.git@mybranch#egg=impresso_commons


## Usage

```python
>>> from impresso_commons.path.path_fs import detect_issues
>>> issues = detect_issues("../impresso-text-acquisition/text_importer/data/sample_data/")
>>> print(issues)
```


## Development settings

**Version**

`3.6`

**Documentation**

Python docstring style https://pythonhosted.org/an_example_pypi_project/sphinx.html

Sphinx configuration file (`docs/conf.py`) generated with:

    sphinx-quickstart --ext-githubpages

To compile the documentation

```bash
cd docs/
make html
```

To view locally:

Install `http-sever` (a node-js package):

    npm install http-server -g

Then:

    cd docs
    http-server

And you'll be able to browse it at <http://127.0.0.1:8080>.



**Testing**

Python pytest framework: https://pypi.org/project/pytest/

Tox: https://tox.readthedocs.io/en/latest/

**Passing arguments**

Doctopt: http://docopt.org/

or

argparse: https://docs.python.org/3.6/howto/argparse.html

**Style**

4 space indentation
