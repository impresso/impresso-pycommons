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
>>> from impresso_commons.path import detect_issues
>>> issues = detect_issues("../impresso-text-acquisition/text_importer/data/sample_data/")
>>> print(issues)
```


## Development settings

**Version**

`3.6`

**Documentation**

Python docstring style https://pythonhosted.org/an_example_pypi_project/sphinx.html 

**Testing**

Python pytest framework: https://pypi.org/project/pytest/

Tox: https://tox.readthedocs.io/en/latest/

**Passing arguments**

Doctopt: http://docopt.org/

or

argparse: https://docs.python.org/3.6/howto/argparse.html

**Style**

4 space indentation 
