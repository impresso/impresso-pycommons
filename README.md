# impresso-pycommons
Python module with bits of code (objects, functions) highly reusable within impresso.

* `impresso_commons.path`: contains objects and functions related to parsing impresso's canonical directory structure.

* `impresso_commons.images`: contains objects and functions related to working with images such as conversions of image box coordinates, etc.

## Installation

With `pip`:

    pip install https://<github_user>:<github_pwd>@github.com/impresso/impresso-pycommons/archive/master.zip

With `pipenv`:

    pipenv install -e git+https://github.com/impresso/impresso-pycommons.git#egg=impresso_commons


## Usage

```python
>>> from impresso_commons.path import detect_issues
>>> issues = detect_issues("../impresso-text-acquisition/text_importer/data/sample_data/")
>>> print(issues)
```
