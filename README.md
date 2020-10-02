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

## License

The 'impresso - Media Monitoring of the Past' project is funded by the Swiss National Science Foundation (SNSF) under  grant number [CRSII5_173719 (Sinergia program)](http://p3.snf.ch/project-173719). The project aims at developing tools to process and explore large-scale collections of historical newspapers, and at studying the impact of this new tooling on historical research practices. More information at https://impresso-project.ch.
Copyright (C) 2020  The impresso team (contributors to this program: Matteo Romanello, Maud Ehrmann, Alex Flückinger, Edoardo Tarek Hölzl).

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.
