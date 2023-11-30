Text Rebuild
==============================

Command Line interface
----------------------

.. automodule:: impresso_commons.text.rebuilder


Config file example
-------------------

(from file: `cluster.json`)::


    [
        {
            "GDL": [1900, 1999]
        }
    ]


Rebuild functions
-----------------

A set of functions to transform JSON files in **impresso's canonical format**
into a number of JSON-based formats for different purposes.
.. automodule:: impresso_commons.text.rebuilder
   :members:
   :undoc-members:
   :show-inheritance:

Helpers
-------

.. automodule:: impresso_commons.text.helpers
   :members:
   :undoc-members:
   :show-inheritance:

Running Rebuild on Runai
------------------------

.. include:: ../scripts/rebuild_on_runai.md
    :parser: myst_parser.sphinx_