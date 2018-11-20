Text Rebuild
============

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
--------------------

A set of functions to transform JSON files in **impresso's canonical format**
into a number of JSON-based formats for different purposes.

.. autofunction:: impresso_commons.text.rebuilder.rebuild_for_solr
.. autofunction:: impresso_commons.text.rebuilder.rebuild_text


Helpers
-------

.. automodule:: impresso_commons.text.helpers
    :members:
