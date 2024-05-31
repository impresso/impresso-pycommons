Data Versioning
================================

The `versioning` package of `impresso_commons` contains several modules and scripts with classes and functions that allow to version Impresso's data at various stages of the processing pipeline.

The main goal of this approach is to version the data and track information at every stage to:

1. **Ensure data consisteny and ease of debugging:** Data elements should be consistent across stages, and inconsistencies/differences should be justifiable through the identification of data leakage points.

2. **Allow partial updates:** It should be possible to (re)run all or part of the processes on subsets of the data, knowing which version of the data was used at each step. This can be necessary when new media collections arrive, or when an existing collection has been patched.

3. **Ensure transparency:** Citation of the various data stages and datasets should be straightforward; users should know when using the interface exactly what versions they are using, and should be able to consult the precise statistics related to them.


Data Statistics and Newspaper Statistics
------------------------------------------

.. automodule:: impresso_commons.versioning.data_statistics
   :members:
   :undoc-members:
   :show-inheritance:

Data Manifest
--------------------------------------------

.. automodule:: impresso_commons.versioning.data_manifest
   :members:
   :undoc-members:
   :show-inheritance:

Versioning Helpers
--------------------------------------------

.. automodule:: impresso_commons.versioning.helpers
   :members:
   :undoc-members:
   :show-inheritance:

Manifest Computing Script
--------------------------------------------

.. automodule:: impresso_commons.versioning.compute_manifest
   :members:
   :undoc-members:
   :show-inheritance:

