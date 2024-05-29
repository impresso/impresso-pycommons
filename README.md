# impresso_pycommons

[![Documentation Status](https://readthedocs.org/projects/impresso-pycommons/badge/?version=latest)](https://impresso-pycommons.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/impresso-commons.svg)](https://badge.fury.io/py/impresso-commons)
![PyPI - License](https://img.shields.io/pypi/l/impresso-commons)

Python module with bits of code (objects, functions) highly-reusable within the [impresso project](https://impresso-project.ch/).

Please refer to the [documentation](https://impresso-pycommons.readthedocs.io/) for further information on this library.

## Installation

With `pip`:

```bash
pip install impresso-commons
```

## Notes

The library supports configuration of s3 credentials via project-specific local .env files.

## License

The second project 'impresso - Media Monitoring of the Past II. Beyond Borders: Connecting Historical Newspapers and Radio' is funded by the Swiss National Science Foundation (SNSF) under grant number [CRSII5_213585](https://data.snf.ch/grants/grant/213585) and the Luxembourg National Research Fund under grant No. 17498891.

Aiming to develop and consolidate tools to process and explore large-scale collections of historical newspapers and radio archives, and to study the impact of this tooling on historical research practices, _Impresso II_ builds upon the first project – 'impresso - Media Monitoring of the Past' (grant number [CRSII5_173719](http://p3.snf.ch/project-173719), Sinergia program). More information at https://impresso-project.ch.

Copyright (C) 2024  The _impresso_ team (contributors to this program: Matteo Romanello, Maud Ehrmann, Alex Flückinger, Edoardo Tarek Hölzl, Pauline Conti).

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of merchantability or fitness for a particular purpose. See the [GNU Affero General Public License](https://github.com/impresso/impresso-pycommons/blob/master/LICENSE) for more details.

## Data Versioning

### Motivation

The `versioning` package of `impresso_commons` contains several modules and scripts that allow to version Impresso's data at various stages of the processing pipeline.
The main goal of this approach is to version the data and track information at every stage to:

1. **Ensure data consisteny and ease of debugging:** Data elements should be consistent across stages, and inconsistencies/differences should be justifiable through the identification of data leakage points.
2. **Allow partial updates:** It should be possible to (re)run all or part of the processes on subsets of the data, knowing which version of the data was used at each step. This can be necessary when new media collections arrive, or when an existing collection has been patched.
3. **Ensure transparency:** Citation of the various data stages and datasets should be straightforward; users should know when using the interface exactly what versions they are using, and should be able to consult the precise statistics related to them.

### Data Stages

Impresso's data processing pipeline is organised in thre main data "meta-stages", mirroring the main processing steps. During each of those meta-stages, different formats of data are created as output of processes and in turn used as inputs to other downstream tasks.

1. **[Data Preparation]**: TODO
2. **[Data Enrichment]**: TODO
3. **[Data Indexation]**: TODO
4. **[Data Releases]**: TODO

### Data Manifests

The versioning aiming to document the data at each step through versions and statistics is implemented through **manifest files**, in JSON format which follow a specific [schema](https://github.com/impresso/impresso-schemas/blob/master/json/versioning/manifest.schema.json). (TODO update JSON schema with yearly modif date.)

After each processing step, a manifest should be created documenting the changes made to the data resulting from that processing. It can also be created on the fly during a processing, and in-between processings to count and sanity-check the contents of a given S3 bucket.
Once created, the manifest file will automatically be uploaded to the S3 bucket corresponding to the data it was computed on, and optionally pushed to the [impresso-data-release](https://github.com/impresso/impresso-data-release) GitHub repository to keep track of all changes made throughout the versions.

#### Computing a manifest - `compute_manifest.py` script

The script `compute_manifest.py`, allows one to compute a manifest on the data present within a specific S3 bucket.
The CLI for this script is the following:

```bash
python compute_manifest.py --config-file=<cf> --log-file=<lf> [--scheduler=<sch> --nworkers=<nw> --verbose]
```

Where the `config_file` should be a simple json file, with specific arguments, all described [here](https://github.com/impresso/impresso-pycommons/blob/data-workflow-versioning/impresso_commons/data/manifest_config/manifest.config.example.md).

- The script uses [dask](https://www.dask.org/) to parallelize its task. By default, it will start a local cluster, with the 8 as defualt number of workers (the parameter `nworkers` can be used to specify any desired value).
- Optinally, a [dask scheduler and workers](https://docs.dask.org/en/stable/deploying-cli.html) can be started in separate terminal windows, and provided to the script via the `scheduler` parameter.

#### Computing a manifest on the fly during a process

TODO

#### Versions and version increments

The manifests use a semantic versioning system, where increments are automatically deduced based on the changes made to the data during a given processing or since the last manifest computation on a bucket. Hence, by default, any data "shown" to the manifest (so added to be taken into account in the statistics) is considered to have been "modified" or re-generated.
However, the option has also been added to compute a manifest on a given bucket to simply count and document its contents (after data was copied from one bucket ot he next for instance). Such cases are noted with the `only_counting` parameter, thank's to which only modifications in the statistics will result in updates/modifications in the final manifest generated.

When the computing of a manifest is launched, the following will take place to determine the version to give to the resulting manifest: 

- _If a an existing version of the manifest for a given data stage exists in the `output_bucket` provided_, this manifest will be read and updated. Its version will be the basis to identify what the version increment should be based on the type of modifications.
- _If no such manifest exists and no manifest can be found in the `output_bucket` provided_, the there are two possibilities:
  - The argument `previous_mft_s3_path` is provided, with the path to a previously computed manifest which is present in _another_ bucket. This manifest is used as the previous one like described above to update the data and compute the next verison.
  - The argument `previous_mft_s3_path` is not provided, then this is the original manifest for a given data stage, and the version in this case is 0.0.1. This is the case for your first manifest.
- Based on the information that was updated, the version increment varies:
  - **Major** version increment if _new title-year pairs_ have been added that were not present in the previous manifest. 
  - **Minor** version increment if:
    - _No new title-year pairs_ have been provided as part of the new manifest's data, and the processing was _not a patch_. 
    - This is in particular the version increment if we re-ingest or re-generate a portion of the corpus, where the underlying stats do not change. If a part of the corpus only was modified/reingested, the specific newspaper titles should be provided within the `newspapers` parameter to indicate which data (within the `media_list`) to consider and update.
  - **Patch** version increment if:
    - The _`only_counting` parameter is set to True_. 
      - This parameter is exactly made for the case scenarios where one wants to recompute the manifest on an _entire bucket of existing data_ which has not necessarily been recomputed or changed (for instance if data was copied, or simply to recount etc). 
      - The computation of the manifest in this context is meant more as a sanity-check of the bucket's contents.
      - The counts and statistics will be computed like in other cases, but the update information (modification date, updated years, git commit url etc) will not be updated unless a change in the statstics is identified (in which case the version is incremented accordingly).
    - The _`is_patch` or `patched_fields` parameters are set to True_. The processing or ingestion versioned in this case is a patch, and the patched_fields will be updated according to the values provided as parameters.

