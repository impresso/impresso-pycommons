# Configuration file for Versioning Manifest generation

The script `impresso_commons/versioning/compute_manifest.py` allows to compute the versioning manifest
based on the contents of a s3 bucket (or bucket partition).
The configuration file should be used to provide all the necessary arguments to the script.
Below is an example for the case of NE-processing, as well as a description of each parameter.

## Example

```json
# ner_mft_config.json

{
    "data_stage": "entities",
    "output_bucket": "processing-canonical-data/ner_el/v02_feb_2024",
    "input_bucket": "rebuilt-data",
    "git_repository": "/local/path/to/NER-EL/repo",
    "newspapers": [
        "DLE", "BNN"
    ],
    "temp_directory": "/local/path/to/a/temp/file",
    "previous_mft_s3_path": "",
    "is_staging": true,
    "is_patch": false,
    "patched_fields": [],
    "push_to_git": false,
    "file_extensions": ".jsonl.bz2",
    "notes": "",
    "log_file": "path/to/log_file.log"
}
```

## Arguments

- __*data_stage*__: (required) The data stage of the data to version with the manifest. Should be a valid data stage: one of : ```"canonical", "rebuilt", "evenized-rebuilt", "entities", "embeddings", "langident", "lingproc", "orcqa", "text-reuse", "topics"```. The exact list is temporary and subject to change based on needs.
- __*output_bucket*__: (required) The S3 bucket (*including* partition if applicable) to read the data from and upload the created manifest to. This is the bucket (partition) that is to be versioned.
- __*input_bucket*__: (optional) The S3 bucket of the data that was used as *input* to the processing step that generated the data that is to be versioned.
  - For any text-mining task, this will correspond to either a bucket with rebuilt or evenized data.
  - Can be left empty (`""` or `null`) in the case of *canonical* processing, were the input data is not on S3.
- __*git_repository*__: (required) The *local* path to the git repository that was used to process the data to inside `output_bucket` to be versioned. Will be used to extract the last commit on the active branch.
- __*newspapers*__: (required) List of newspaper titles to consider for  the versioning, used for filtering the files within the bucket partition.
  - If left empty, *all* files within the bucket partition will be considered.
  - If the processing can only performed on the entire corpus at once, should be left empty.
- __*temp_directory*__: (required) Temporary directory to use to clone the `impresso/impresso-data-release` git repository (to push the generated manifest).
- __*previous_mft_s3_path*__: (optional) S3 path of the *previous* manifest to use, if it is not located inside `output_bucket`.
  - Should be left empty (`""` or `null`) if a versioning manifest for this specific `data_stage` is already present inside `output_bucket`.
  - The *previous* manifest is the most recent manifest that had been generated for this specific `data_stage`, and can sometimes be in another bucket.
- __*is_staging*__: (required) Boolean value, set to `true` if the manifest is versioning 'staging' data, that is not yet final or ready for production.
  - Also determines to which *branch* of the `impresso/impresso-data-release` git repository the generated manifest will be pushed to: `staging` if `true`, `master` otherwise.
- __*is_patch*__: (optional) Whether the update made to the current data to be versioned in `output_bucket` was a patch.
  - An update is considered to be a patch if only some of the output's properties were modified, and that the update was not performed by a re-ingestion/recomputation from the input data (rather the modification of the existing one).
  - If set to `true`, the version of the generated manifest will reflect with a patch increment.
  - Can be left to `false` or `null` by default.
- __*patched_fields*__: (optional) In the case `is_patch=true`, should be the list of individual properties modified or added to the data present in `output_bucket`.
  - Can be left empty if `is_patch=false`.
- __*push_to_git*__: (optional) Whether to push the generated manifest to the `impresso/impresso-data-release` gitrepository as it's uploaded to S3.
  - Can be set to `false` or `null` during experimentation or debugging to only upload to S3 and not push to git.
- __*notes*__ : (optional) Note for the manifest, about the processing.
  - Can be left empty (`""` or `null`), in which case a generic note with the processed titles will be used.
- __*file_extensions*__: (required) The extension of the files to consider within `output_bucket`, *including* the first `.` (`.jsonl.bz2` instead of `jsonl.bz2`).
  - Is necessary for the `fixed_s3fs_glob` function used internally, and allows to ensure only desired files are considered (eg. if `.txt` or `.json` files are present in the bucket).