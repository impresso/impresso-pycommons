Utilities
#########


Command Line interface
----------------------

to come...


Basic utils
-----------

.. autofunctions:: impresso_commons.utils.init_logger
.. autofunctions:: impresso_commons.utils.user_confirmation
.. autofunctions:: impresso_commons.utils.timestamp
.. autofunctions:: impresso_commons.utils.Timer
.. autofunctions:: impresso_commons.utils.chunk


S3 Utils Functions
------------------


.. autofunctions:: impresso_commons.utils.s3.get_s3_client
.. autofunctions:: impresso_commons.utils.s3.get_s3_resource
.. autofunctions:: impresso_commons.utils.s3.get_s3_connection
.. autofunctions:: impresso_commons.utils.s3.get_bucket
.. autofunctions:: impresso_commons.utils.s3.get_bucket_boto3
.. autofunctions:: impresso_commons.utils.s3.s3_get_articles
.. autofunctions:: impresso_commons.utils.s3.s3_get_pages
.. autofunctions:: impresso_commons.utils.s3.get_s3_versions
.. autofunctions:: impresso_commons.utils.s3.get_s3_versions_client
.. autofunctions:: impresso_commons.utils.s3.read_jsonlines
.. autofunctions:: impresso_commons.utils.s3.readtext_jsonlines
.. autofunctions:: impresso_commons.utils.s3.upload


Dask Utils Functions
--------------------
.. autofunctions:: impresso_commons.utils.daskutils.create_even_partitions


Config file
-----------

Proper generic configuration handling will follow.

As of now, here is a generic config:

```
{
  "solr_server" : URL of the server,

  "solr_core": name of the core,

  "s3_host": S3 host,

  "s3_bucket_rebuilt": s3 bucket,

  "s3_bucket_partitions": s3 bucket,

  "s3_bucket_processed": s3 bucket,

  "key_batches": number of key in batch,

  "number_partitions": number of partition,

  "newspapers" : {              // newspaper config, to be detailed
        "GDL": [1991,1998]
    }

}




```




