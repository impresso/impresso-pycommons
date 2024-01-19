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
