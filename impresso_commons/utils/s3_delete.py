"""
Simple CLI script to delete versioned keys from S3

Usage:
    impresso_commons/utils/s3_delete.py --bucket=<b> --prefix=<p>

Options:
    --bucket=<b>    Target S3 bucket
    --prefix=<p>    Prefix of keys to delete
"""

from docopt import docopt

from impresso_commons.utils.s3 import get_s3_resource
from impresso_commons.utils import user_confirmation

def delete_versioned_keys(
    client,
    Bucket,
    Prefix,
    IsTruncated=True,
    MaxKeys=1000,
    KeyMarker=None
):
    """TODO"""
    while IsTruncated == True:
            if not KeyMarker:
                version_list = client.list_object_versions(
                        Bucket=Bucket,
                        MaxKeys=MaxKeys,
                        Prefix=Prefix)
            else:
                version_list = client.list_object_versions(
                        Bucket=Bucket,
                        MaxKeys=MaxKeys,
                        Prefix=Prefix,
                        KeyMarker=KeyMarker)

            try:
                objects = []
                versions = version_list['Versions']
                for v in versions:
                        objects.append({
                            'VersionId':v['VersionId'],
                            'Key': v['Key']
                        })
                response = client.delete_objects(
                    Bucket=Bucket,
                    Delete={'Objects':objects}
                )
                print(f"Deleted {len(response['Deleted'])} keys")
            except:
                pass

            try:
                objects = []
                delete_markers = version_list['DeleteMarkers']
                for d in delete_markers:
                        objects.append({
                            'VersionId':d['VersionId'],
                            'Key': d['Key']
                        })
                response = client.delete_objects(
                    Bucket=Bucket,
                    Delete={'Objects':objects}
                )
                print(f"Deleted {len(response['Deleted'])} keys")
            except:
                pass

            IsTruncated = version_list['IsTruncated']
            try:
                KeyMarker = version_list['NextKeyMarker']
            except KeyError:
                print("Done!")


def main():
    args = docopt(__doc__)
    b = args['--bucket']
    p = args['--prefix']
    q_1 = f"all keys with prefix `{p}`"
    q = f"\nAre you sure you want to delete {q_1} from bucket `s3://{b}` ?"

    if user_confirmation(question=q):
        print("Ok, let's start (it will take a while!)")
        s3_client = get_s3_resource().meta.client
        delete_versioned_keys(client=s3_client, Bucket=b, Prefix=p)
    else:
        print('Ok then, see ya!')

if __name__ == '__main__':
    main()
