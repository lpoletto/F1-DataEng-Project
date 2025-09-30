from os import environ as env
import logging
from minio import Minio


def create_bucket(bucket_name, region=None) -> bool:
    """Create an bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        client = Minio(
            "minio:9000",
            access_key=env["MINIO_ROOT_USER"],
            secret_key=env["MINIO_ROOT_PASSWORD"],
            secure=False
        )

        minio_bucket = bucket_name

        found = client.bucket_exists(minio_bucket)
        if not found:
            client.make_bucket(minio_bucket)
            print(f"Bucket s3a://{minio_bucket} created.")
        else:
            print(f"Bucket {minio_bucket} already exists.")
            return False
    except Exception as e:
        logging.error(e)
        return False
    return True