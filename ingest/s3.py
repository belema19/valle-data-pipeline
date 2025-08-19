import os
import logging
import config
import boto3
from botocore.exceptions import ClientError


def get_s3_client():
    """returns a s3 client"""

    s3 = boto3.client("s3")
    return s3


def get_objects(bucket, prefix):
    """shows all objects within RAW_DATA_BUCKET"""
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for obj in response["Contents"]:
        print(obj["Key"])


def download_raw_data(bucket, prefix):
    """downloads all objects from RAW_DATA_BUCKET"""

    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    response = list(response["Contents"])[1:]

    for obj in response:
        if obj["Key"] == "exports/raw/":
            continue
        s3.download_file(config.S3.Bucket, f"{obj['Key']}", f"./data/{obj['Key']}")


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
