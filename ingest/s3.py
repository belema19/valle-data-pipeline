import os
import logging
import config
import boto3
from botocore.exceptions import ClientError


def get_s3_resource():
    """returns a s3 client"""

    s3 = boto3.resource("s3")
    return s3

def get_s3_client():

    s3 = boto3.client("s3")
    return s3


def get_objects(bucket: str, prefix: str, marker: str):
    """shows all objects within RAW_DATA_BUCKET"""
    s3 = get_s3_resource()
    bucket = s3.Bucket(config.S3.Bucket)
    return bucket.objects.filter(
        Prefix=prefix,
        Marker=marker
    ).all()

def download_raw_data(bucket: str, prefix: str, marker: str):
    """downloads all objects from RAW_DATA_BUCKET"""


    config.check_dir_exists(
        f"{config.Local_Dir.Data}/{marker}"
    )

    if not os.listdir(f"{config.Local_Dir.Data}/{marker}"):

        s3 = get_s3_client()
        response = get_objects(bucket, prefix, marker)

        for obj in response:
            print(obj.key)

        for obj in response:
            s3.download_file(
                bucket,
                f"{obj.key}",
                f"{config.Local_Dir.Data}/{obj.key}"
            )

        return True
    return False

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
