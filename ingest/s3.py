"""Manage S3 API calls.

This modules stores functions to make CRUD operations
on a S3 Bucket. For the S3 API to work, it is needed a
~/.config/aws directory with credential on the local machine.

Attributes:
    - get_s3_client: creates a s3 client instance.

    - get_objects: returns a list of objects in the given s3 path.

    - download_raw_data: downloads data from a given raw s3 path
      to a default raw local path.

    - upload_clean_data: uploads data to a given s3 path,
      from a given local path.
"""

import os
import logging
import config
import boto3
from botocore.exceptions import ClientError


def get_s3_client():
    """Creates a s3 client instance.

    Returns:
        boto3.Client
    """

    s3 = boto3.client("s3")
    return s3


def get_objects(bucket: str, prefix: str, start_after: str) -> list:
    """Shows all objects in a given s3 path.

    Args:
        bucket (str): target s3 bucket.
        prefix (str): s3 objects common prefix.
        start_after (str): from where to start reading.

        Returns:
            A list of S3 objects if there are any in the given prefix,
              else returns a empty list.
    """
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, StartAfter=start_after)
    if response["KeyCount"] > 0:
        return response["Contents"]
    else:
        return []


def download_raw_data(
    bucket: str, prefix: str, start_after: str, verbose: bool = False
):
    """Downloads S3 objects from a given S3 path.

    Args:
        bucket (str): target s3 bucket.
        prefix (str): s3 objects common prefix.
        start_after (str): from where to start reading.
        verbose (bool): whether to print or not each found object.
          default is False.

    Returns:
        bool: True if data was downloaded, else False.
    """

    config.check_dir_exists(f"{config.Local_Dir.Data}/{start_after}")

    if not os.listdir(f"{config.Local_Dir.Data}/{start_after}"):
        s3 = get_s3_client()
        response = get_objects(bucket, prefix, start_after)

        if verbose:
            for obj in response:
                print(obj["Key"])

        for obj in response:
            s3.download_file(
                bucket, f"{obj['Key']}", f"{config.Local_Dir.Data}/{obj['Key']}"
            )

        return True
    return False


def upload_clean_data(filename: str, bucket: str, object_name: str):
    """Upload a file to an S3 given path.

    Args:
        filename (str): local file path to upload.
        bucket (str): target S3 bucket to upload.
        object_name (str): target S3 object path to upload.

    Returns:
        True if data was uploaded, else False.
    """

    s3 = boto3.client("s3")
    try:
        s3.upload_file(filename, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
