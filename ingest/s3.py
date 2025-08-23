import os
import logging
import config
import boto3
from botocore.exceptions import ClientError


def get_s3_client():
    s3 = boto3.client("s3")
    return s3


def get_objects(bucket: str, prefix: str, start_after: str) -> list:
    """shows all objects within RAW_DATA_BUCKET"""
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, StartAfter=start_after)
    if response["KeyCount"] > 0:
        return response["Contents"]
    else:
        return []


def download_raw_data(bucket: str, prefix: str, start_after: str):
    """downloads all objects from RAW_DATA_BUCKET"""

    config.check_dir_exists(f"{config.Local_Dir.Data}/{start_after}")

    if not os.listdir(f"{config.Local_Dir.Data}/{start_after}"):
        s3 = get_s3_client()
        response = get_objects(bucket, prefix, start_after)

        if response:
            for obj in response:
                print(obj["Key"])

            for obj in response:
                s3.download_file(
                    bucket, f"{obj['Key']}", f"{config.Local_Dir.Data}/{obj['Key']}"
                )

            return True
        return False
    return False


def upload_clean_data(filename, bucket, object_name):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name
    :return: True if file was uploaded, else False
    """

    s3 = boto3.client("s3")
    try:
        s3.upload_file(filename, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
