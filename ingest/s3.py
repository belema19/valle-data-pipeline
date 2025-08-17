import boto3

RAW_DATA_BUCKET = "talento-tech-raw-data"

def get_s3_client():
    """returns a s3 client"""

    s3 = boto3.client("s3")
    return s3

def get_objects():
    """shows all objects within RAW_DATA_BUCKET"""
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=RAW_DATA_BUCKET)

    for obj in response['Contents']:
        print(obj['Key'])

def download_raw_data():
    """downloads all objects from RAW_DATA_BUCKET"""

    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=RAW_DATA_BUCKET)

    for obj in response["Contents"]:
        s3.download_file(
            RAW_DATA_BUCKET, f"{obj['Key']}", f"./data/raw-data/{obj['Key']}"
        )