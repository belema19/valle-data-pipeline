import os
import config
from s3 import download_raw_data, get_objects, upload_file
from load import unify_csv, Load

# TODO
# Working on extract and load efficiency

def main():
    try:
        objects = [config.S3.Exports["raw"], config.S3.Local_Commerce["raw"]]

        for obj in objects:
            download_raw_data(bucket=config.S3.Bucket, prefix=obj, marker=obj)
    except:
        raise

    try:
        data = unify_csv(config.Local_Dir.Exports["raw"])

        data = Load(data)
    except:
        raise

    try:
        data = (data
        .get_pyarrow_table()
        .purge_columns(columns=config.Exports.Columns_To_Drop)
        )
        data.show_info()
    except:
        raise

if __name__ == "__main__":
    main()
