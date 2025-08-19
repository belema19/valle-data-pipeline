import os
import config
from s3 import download_raw_data, get_objects, upload_file
from load import unify_csv, clean_exports, load_exports


def main():
    def extract_load_exports():
        # Extract exports
        if not os.listdir(config.Local_Dir.Exports["raw"]):
            download_raw_data(bucket=config.S3.Bucket, prefix=config.S3.Exports["raw"])

        # TODO:
        #   Upload to S3
        #   Automate folders creation

        # Clean Exports
        if not os.listdir(config.Local_Dir.Exports["clean"]):
            load_exports(clean_exports(unify_csv(config.Local_Dir.Exports["raw"])))

    extract_load_exports()


if __name__ == "__main__":
    main()
