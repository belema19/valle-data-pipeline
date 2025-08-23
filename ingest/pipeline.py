import os
import config
from s3 import download_raw_data, get_objects, upload_clean_data
from load import unify_csv, Load


def main(arg):
    # Extract
    if arg == "clean":
        try:
            for key in config.datasets.keys():
                download_raw_data(
                    bucket=config.S3.Bucket,
                    prefix=config.datasets[key]["s3-raw"],
                    start_after=config.datasets[key]["s3-raw"],
                )

                data = unify_csv(config.datasets[key]["local-raw"])

                data = Load(data)

                data = (
                    data.get_pyarrow_table()
                    .purge_columns(columns=config.datasets[key]["drop-cols"])
                    .cast_dtypes(dtypes=config.datasets[key]["dtypes"])
                    .table_to_dataframe()
                    .purge_duplicates()
                )

                data.show_info()
                data.save_to_parquet(
                    dir=config.datasets[key]["local-clean"],
                    filename=config.datasets[key]["filename-clean"],
                )
        except Exception as e:
            print(
                f"There was an error in the cleaning phase: {e}\n"
                "This error in non-recoverable!\n"
            )
            raise

    if arg == "load":
        for key in config.datasets.keys():
            local_dir = config.datasets[key]["local-clean"]
            s3_dir = config.datasets[key]["s3-clean"]
            bucket = config.S3.Bucket

            if not get_objects(
                bucket=bucket, prefix=s3_dir, start_after=s3_dir
            ) and os.listdir(local_dir):
                for filename in os.listdir(local_dir):
                    upload_clean_data(
                        bucket=config.S3.Bucket,
                        filename=local_dir + filename,
                        object_name=f"{config.datasets[key]['s3-clean']}{filename}",
                    )

            else:
                print(
                    f"{local_dir} is empty or {s3_dir} has data\n"
                    "Nothing was uploaded!\n"
                )


if __name__ == "__main__":
    main(arg="load")
