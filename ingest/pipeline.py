import os
import config
from s3 import download_raw_data, get_objects, upload_file
from load import unify_csv, Load


def main():
    for key in config.datasets.keys():
        download_raw_data(bucket=config.S3.Bucket, prefix=config.datasets[key]["s3-raw"], marker=config.datasets[key]["s3-raw"])

        data = unify_csv(config.datasets[key]["local-raw"])

        data = Load(data)

        data = (data
        .get_pyarrow_table()
        .purge_columns(columns=config.datasets[key]["drop-cols"])
        .cast_dtypes(dtypes=config.datasets[key]["dtypes"])
        .table_to_dataframe()
        .purge_duplicates()
        )

        data.show_info()
        data.save_to_parquet(
            dir=config.datasets[key]["local-clean"],
            filename=config.datasets[key]["filename-clean"]
        )

if __name__ == "__main__":
    main()
