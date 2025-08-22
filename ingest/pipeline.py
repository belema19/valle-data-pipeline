import os
import config
from s3 import download_raw_data, get_objects, upload_file
from load import unify_csv, Load

# TODO
# Working on extract and load efficiency

def main():
    datasets = {
        "exports": {
            "s3-raw": config.S3.Exports["raw"],
            "local-raw": config.Local_Dir.Exports["raw"],
            "s3-clean": config.S3.Exports["clean"],
            "local-clean": config.Local_Dir.Exports["clean"],
            "filename-clean": config.Filename.Exports["clean"],
            "drop-cols": config.Exports.Columns_To_Drop,
            "dtypes": config.Exports.Dtypes
        },
        "local-commerce": {
            "s3-raw": config.S3.Local_Commerce["raw"],
            "local-raw": config.Local_Dir.Local_Commerce["raw"],
            "s3-clean": config.S3.Local_Commerce["clean"],
            "local-clean": config.Local_Dir.Local_Commerce["clean"],
            "filename-clean": config.Filename.Local_Commerce["clean"],
            "drop-cols": config.Local_Commerce.Columns_To_Drop,
            "dtypes": config.Local_Commerce.Dtypes
        },
        "korea-imports": {
            "s3-raw": config.S3.Korea_Imports["raw"],
            "local-raw": config.Local_Dir.Korea_Imports["raw"],
            "s3-clean": config.S3.Korea_Imports["clean"],
            "local-clean": config.Local_Dir.Korea_Imports["clean"],
            "filename-clean": config.Filename.Korea_Imports["clean"],
            "drop-cols": config.Korea_Imports.Columns_To_Drop,
            "dtypes": config.Korea_Imports.Dtypes
        }
    }

    for key in datasets.keys():
        download_raw_data(bucket=config.S3.Bucket, prefix=datasets[key]["s3-raw"], marker=datasets[key]["s3-raw"])

        data = unify_csv(datasets[key]["local-raw"])

        data = Load(data)

        data = (data
        .get_pyarrow_table()
        .purge_columns(columns=datasets[key]["drop-cols"])
        .cast_dtypes(dtypes=datasets[key]["dtypes"])
        .table_to_dataframe()
        .purge_duplicates()
        )

        data.show_info()
        data.save_to_parquet(
            dir=datasets[key]["local-clean"],
            filename=datasets[key]["filename-clean"]
        )

if __name__ == "__main__":
    main()
