"""Executable of the ingest phase.

This module coordinates all modules within the ingest phase
in order to prepare the data for transformation.
"""

import os
import config
import s3
from load import unify_csv, Load

BUCKET = config.S3.Bucket


def main(arg: str):
    # Extract
    if arg == "clean" or arg == "all":
        try:
            for key in config.datasets.keys():
                s3.download_raw_data(
                    bucket=BUCKET,
                    prefix=config.datasets[key]["s3-raw"],
                    start_after=config.datasets[key]["s3-raw"],
                )

                data = unify_csv(config.datasets[key]["local-raw"])

                data = Load(data)

                data = (
                    data.get_pyarrow_table()  # type: ignore
                    .purge_columns(columns=config.datasets[key]["drop-cols"])
                    .table_to_dataframe()
                    .fix_monetary_punctuation(config.datasets[key]["monetary-cols"])
                    .format_commoditie_code(
                        commoditie_col=config.datasets[key]["commoditie-col-format"][
                            "commoditie-col"
                        ],
                        pad_=config.datasets[key]["commoditie-col-format"]["pad"],
                        slice_=config.datasets[key]["commoditie-col-format"]["slice"],
                        width=config.datasets[key]["commoditie-col-format"]["width"],
                        side=config.datasets[key]["commoditie-col-format"]["side"],
                        fillchar=config.datasets[key]["commoditie-col-format"][
                            "fillchar"
                        ],
                        stop=config.datasets[key]["commoditie-col-format"]["stop"],
                    )
                    .cast_dtypes(dtypes=config.datasets[key]["dtypes"])
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

    # load
    if arg == "load" or arg == "all":
        try:
            for key in config.datasets.keys():
                local_dir = config.datasets[key]["local-clean"]
                s3_dir = config.datasets[key]["s3-clean"]

                if not s3.get_objects(
                    bucket=BUCKET, prefix=s3_dir, start_after=s3_dir
                ) and os.listdir(local_dir):
                    for filename in os.listdir(local_dir):
                        s3.upload_clean_data(
                            bucket=BUCKET,
                            filename=local_dir + filename,
                            object_name=f"{s3_dir}{filename}",
                        )

                else:
                    print(
                        f"{local_dir} is empty or {s3_dir} has data\n"
                        "Nothing was uploaded!\n"
                    )

        except Exception as e:
            print(
                "There was an error in loading phase: {e}\n"
                "This error is non-recoverable!\n"
            )
            raise


if __name__ == "__main__":
    main(arg="all")
