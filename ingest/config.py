import os
import pyarrow as pa


def check_dir_exists(dir: str):
    if os.path.exists(dir):
        print(f"directory '{dir}' already exists.\n")
    else:
        os.makedirs(dir)
        print(f"directory '{dir}' created.\n")


class S3:
    Bucket = "talento-tech-project"
    Exports = {
        "raw": "exports/raw/",
        "clean": "exports/clean/",
        "transformed": "exports/transformed/",
    }
    Local_Commerce = {
        "raw": "local-commerce/raw/",
        "clean": "local-commerce/clean/",
        "transformed": "local-commerce/transformed/",
    }


class Local_Dir:
    Data = "./data"

    Exports = {
        "raw": f"./data/{S3.Exports['raw']}",
        "clean": f"./data/{S3.Exports['clean']}",
        "transformed": f"./data/{S3.Exports['transformed']}",
    }


class Exports:
    Columns_To_Drop = [
        "PAIS",
        "COD_SAL1",
        "BANDERA",
        "REGIM",
        "FINALID",
        "CER_ORI1",
        "SISESP",
        "UNID",
        "CODUNI2",
        "PBK",
        "PNK",
    ]

    Dtypes = [
        ("FECH", pa.uint16()),
        ("ADUA", pa.uint8()),
        ("COD_PAI4", pa.string()),
        ("COD_SAL", pa.string()),
        ("DPTO2", pa.uint8()),
        ("VIA", pa.uint8()),
        ("MODAD", pa.uint16()),
        ("POSAR", pa.uint64()),
        ("DPTO1", pa.uint8()),
        ("CANTI", pa.string()),
        ("FOBDOL", pa.string()),
        ("FOBPES", pa.string()),
        ("AGRENA", pa.string()),
        ("FLETES", pa.string()),
        ("SEGURO", pa.string()),
        ("OTROSG", pa.string()),
    ]


class Filename:
    Parquet = {"exports": {"clean": "clean_exports.parquet"}}
