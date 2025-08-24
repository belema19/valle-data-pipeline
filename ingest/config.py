"""Configuration for the ingest phase.

This module stores relevant configuration values for the operation
of the modules within the ingest phase.

Attributes:
    check_dir_exists: checks if a given local directory exists.
    S3: configuration values for S3 API.
    Local_Dir: configuration values for project folder.
    Filename: configuration values for created files.
    Exports: configuration values for exports data.
    Local_Commerce: configuration values for local commerce data.
    Korea_Imports: configuration values for korea imports data.
    datasets: collection of configuration values."""

import os
import typing
import pyarrow as pa


def check_dir_exists(dir: str):
    """checks if a given directory exists.

    If the directory doesn't exist, it is created.

    Args:
        dir (str): directory relative path.
    """
    if os.path.exists(dir):
        print(f"directory '{dir}' already exists.\n")
    else:
        os.makedirs(dir)
        print(f"directory '{dir}' created.\n")


class S3:
    """S3 project bucket relevant paths.

    Attributes:
        Bucket (str): project bucket.
        Exports (dict[str]): exports objects.
        Local_Commerce (dict[str]): local commerce objects.
        Korea_Imports (dict[str]): korea imports objects.
    """

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
    Korea_Imports = {
        "raw": "korea-imports/raw/",
        "clean": "korea-imports/clean/",
        "transformed": "korea-imports/transformed/",
    }


class Local_Dir:
    """Local project directory relevant paths.

    Attributes:
        Data (str): root data folder.
        Exports (dict[str]): exports data folder.
        Local_Commerce (dict[str]): local commerce data folder.
        Korea_Imports (dict[str]): korea imports data folder.
    """

    Data = "./data/"

    Exports = {
        "raw": Data + S3.Exports["raw"],
        "clean": Data + S3.Exports["clean"],
        "transformed": Data + S3.Exports["transformed"],
    }

    Local_Commerce = {
        "raw": Data + S3.Local_Commerce["raw"],
        "clean": Data + S3.Local_Commerce["clean"],
        "transformed": Data + S3.Local_Commerce["transformed"],
    }

    Korea_Imports = {
        "raw": Data + S3.Korea_Imports["raw"],
        "clean": Data + S3.Korea_Imports["clean"],
        "transformed": Data + S3.Korea_Imports["transformed"],
    }


class Filename:
    """Filename for processed data.

    Attributes:
        Exports (dict[str]): exports processed data.
        Local_Commerce (dict[str]): local commerce processed data.
        Korea_Imports (dict[str]): korea imports processed data.
    """

    Exports = {"clean": "clean_exports.parquet"}

    Local_Commerce = {"clean": "clean_localcommerce.parquet"}

    Korea_Imports = {"clean": "clean_koreaimports.parquet"}


class Exports:
    """Exports relevant arguments.

    Attributes:
        Columns_To_Drop (list[str]): columns from raw data to drop.
        Dtypes (list[tuple]): column-dtype pairs to cast clean data.
    """

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

    Monetary_Cols = [
        "FOBDOL",
        "FOBPES",
        "AGRENA",
        "FLETES",
        "SEGURO",
        "OTROSG"
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
        ("FOBDOL", pa.float64()),
        ("FOBPES", pa.float64()),
        ("AGRENA", pa.float64()),
        ("FLETES", pa.float64()),
        ("SEGURO", pa.float64()),
        ("OTROSG", pa.float64()),
    ]


class Local_Commerce:
    """Local commerce relevant arguments.

    Attributes:
        Columns_To_Drop (list[str]): columns from raw data to drop.
        Dtypes (list[tuple]): column-dtype pairs to cast clean data.
    """

    Columns_To_Drop = ["V1", "Personal"]

    Monetary_Cols = [
        "VENTA"
    ]

    Dtypes = [("DEPTO", pa.uint8()), ("CORRELA_9", pa.string()), ("VENTA", pa.float64())]


class Korea_Imports:
    """Korea imports relevant arguments.

    Attributes:
        Columns_To_Drop (list[str]): columns from raw data to drop.
        Dtypes (list[tuple]): column-dtype pairs to cast clean data.
    """

    Columns_To_Drop = [
        "typeCode",
        "freqCode",
        "refPeriodId",
        "refYear",
        "refMonth",
        "period",
        "reporterCode",
        "reporterISO",
        "reporterDesc",
        "flowCode",
        "flowDesc",
        "partnerCode",
        "partnerISO",
        "partner2Code",
        "partner2ISO",
        "partner2Desc",
        "classificationCode",
        "classificationSearchCode",
        "isOriginalClassification",
        "aggrLevel",
        "isLeaf",
        "customsCode",
        "customsDesc",
        "mosCode",
        "motCode",
        "motDesc",
        "qtyUnitCode",
        "qtyUnitAbbr",
        "qty",
        "altQtyUnitCode",
        "altQtyUnitAbbr",
        "altQty",
        "isAltQtyEstimated",
        "isQtyEstimated",
        "netWgt",
        "isNetWgtEstimated",
        "grossWgt",
        "isGrossWgtEstimated",
        "cifvalue",
        "fobvalue",
        "legacyEstimationFlag",
        "isReported",
        "isAggregate",
    ]

    Monetary_Cols = [
        "primaryValue"
    ]

    Dtypes = [
        ("partnerDesc", pa.string()),
        ("cmdCode", pa.uint16()),
        ("cmdDesc", pa.string()),
        ("primaryValue", pa.float64()),
    ]


datasets: dict[str, dict[str, typing.Any]] = {
    "exports": {
        "s3-raw": S3.Exports["raw"],
        "local-raw": Local_Dir.Exports["raw"],
        "s3-clean": S3.Exports["clean"],
        "local-clean": Local_Dir.Exports["clean"],
        "filename-clean": Filename.Exports["clean"],
        "drop-cols": Exports.Columns_To_Drop,
        "dtypes": Exports.Dtypes,
        "monetary-cols": Exports.Monetary_Cols
    },
    "local-commerce": {
        "s3-raw": S3.Local_Commerce["raw"],
        "local-raw": Local_Dir.Local_Commerce["raw"],
        "s3-clean": S3.Local_Commerce["clean"],
        "local-clean": Local_Dir.Local_Commerce["clean"],
        "filename-clean": Filename.Local_Commerce["clean"],
        "drop-cols": Local_Commerce.Columns_To_Drop,
        "dtypes": Local_Commerce.Dtypes,
        "monetary-cols": Local_Commerce.Monetary_Cols
    },
    "korea-imports": {
        "s3-raw": S3.Korea_Imports["raw"],
        "local-raw": Local_Dir.Korea_Imports["raw"],
        "s3-clean": S3.Korea_Imports["clean"],
        "local-clean": Local_Dir.Korea_Imports["clean"],
        "filename-clean": Filename.Korea_Imports["clean"],
        "drop-cols": Korea_Imports.Columns_To_Drop,
        "dtypes": Korea_Imports.Dtypes,
        "monetary-cols": Korea_Imports.Monetary_Cols
    },
}
"""dict[str, dict[str, Any]]: collection of relevant info about exports, local commerce and korea imports."""
