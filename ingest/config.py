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


class Database:
    dir = "/workspaces/talento_tech/data/transformed/"
    filename = "db.duckdb"


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
    }
    Korea_Imports = {
        "raw": "korea-imports/raw/",
        "clean": "korea-imports/clean/",
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
    }

    Korea_Imports = {
        "raw": Data + S3.Korea_Imports["raw"],
        "clean": Data + S3.Korea_Imports["clean"],
    }


class Filename:
    """Filename for processed data.

    Attributes:
        Exports (dict[str]): exports processed data.
        Local_Commerce (dict[str]): local commerce processed data.
        Korea_Imports (dict[str]): korea imports processed data.
    """

    Exports = {"clean": "clean_exports.parquet"}

    Korea_Imports = {"clean": "clean_koreaimports.parquet"}


class Exports:
    """Exports relevant arguments.

    Attributes:
        Columns_To_Drop (list[str]): columns from raw data to drop.
        Dtypes (list[tuple]): column-dtype pairs to cast clean data.
    """

    Columns_To_Drop = [
        "FECH",
        "ADUA",
        "COD_SAL",
        "VIA",
        "CANTI",
        "FOBDOL",
        "FLETES",
        "SEGURO",
        "OTROSG",
        "PAIS",
        "COD_SAL1",
        "DPTO2",
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
        "FOBPES",
        "AGRENA",
    ]

    Dtypes = [
        ("COD_PAI4", pa.string()),
        ("POSAR", pa.uint8()),
        ("DPTO1", pa.uint8()),
        ("FOBPES", pa.float64()),
        ("AGRENA", pa.float64()),
    ]

    Commoditie_Code_Format = {
        "commoditie-col": "POSAR",
        "pad": True,
        "slice": True,
        "width": 10,
        "side": "left",
        "fillchar": "0",
        "stop": 2,
    }


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
        "cmdDesc",
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

    Monetary_Cols = ["primaryValue"]

    Dtypes = [
        ("partnerDesc", pa.string()),
        ("cmdCode", pa.uint8()),
        ("primaryValue", pa.float64()),
    ]

    Commoditie_Code_Format = {
        "commoditie-col": "cmdCode",
        "pad": True,
        "slice": True,
        "width": 4,
        "side": "left",
        "fillchar": "0",
        "stop": 2,
    }


datasets: dict[str, dict[str, typing.Any]] = {
    "exports": {
        "s3-raw": S3.Exports["raw"],
        "local-raw": Local_Dir.Exports["raw"],
        "s3-clean": S3.Exports["clean"],
        "local-clean": Local_Dir.Exports["clean"],
        "filename-clean": Filename.Exports["clean"],
        "drop-cols": Exports.Columns_To_Drop,
        "dtypes": Exports.Dtypes,
        "monetary-cols": Exports.Monetary_Cols,
        "commoditie-col-format": Exports.Commoditie_Code_Format,
    },
    "korea-imports": {
        "s3-raw": S3.Korea_Imports["raw"],
        "local-raw": Local_Dir.Korea_Imports["raw"],
        "s3-clean": S3.Korea_Imports["clean"],
        "local-clean": Local_Dir.Korea_Imports["clean"],
        "filename-clean": Filename.Korea_Imports["clean"],
        "drop-cols": Korea_Imports.Columns_To_Drop,
        "dtypes": Korea_Imports.Dtypes,
        "monetary-cols": Korea_Imports.Monetary_Cols,
        "commoditie-col-format": Korea_Imports.Commoditie_Code_Format,
    },
}
"""dict[str, dict[str, Any]]: collection of relevant info about exports, local commerce and korea imports."""
