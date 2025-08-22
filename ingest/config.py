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
        "transformed": "exports/transformed/"
    }
    Local_Commerce = {
        "raw": "local-commerce/raw/",
        "clean": "local-commerce/clean/",
        "transformed": "local-commerce/transformed/"
    }
    Korea_Imports = {
        "raw": "korea-imports/raw/",
        "clean": "korea-imports/clean/",
        "transformed": "korea-imports/transformed/"
    }


class Local_Dir:
    Data = "./data/"

    Exports = {
        "raw": Data + S3.Exports['raw'],
        "clean": Data + S3.Exports['clean'],
        "transformed": Data + S3.Exports['transformed']
    }

    Local_Commerce = {
        "raw": Data + S3.Local_Commerce["raw"],
        "clean": Data + S3.Local_Commerce["clean"],
        "transformed": Data + S3.Local_Commerce["transformed"]
    }

    Korea_Imports = {
        "raw": Data + S3.Korea_Imports["raw"],
        "clean": Data + S3.Korea_Imports["clean"],
        "transformed": Data + S3.Korea_Imports["transformed"]
    }

class Filename:
    Exports = {
        "clean": "clean_exports.parquet"
    }

    Local_Commerce = {
        "clean": "clean_localcommerce.parquet"
    }

    Korea_Imports = {
        "clean": "clean_koreaimports.parquet"
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

class Local_Commerce:
    Columns_To_Drop = [
        "V1",
        "Personal"
    ]

    Dtypes = [
        ("DEPTO", pa.uint8()),
        ("CORRELA_9", pa.string()),
        ("VENTA", pa.string())
    ]

class Korea_Imports:
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
        "isAggregate"
    ]

    Dtypes = [
        ("partnerDesc", pa.string()),
        ("cmdCode", pa.uint16()),
        ("cmdDesc", pa.string()),
        ("primaryValue", pa.float64())
    ]

datasets = {
    "exports": {
        "s3-raw": S3.Exports["raw"],
        "local-raw": Local_Dir.Exports["raw"],
        "s3-clean": S3.Exports["clean"],
        "local-clean": Local_Dir.Exports["clean"],
        "filename-clean": Filename.Exports["clean"],
        "drop-cols": Exports.Columns_To_Drop,
        "dtypes": Exports.Dtypes
    },
    "local-commerce": {
        "s3-raw": S3.Local_Commerce["raw"],
        "local-raw": Local_Dir.Local_Commerce["raw"],
        "s3-clean": S3.Local_Commerce["clean"],
        "local-clean": Local_Dir.Local_Commerce["clean"],
        "filename-clean": Filename.Local_Commerce["clean"],
        "drop-cols": Local_Commerce.Columns_To_Drop,
        "dtypes": Local_Commerce.Dtypes
    },
    "korea-imports": {
        "s3-raw": S3.Korea_Imports["raw"],
        "local-raw": Local_Dir.Korea_Imports["raw"],
        "s3-clean": S3.Korea_Imports["clean"],
        "local-clean": Local_Dir.Korea_Imports["clean"],
        "filename-clean": Filename.Korea_Imports["clean"],
        "drop-cols": Korea_Imports.Columns_To_Drop,
        "dtypes": Korea_Imports.Dtypes
    }
}