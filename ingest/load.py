import config
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import duckdb


def unify_csv(target: str):
    data = duckdb.read_csv(target + "*.csv")
    return data


def clean_exports(data: duckdb.DuckDBPyRelation):
    print("Working on exports data...\n")
    exports_table = data.arrow()
    print(exports_table)

    print("Droping unwanted columns...\n")
    exports_table = exports_table.drop_columns(config.Exports.Columns_To_Drop)

    # Check shape
    print("Table shape:\n")
    print(exports_table.shape)

    # Handle null values
    print("Checking for null values...\n")
    for column in exports_table.column_names:
        print(f"for column {column}")
        print(pc.count(column, mode="only_null"), "\n")
    print("Droping any null values...\n")
    exports_table.drop_null()

    # Cast dtypes
    print("Optimizing dtypes...\n")
    exports_schema = pa.schema(config.Exports.Dtypes)
    exports_table = exports_table.cast(exports_schema)
    print("New table schema:\n")
    print(exports_table)

    # Use pandas integration to remove duplicate rows if any
    # I would rather not to change back and forth between pandas df and pyarrow table
    # But i didn't find a better option
    # I guess this is because arrow treats tables as columnar data
    # and pandas as row data, which allows it to identify duplicated rows
    print("Migrating to dataframe...\n")
    exports_table = exports_table.to_pandas(types_mapper=pd.ArrowDtype)
    print("\nDataframe dtypes:")
    print(exports_table.dtypes)
    print("\nDataframe shape:")
    print(exports_table.shape)

    exports_table = exports_table.drop_duplicates()
    print("Dataframe shape after duplicated rows removal:\n")
    print(exports_table.shape)
    print(exports_table.info())

    return exports_table


def load_exports(data: pd.DataFrame):
    data.to_parquet(
        config.Local_Dir.Exports["clean"],
        engine="pyarrow",
        index=False,
        partition_cols=["FECH"],
    )


if __name__ == "__main__":
    pass
