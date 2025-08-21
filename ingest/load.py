import os
import config
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import duckdb


def unify_csv(source_dir: str) -> duckdb.DuckDBPyRelation:
    data = duckdb.read_csv(source_dir + "*.csv")
    return data


class Load:
    def __init__(self, data: duckdb.DuckDBPyRelation):
        self.data = data

    def __str__(self):
        return f"{type(self.data)}"

    def get_pyarrow_table(self) -> pa.Table:
        self.data = self.data.arrow()
        return self

    def show_info(self):
        if isinstance(self.data, pd.DataFrame):
            print(self.data.info())
            print(self.data.shape)
        else:
            print(self.data)
            print(self.data.shape)

    def purge_columns(self, columns: list) -> pa.Table:
        self.data = self.data.drop_columns(columns)
        return self

    def cast_dtypes(self, dtypes: dict) -> pa.Table:
        schema = pa.schema(dtypes)
        self.data = self.data.cast(target_schema=schema)
        return self

    def purge_nulls(self) -> pa.Table:
        self.data = self.data.drop_null()
        return self

# TODO
# Use pyarrow to get rid of duplicates

    def table_to_dataframe(self) -> pd.DataFrame:
        self.data =  self.data.to_pandas(types_mapper=pd.ArrowDtype)
        return self

    def purge_duplicates(self) -> pd.DataFrame:
        self.data = self.data.drop_duplicates()
        return self

    def save_to_parquet(self, dir: str, filename: str):
        if self.data.empty:
            raise ValueError("Dataframe for exporting to parquet is empty")

        self.data.to_parquet(dir + filename, engine="pyarrow", index=False)


if __name__ == "__main__":
    pass
