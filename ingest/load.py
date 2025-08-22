import os
import config
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import duckdb


def unify_csv(source_dir: str) -> duckdb.DuckDBPyRelation:

    data = duckdb.read_csv(source_dir + "*.csv", ignore_errors=True)
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
            for column in self.data.column_names:
                print(column, ": ", sep="", end="")
                print(self.data.column(column).type)
            print(self.data.shape)

    def purge_columns(self, columns: list) -> pa.Table:
        self.data = self.data.drop_columns(columns)
        return self

    def cast_dtypes(self, dtypes: list[tuple]) -> pa.Table:
        schema = pa.schema(dtypes)
        self.data = self.data.cast(target_schema=schema)
        return self

    def purge_nulls(self) -> pa.Table:
        self.data = self.data.drop_null()
        return self

# Here I use dataframes because I need a row approach, instead of a columnar one
    def table_to_dataframe(self) -> pd.DataFrame:
        self.data =  self.data.to_pandas(types_mapper=pd.ArrowDtype)
        return self

    def purge_duplicates(self) -> pd.DataFrame:
        self.data = self.data.drop_duplicates()
        return self

    def save_to_parquet(self, dir: str, filename: str):
        if self.data.empty:
            raise ValueError("Dataframe for exporting to parquet cannot be empty")

        config.check_dir_exists(dir)

        if not os.listdir(dir):
            print("Saving to parquet...\n")
            self.data.to_parquet(dir + filename, engine="pyarrow", index=False)
            print("Data saved succesfully!\n")
        else:
            print(
                f"There's data in {dir}.\n"
                "This process doesn't allows overwriting.\n"
                "Data was not saved!\n"
            )


if __name__ == "__main__":
    pass
