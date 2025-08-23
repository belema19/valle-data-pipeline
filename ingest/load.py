import os
import config
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import duckdb


def unify_csv(source_dir: str) -> duckdb.DuckDBPyRelation:
    """Creates a duckdb relation from all .csv files in a given directory.

    Args:
        source_dir (str): directory where the .csv files are located.

    Returns:
        A duckdb relation

    Raises:
        Exception: higher class exception that avoid error propagation
          since this error is non-recoverable.
    """
    try:
        data = duckdb.read_csv(source_dir + "*.csv", ignore_errors=True)
        return data
    except Exception as e:
        print(
            f"There was an error unifying the .csv file: {e}\n"
            "This error is non-recoverable!\n"
        )
        raise


class Load:
    """Collection of methods for cleaning and loading data.

    This class was made for allowing chaining methods

    Typical example:
        data = Load(data=duckdb_rel)
        data = (
            data.get_pyarrow_table()
            .purge_nulls()
        )
    """

    def __init__(self, data: duckdb.DuckDBPyRelation):
        """Class contructor.

        This constructor instantiates a duckdb relation.

        Args:
            data (DuckDBPyRelation): data to clean and load.
        """
        self.data = data

    def get_pyarrow_table(self) -> pa.Table:
        """Creates a pyarrow table from a DuckDBPyRelation."""
        self.data = self.data.arrow()
        return self

    def show_info(self: pa.Table | pd.DataFrame) -> None:
        """Shows informationo about the actual Load instance.

        It can be a pyarrow table or pandas dataframe.
        """
        if isinstance(self.data, pd.DataFrame):
            print(self.data.info())
            print(self.data.shape)
        else:
            for column in self.data.column_names:
                print(column, ": ", sep="", end="")
                print(self.data.column(column).type)
            print(self.data.shape)

    def purge_columns(self: pa.Table, columns: list[str]) -> pa.Table:
        """Drops a list of given columns.

        Instance must be a pyarrow table.

        Args:
            columns (list[str]): columns to drop.
        """
        self.data = self.data.drop_columns(columns)
        return self

    def cast_dtypes(self: pa.Table, dtypes: list[tuple]) -> pa.Table:
        """Casts dtypes.

        Instance must be a pyarrow table.

        Args:
            dtypes (list[tuple]): column-dtype pairs to cast.
        """
        schema = pa.schema(dtypes)
        self.data = self.data.cast(target_schema=schema)
        return self

    def purge_nulls(self: pa.Table) -> pa.Table:
        """Purges all null values.

        Instance must be a pyarrow table.
        """
        self.data = self.data.drop_null()
        return self

    # Here I used dataframes because I needed a row approach, instead of a columnar one
    def table_to_dataframe(self: pa.Table) -> pd.DataFrame:
        """Casts pyarrow table to pandas dataframe."""
        self.data = self.data.to_pandas(types_mapper=pd.ArrowDtype)
        return self

    def purge_duplicates(self: pd.DataFrame) -> pd.DataFrame:  # type: ignore
        """Purges duplicated rows.

        Instance must be pandas dataframe.
        """
        self.data = self.data.drop_duplicates()
        return self

    def save_to_parquet(self: pd.DataFrame, dir: str, filename: str):  # type: ignore
        """save data to parquet file.

        Instance must be a pandas dataframe. If there's data in the given
          storage directory nothing is saved.

        Args:
            dir (str): storage directory.
            filename (str): parquet file filename.
        """
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
