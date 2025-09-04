import os, sys
import duckdb

sys.path.append("/workspaces/talento_tech/ingest/")
import config, s3  # type: ignore


def main():
    db_dir = config.Database.dir
    db_filename = config.Database.filename
    db_path = os.path.join(db_dir, db_filename)

    ddb = duckdb.connect(database=db_path)

    ddb.execute(
        """EXPORT DATABASE '/workspaces/talento_tech/data/transformed/parquet/' (FORMAT parquet);"""
    )

    for file in os.listdir(db_dir + "parquet/"):
        print(file)
        s3.upload_object(db_dir + "parquet/" + file, config.S3.Bucket, "database/" + file)

if __name__ == "__main__":
    main()
