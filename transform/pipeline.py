# TODO:
# upload db.duckdb to S3
# graphic some heatmaps or bivariate distribution (hscode, value)
# create machine learning model
import os, sys

sys.path.append("/workspaces/talento_tech/ingest/")
import config, s3  # type: ignore


def main():
    db_dir = config.Database.dir
    db_filename = config.Database.filename
    db_path = os.path.join(db_dir, db_filename)

    if not os.path.exists(db_path):
        os.makedirs(db_dir, exist_ok=True)

    s3.upload_object(db_path, config.S3.Bucket, db_filename)


if __name__ == "__main__":
    main()
