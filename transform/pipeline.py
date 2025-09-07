import os, sys
import duckdb

sys.path.append("/workspaces/talento_tech/ingest/")
import config, s3  # type: ignore


def main():
    s3.upload_object(
        os.path.join(config.Database.dir, config.Database.filename),
        config.S3.Bucket,
        config.Database.filename,
    )


if __name__ == "__main__":
    main()
