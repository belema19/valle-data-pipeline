import os
from s3 import (
    download_raw_data
)

def main():
    if not os.listdir('./data/raw-data'):
        download_raw_data()


if __name__ == "__main__":
    main()
