"""Download NYC Yellow Taxi trip data from the TLC website."""

import os
import requests


DATA_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2024-01.parquet"
)

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Data", "raw")


def download_taxi_data(output_dir: str = RAW_DIR) -> str:
    """Download the Yellow Taxi parquet file and return the local path."""
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "yellow_tripdata_2024-01.parquet")

    if os.path.exists(output_path):
        print(f"Data already exists at {output_path}")
        return output_path

    print(f"Downloading data from {DATA_URL} ...")
    resp = requests.get(DATA_URL, stream=True, timeout=300)
    resp.raise_for_status()

    with open(output_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Downloaded to {output_path}")
    return output_path


if __name__ == "__main__":
    download_taxi_data()
