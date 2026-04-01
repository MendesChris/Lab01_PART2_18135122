"""Bronze layer: load raw data without transformations."""

import os
import shutil
from pyspark.sql import DataFrame, SparkSession


RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Data", "raw")

# Limit rows to avoid OOM in constrained Docker environments
MAX_ROWS = 100_000


def load_raw_data(spark: SparkSession, path: str) -> DataFrame:
    """Read raw parquet data into a Spark DataFrame (Bronze layer).

    Samples up to MAX_ROWS to keep memory usage manageable.
    """
    print(f"[Bronze] Loading raw data from {path} ...")

    if path.endswith(".csv"):
        df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    else:
        df = spark.read.parquet(path)

    total = df.count()
    print(f"[Bronze] Source has {total} rows")

    if total > MAX_ROWS:
        fraction = MAX_ROWS / total
        df = df.sample(fraction=fraction, seed=42).limit(MAX_ROWS)
        print(f"[Bronze] Sampled down to {df.count()} rows")

    return df


def save_raw_csv(df: DataFrame, output_dir: str = RAW_DIR) -> str:
    """Persist bronze data as a single CSV using Spark (no toPandas)."""
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "taxi_trips.csv")

    # Write to a temp directory, then merge into a single CSV
    tmp_dir = os.path.join(output_dir, "_tmp_csv")
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)

    # Find the single part file and move it
    for fname in os.listdir(tmp_dir):
        if fname.startswith("part-") and fname.endswith(".csv"):
            shutil.move(os.path.join(tmp_dir, fname), output_path)
            break

    shutil.rmtree(tmp_dir, ignore_errors=True)
    print(f"[Bronze] Saved CSV to {output_path}")
    return output_path
