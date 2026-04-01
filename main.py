"""NYC Taxi Data Engineering Pipeline - Medallion Architecture.

Orchestrates the full Bronze -> Silver -> Gold pipeline with
Great Expectations validation on the raw layer.
"""

from src.spark_session import get_spark
from src.download_data import download_taxi_data
from src.bronze import load_raw_data, save_raw_csv
from src.silver import (
    clean_data,
    create_dim_payment,
    create_dim_vendor,
    create_dim_date,
    create_dim_location,
    create_fact_trips,
)
from src.gold import load_to_postgres
from src.validate import run_validation


def main():
    # ========================
    # DOWNLOAD DATA
    # ========================
    parquet_path = download_taxi_data()

    spark = get_spark()

    # ========================
    # BRONZE
    # ========================
    df_raw = load_raw_data(spark, parquet_path)
    csv_path = save_raw_csv(df_raw)

    # ========================
    # GREAT EXPECTATIONS
    # ========================
    run_validation(csv_path)

    # ========================
    # SILVER
    # ========================
    df_clean = clean_data(df_raw)

    # ========================
    # GOLD - DIMENSIONS
    # ========================
    dim_payment = create_dim_payment(df_clean)
    dim_vendor = create_dim_vendor(df_clean)
    dim_date = create_dim_date(df_clean)
    dim_location = create_dim_location(df_clean)

    # ========================
    # GOLD - FACT
    # ========================
    fact_trips = create_fact_trips(
        df_clean,
        dim_payment,
        dim_vendor,
        dim_date,
        dim_location,
    )

    # ========================
    # LOAD TO POSTGRES
    # ========================
    load_to_postgres(dim_payment, "dim_payment")
    load_to_postgres(dim_vendor, "dim_vendor")
    load_to_postgres(dim_date, "dim_date")
    load_to_postgres(dim_location, "dim_location")
    load_to_postgres(fact_trips, "fact_trips")

    print("Pipeline completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
