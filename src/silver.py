"""Silver layer: data cleaning and dimension / fact table creation."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth, dayofweek,
    row_number, unix_timestamp,
)
from pyspark.sql.window import Window


# ---------------------
# SILVER - cleaning
# ---------------------

def clean_data(df: DataFrame) -> DataFrame:
    """Remove nulls, invalid trips, duplicates, and create duration column."""
    print("[Silver] Cleaning data ...")

    df = df.dropDuplicates()
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                            "trip_distance", "fare_amount", "total_amount"])

    # Filter invalid trips
    df = df.filter(col("trip_distance") > 0)
    df = df.filter(col("fare_amount") > 0)

    # Cast datetime columns
    df = df.withColumn("pickup_datetime",
                       col("tpep_pickup_datetime").cast("timestamp"))
    df = df.withColumn("dropoff_datetime",
                       col("tpep_dropoff_datetime").cast("timestamp"))

    # Trip duration in minutes
    df = df.withColumn(
        "trip_duration_min",
        (unix_timestamp(col("dropoff_datetime"))
         - unix_timestamp(col("pickup_datetime"))) / 60,
    )

    # Remove outliers
    df = df.filter(col("trip_duration_min") > 0)
    df = df.filter(col("trip_duration_min") < 300)

    print(f"[Silver] {df.count()} rows after cleaning")
    return df


# ---------------------
# DIMENSIONS
# ---------------------

def create_dim_payment(df: DataFrame) -> DataFrame:
    window = Window.orderBy("payment_type")
    return (
        df.select("payment_type")
        .distinct()
        .withColumn("payment_id", row_number().over(window))
    )


def create_dim_vendor(df: DataFrame) -> DataFrame:
    window = Window.orderBy("VendorID")
    return (
        df.select("VendorID")
        .distinct()
        .withColumn("vendor_key", row_number().over(window))
    )


def create_dim_date(df: DataFrame) -> DataFrame:
    window = Window.orderBy("date")
    return (
        df.withColumn("date", to_date(col("pickup_datetime")))
        .select("date")
        .distinct()
        .withColumn("date_id", row_number().over(window))
        .withColumn("year", year("date"))
        .withColumn("month", month("date"))
        .withColumn("day", dayofmonth("date"))
        .withColumn("weekday", dayofweek("date"))
    )


def create_dim_location(df: DataFrame) -> DataFrame:
    window = Window.orderBy("PULocationID", "DOLocationID")
    return (
        df.select("PULocationID", "DOLocationID")
        .distinct()
        .withColumn("location_id", row_number().over(window))
    )


# ---------------------
# FACT TABLE
# ---------------------

def create_fact_trips(
    df: DataFrame,
    dim_payment: DataFrame,
    dim_vendor: DataFrame,
    dim_date: DataFrame,
    dim_location: DataFrame,
) -> DataFrame:
    df = df.withColumn("date", to_date(col("pickup_datetime")))

    fact = (
        df.join(dim_payment, "payment_type", "left")
          .join(dim_vendor, "VendorID", "left")
          .join(dim_date, "date", "left")
          .join(dim_location, ["PULocationID", "DOLocationID"], "left")
    )

    return fact.select(
        col("vendor_key"),
        col("payment_id"),
        col("date_id"),
        col("location_id"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("passenger_count"),
        col("trip_distance"),
        col("fare_amount"),
        col("tip_amount"),
        col("total_amount"),
        col("trip_duration_min"),
    )
