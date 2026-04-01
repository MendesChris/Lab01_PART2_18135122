import os
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Create and return a configured SparkSession."""
    builder = (
        SparkSession.builder
        .appName("TaxiPipeline")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
    )

    # Add JDBC driver if available (inside Docker image)
    jdbc_jar = "/app/jars/postgresql-42.7.3.jar"
    if os.path.exists(jdbc_jar):
        builder = builder.config("spark.jars", jdbc_jar)

    return builder.getOrCreate()
