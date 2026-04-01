"""Gold layer: load star-schema tables into PostgreSQL."""

import io
from src.db import get_connection


def load_to_postgres(df, table_name: str) -> None:
    """Stream a Spark DataFrame into PostgreSQL using COPY."""
    print(f"[Gold] Loading {table_name} to PostgreSQL ...")

    conn = get_connection()
    cur = conn.cursor()

    # Drop and recreate
    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
    conn.commit()

    # Build CREATE TABLE from Spark schema
    type_map = {
        "IntegerType()": "INTEGER",
        "LongType()": "BIGINT",
        "DoubleType()": "DOUBLE PRECISION",
        "FloatType()": "REAL",
        "StringType()": "TEXT",
        "TimestampType()": "TIMESTAMP",
        "TimestampNTZType()": "TIMESTAMP",
        "DateType()": "DATE",
        "BooleanType()": "BOOLEAN",
        "ShortType()": "SMALLINT",
    }

    columns = []
    for field in df.schema.fields:
        spark_type = str(field.dataType)
        pg_type = type_map.get(spark_type, "TEXT")
        columns.append(f'"{field.name}" {pg_type}')

    create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
    cur.execute(create_sql)
    conn.commit()

    # Stream data via COPY
    buffer = io.StringIO()
    row_count = 0

    for row in df.toLocalIterator():
        line = ",".join(
            "" if v is None else str(v)
            for v in row
        )
        buffer.write(line + "\n")
        row_count += 1

        if buffer.tell() > 5_000_000:  # flush every ~5 MB
            buffer.seek(0)
            cur.copy_expert(
                f"COPY {table_name} FROM STDIN WITH CSV NULL ''", buffer
            )
            buffer.seek(0)
            buffer.truncate(0)

    # Final flush
    if buffer.tell() > 0:
        buffer.seek(0)
        cur.copy_expert(
            f"COPY {table_name} FROM STDIN WITH CSV NULL ''", buffer
        )
    buffer.close()

    conn.commit()
    cur.close()
    conn.close()

    print(f"[Gold] Loaded {row_count} rows into {table_name}")
