import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "taxi_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "admin"),
    "port": int(os.getenv("DB_PORT", 5432)),
}

JDBC_URL = (
    f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/"
    f"{DB_CONFIG['database']}"
)

JDBC_PROPERTIES = {
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "driver": "org.postgresql.Driver",
}
