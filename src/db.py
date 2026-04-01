import psycopg2
import time
from src.config import DB_CONFIG


def get_connection():
    """Return a psycopg2 connection, retrying up to 10 times."""
    for i in range(10):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print("Connected to PostgreSQL")
            return conn
        except Exception:
            print(f"Waiting for DB... (attempt {i + 1}/10)")
            time.sleep(3)

    raise Exception("Could not connect to DB after 10 attempts")
