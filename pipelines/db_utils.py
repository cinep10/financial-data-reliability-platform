import os
from sqlalchemy import create_engine, text


def get_engine():
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("DB_URL environment variable is required")
    return create_engine(db_url, future=True)


def execute_sql(conn, sql: str, params=None):
    return conn.execute(text(sql), params or {})
