"""
TODO: fix possible parsing issues
import urllib.parse

urllib.parse.quote_plus("kx@jj5/g")
"""

import os

import pandas as pd
from sqlalchemy import create_engine, text

from src.utils.logger import logger


def create_db_session(
    db_username: str = None,
    db_password: str = None,
    db_host: str = None,
    db_port: int = None,
    db_name: str = None,
):
    username = db_username or os.getenv("POSTGRES_USER")
    password = db_password or os.getenv("POSTGRES_PASSWORD")
    host = db_host or os.getenv("POSTGRES_HOST")
    port = db_port or os.getenv("POSTGRES_PORT", db_port)
    db_name = db_name or os.getenv("POSTGRES_DB", db_name)

    if not username or not password:
        raise ValueError("Database credentials not provided.")

    conn_string = f"postgresql://{username}:{password}@{host}:{port}/{db_name}"
    return create_engine(conn_string, pool_pre_ping=True)


class DBHandler:
    """PostgreSQL database handler"""

    def __init__(
        self,
        db_username: str = None,
        db_password: str = None,
        db_host: str = None,
        db_port: int = None,
        db_name: str = None,
        db_schema: str = None,
    ):
        self.schema = db_schema or os.getenv("POSTGRES_SCHEMA")
        self.engine = create_db_session(
            db_username=db_username,
            db_password=db_password,
            db_host=db_host,
            db_port=db_port,
            db_name=db_name,
        )

    def execute(self, q: str):
        q = text(q)
        with self.engine.begin() as connection:
            return connection.execute(q)

    def drop_duplicates(self, table: str, key_columns: list, date_col: str = "updated_at"):
        logger.info(f"Cleaning '{table}' table from duplicates...")

        q = f"""
        DELETE FROM {self.schema}.{table} t1
            USING {self.schema}.{table} t2
        WHERE t1.{date_col} < t2.{date_col}
        """

        for col in key_columns:
            q += f"\tAND t1.{col} = t2.{col}"

        self.execute(q)
        logger.info("✅ Duplicates are dropped!")

    def update_table(self, data: pd.DataFrame, table: str):
        columns = pd.read_sql(
            f"SELECT * FROM {self.schema}.{table} LIMIT 0;", con=self.engine
        ).columns

        columns = [c for c in columns if c in data.columns]
        data[columns].to_sql(
            table, con=self.engine, if_exists="append", index=False, schema=self.schema
        )

    def clear_table(self, table):
        q = f"""DELETE FROM {self.schema}.{table}"""
        self.execute(q)

    def get_table_stats(self):
        q = f"""
        SELECT
            relname AS table_name,
            n_live_tup AS row_count,
            pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
            pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) AS index_size
        FROM pg_stat_user_tables
        WHERE schemaname = '{self.schema}';
        """

        return pd.read_sql(q, con=self.engine)

    def read_sql(self, query: str, **kwargs):
        return pd.read_sql(query, con=self.engine, **kwargs)
