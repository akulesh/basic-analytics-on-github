"""
TODO: fix possible parsing issues
import urllib.parse

urllib.parse.quote_plus("kx@jj5/g")
"""

import os
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine


class DBHandler:
    """PostgreSQL database handler"""

    def __init__(
        self,
        username: str = None,
        password: str = None,
        host: str = "0.0.0.0",
        port: int = 5432,
        db_name: str = "postgres",
        schema: str = "public",
    ):
        self.schema = schema
        self._initialize_connection(username, password, host, port, db_name)

    def _initialize_connection(
        self, username: str, password: str, host: str, port: int, db_name: str
    ):
        username = os.getenv("POSTGRES_USER", username)
        password = os.getenv("POSTGRES_PASSWORD", password)
        host = os.getenv("POSTGRES_HOST", host)
        port = os.getenv("POSTGRES_PORT", port)
        db_name = os.getenv("POSTGRES_DB", db_name)

        if not username or not password:
            raise ValueError("Database credentials not provided.")

        conn_string = f"postgresql://{username}:{password}@{host}:{port}/{db_name}"
        self.engine = create_engine(conn_string, pool_pre_ping=True)

    def execute(self, q: str):
        with self.engine.connect() as connection:
            return connection.execute(q)

    def drop_duplicates(self, table: str, key_columns: list):
        q = f"""
        DELETE FROM
            {self.schema}.{table} t1
                USING {self.schema}.{table} t2
        WHERE t1.updated_at < t2.updated_at
        """

        for col in key_columns:
            q += f"\n\tAND t1.{col} = t2.{col}"

        self.execute(q)

    def update_table(self, data: pd.DataFrame, table: str):
        columns = pd.read_sql(
            f"SELECT * FROM {self.schema}.{table} LIMIT 0;", con=self.engine
        ).columns

        data = data.copy()
        data.loc[:, "updated_at"] = datetime.now()
        columns = [c for c in columns if c in data.columns]
        data[columns].to_sql(
            table, con=self.engine, if_exists="append", index=False, schema=self.schema
        )

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
