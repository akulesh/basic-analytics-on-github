import argparse
from datetime import datetime
from glob import glob
import time

import pandas as pd
from tqdm import tqdm
from prefect import flow

from src.data_processing.db_handler import DBHandler
from src.data_processing.aggregator import DataAggregator
from src.utils import logger, get_languages


class DataTransformer:
    """Transform raw data and upload into database"""

    def __init__(self, db: DBHandler):
        self.db = db
        self.schema = db.schema

    @staticmethod
    def process_file(filename):
        columns = [
            "id",
            "name",
            "owner",
            "html_url",
            "language",
            "description",
            "topics",
            "license",
            "archived",
            "has_wiki",
            "size",
            "default_branch",
            "stargazers_count",
            "watchers_count",
            "forks_count",
            "open_issues_count",
            "created_at",
            "pushed_at",
        ]
        data = pd.read_parquet(filename, columns=columns)
        data["owner_url"] = data["owner"].apply(lambda x: x.get("html_url") if x else x)
        data["owner"] = data["owner"].apply(lambda x: x.get("login") if x else x)
        data["license"] = (
            data["license"].apply(lambda x: x.get("name") if x else x).fillna("__NA__")
        )
        data["has_license"] = (data["license"] != "__NA__").astype(int)
        data["has_description"] = (~data["description"].isnull()).astype(int)
        data["has_topic"] = (~data["topics"].isnull()).astype(int)
        data["has_wiki"] = data["has_wiki"].astype(int)
        data["archived"] = data["archived"].astype(int)
        data["description"] = data["description"].astype("str")
        data["created_at"] = pd.to_datetime(data["created_at"]).dt.round("D").dt.tz_localize(None)
        data["pushed_at"] = pd.to_datetime(data["pushed_at"]).dt.round("D").dt.tz_localize(None)
        data["days_since_creation"] = (datetime.now() - data["created_at"]).dt.days
        data["days_since_last_commit"] = (datetime.now() - data["pushed_at"]).dt.days

        data = data.drop_duplicates(subset="id")
        data = data.rename(
            columns={
                "stargazers_count": "stars",
                "watchers_count": "watchers",
                "forks_count": "forks",
                "open_issues_count": "open_issues",
                "html_url": "url",
                "created_at": "creation_date",
                "pushed_at": "last_commit_date",
            }
        )
        data = data.reset_index(drop=True)

        return data

    @staticmethod
    def get_topic_table(data: pd.DataFrame):
        columns = ["id", "topics", "language"]
        data = data[columns].explode("topics")
        data["topics"] = data["topics"].astype(str).str.lower().replace({"nan": "__NA__"})
        data = data.rename(columns={"topics": "topic", "id": "repo_id"})

        return data

    @staticmethod
    def get_owner_table(data: pd.DataFrame):
        columns = ["id", "owner", "owner_url"]
        data = data[columns]

        data = data.groupby(["owner", "owner_url"]).count().reset_index()
        data = data.rename(columns={"owner": "name", "owner_url": "url", "id": "n_repos"})
        return data

    @staticmethod
    def get_repo_table(data: pd.DataFrame):
        columns = [
            "id",
            "name",
            "owner",
            "language",
            "url",
            "topics",
            "archived",
            "has_license",
            "has_description",
            "has_topic",
            "has_wiki",
            "default_branch",
            "license",
            "description",
            "size",
            "stars",
            "watchers",
            "forks",
            "open_issues",
            "days_since_creation",
            "days_since_last_commit",
            "creation_date",
            "last_commit_date",
        ]
        data = data[columns].copy()

        def transform(value, max_length: int = 255):
            value = "|".join(value)

            if len(value) > max_length:
                value = value[:max_length]
                topics = value.split("|")[:-1]
                value = "|".join(topics)

            return value

        data.loc[:, "topics"] = data["topics"].apply(transform)

        return data

    def update_tables(self, data: pd.DataFrame, mapping: dict):
        for table, func in mapping.items():
            df = func(data)
            self.db.update_table(df, table=table)

    def run(self, input_dir: str, languages: list, limit=None):
        mapping = {
            "repo": {"get_table": self.get_repo_table, "key_columns": ["id"]},
            "repo_topic": {"get_table": self.get_topic_table, "key_columns": ["repo_id", "topic"]},
            "owner": {"get_table": self.get_owner_table, "key_columns": ["url"]},
        }

        for lang in languages:
            logger.info(f"🕐 Language: {lang}")
            files = glob(f"{input_dir}/language={lang}/*.parquet")

            if limit:
                files = files[:limit]

            _mapping = {table: val["get_table"] for table, val in mapping.items()}
            for f in tqdm(files, total=len(files)):
                data = self.process_file(f)
                self.update_tables(data, _mapping)

        for table, val in mapping.items():
            logger.info(f"Cleaning table '{table}' from duplicates...")
            self.db.drop_duplicates(table=table, key_columns=val["key_columns"])
            logger.info("Duplicated are dropped.")


@flow(flow_run_name="Github Data Transformation Flow", log_prints=True)
def run_transformation_flow(db, input_dir: str, languages: list, limit=None):
    transformer = DataTransformer(db)
    transformer.run(input_dir=input_dir, languages=languages, limit=limit)

    aggregator = DataAggregator(db)
    aggregator.run()

    time.sleep(1)
    stats = db.get_table_stats()
    logger.info(f"\tTables info:\n\t{stats}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir")
    parser.add_argument("--languages", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--db_username", default=None)
    parser.add_argument("--db_password", default=None)
    parser.add_argument("--db_host", default="0.0.0.0")
    parser.add_argument("--db_port", type=int, default=5432)
    parser.add_argument("--db_name", default="postgres")

    args = parser.parse_args()
    logger.info(f"Args: {args}")

    db = DBHandler(
        schema="github",
        username=args.db_username,
        password=args.db_password,
        host=args.db_host,
        port=args.db_port,
        db_name=args.db_name,
    )

    run_transformation_flow(
        db, input_dir=args.input_dir, languages=get_languages(args.languages), limit=args.limit
    )


if __name__ == "__main__":
    main()
