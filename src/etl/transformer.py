import argparse
import os
import time
from datetime import datetime

import pandas as pd
from tqdm import tqdm

from src.etl.aggregator import DataAggregator
from src.utils.api import SUPPORTED_LANGUAGES
from src.utils.db_handler import DBHandler
from src.utils.logger import logger


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
            "lang_alias",
            "url",
            "topics",
            "archived",
            "has_license",
            "has_description",
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
        data["has_topic"] = (data["topics"] != "").astype(int)

        return data

    def update_tables(self, data: pd.DataFrame, mapping: dict):
        for table, func in mapping.items():
            df = func(data)
            self.db.update_table(df, table=table)

    @staticmethod
    def get_languages_from_path(path: str):
        values = [x.split("=")[-1] for x in os.listdir(path)]
        return [x for x in values if x in SUPPORTED_LANGUAGES]

    def run(self, input_dir: str, start_date: str, end_date: str = None, languages: list = None):
        mapping = {
            "repo": {"get_table": self.get_repo_table, "key_columns": ["id"]},
            "repo_topic": {"get_table": self.get_topic_table, "key_columns": ["repo_id", "topic"]},
            "owner": {"get_table": self.get_owner_table, "key_columns": ["url"]},
        }
        _mapping = {table: val["get_table"] for table, val in mapping.items()}

        end_date = end_date or start_date
        dates = pd.date_range(start=start_date, end=end_date, freq="D").strftime("%Y-%m-%d")

        languages = languages or self.get_languages_from_path(input_dir)
        for lang in languages:
            logger.info(f"🕐 Language: {lang}")

            for date in tqdm(dates, total=len(dates)):
                filename = os.path.join(input_dir, f"language={lang}", f"{date}.parquet")
                if os.path.exists(filename):
                    data = self.process_file(filename)
                    data.loc[:, "lang_alias"] = lang
                    self.update_tables(data, _mapping)
                else:
                    logger.warning(f"File '{filename}' does not exist.")

            logger.info(f"✅ Processing completed for '{lang}'!")

        for table, val in mapping.items():
            self.db.drop_duplicates(table=table, key_columns=val["key_columns"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir")
    parser.add_argument("--start_date")
    parser.add_argument("--end_date")
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
        db_username=args.db_username,
        db_password=args.db_password,
        db_host=args.db_host,
        db_port=args.db_port,
        db_name=args.db_name,
        db_schema="github",
    )

    transformer = DataTransformer(db)
    transformer.run(
        input_dir=args.input_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        languages=args.languages,
    )

    aggregator = DataAggregator(db)
    aggregator.run(args.start_date, args.end_date)

    time.sleep(1)
    stats = db.get_table_stats()
    logger.info(f"\tTables info:\n\t{stats}")


if __name__ == "__main__":
    main()
