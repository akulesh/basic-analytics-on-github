import argparse
import time
import os

from prefect import flow

from src.etl.extractors.repo_metadata import RepoMetadataExtractor
from src.etl.transformer import DataTransformer
from src.etl.aggregator import DataAggregator
from src.utils.db_handler import DBHandler
from src.utils.api import SUPPORTED_LANGUAGES, get_languages


def get_db_session(db_config: dict = None):
    db_config = db_config or {}
    return DBHandler(**db_config)


@flow(
    name="repo-metadata-extraction",
    flow_run_name="Extraction Subflow",
    log_prints=True,
)
def extract(
    output_dir: str,
    start_date: str = "2020-01-01",
    end_date: str = None,
    languages: str | list = None,
    overwrite_existed_files: bool = False,
    db_config: dict = None,
    min_stars_count: int = 1,
    api_token: str = None,
):
    db = get_db_session(db_config)
    extractor = RepoMetadataExtractor(
        output_dir=output_dir, min_stars_count=min_stars_count, api_token=api_token, db=db)
    extractor.run(
        start_date=start_date,
        end_date=end_date,
        languages=languages or SUPPORTED_LANGUAGES,
        overwrite=overwrite_existed_files,
    )


@flow(
    name="repo-metadata-transformation",
    flow_run_name="Transformation Subflow",
    log_prints=True,
)
def transform_load(
    input_dir: str,
    start_date: str,
    end_date: str = None,
    languages: list = None,
    db_config: dict = None,
):
    db = get_db_session(db_config)
    transformer = DataTransformer(db)
    transformer.run(
        input_dir=input_dir, start_date=start_date, end_date=end_date, languages=languages
    )

    aggregator = DataAggregator(db)
    aggregator.run(start_date, end_date)

    time.sleep(1)
    stats = db.get_table_stats()
    print(f"Tables info:\n{stats}")


@flow(name="etl-flow", flow_run_name="ETL Flow", log_prints=True)
def run_etl(
    source_dir: str = os.getenv("DOCKER_PATH_RAW_DATA", "/volumes/data"),
    start_date: str = "2020-01-01",
    end_date: str = "2020-01-01",
    languages: str | list = None,
    skip_extraction: bool = False,
    overwrite_existed_files: bool = False,
    min_stars_count: int = 1,
    api_token: str = None,
    db_config: dict = None
):
    languages = get_languages(languages)
    source_dir = os.path.join(source_dir, "repos")
    print(f"Source directory: {source_dir}")

    if not skip_extraction:
        extract(
            output_dir=source_dir,
            start_date=start_date,
            end_date=end_date,
            languages=languages,
            overwrite_existed_files=overwrite_existed_files,
            min_stars_count=min_stars_count,
            api_token=api_token,
            db_config=db_config,
        )

    transform_load(
        input_dir=source_dir,
        start_date=start_date,
        end_date=end_date,
        languages=languages,
        db_config=db_config,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", default="2020-01-01")
    parser.add_argument("--end_date", default=None)
    parser.add_argument("--source_dir", default="./tmp/data")
    parser.add_argument("--languages", default=None)
    parser.add_argument("--skip_extraction", action="store_true")
    parser.add_argument("--overwrite_existed_files", action="store_true")
    parser.add_argument("--db_username", default=None)
    parser.add_argument("--db_password", default=None)
    parser.add_argument("--db_host", default="0.0.0.0")
    parser.add_argument("--db_port", type=int, default=5432)
    parser.add_argument("--db_name", default=os.getenv("POSTGRES_DB", "postgres"))
    args = parser.parse_args()

    run_etl(
        start_date=args.start_date,
        end_date=args.end_date,
        source_dir=args.source_dir,
        languages=args.languages,
        skip_extraction=args.skip_extraction,
        overwrite_existed_files=args.overwrite_existed_files,
        db_config={
            "db_username": args.db_username,
            "db_password": args.db_password,
            "db_host": args.db_host,
            "db_port": args.db_port,
            "db_name": args.db_name,
        },
    )
