import argparse
import os
import time

from prefect import flow

import src.etl.extractors.constants as consts
from src.etl.aggregator import DataAggregator
from src.etl.extractors.repo_metadata import RepoMetadataExtractor
from src.etl.extractors.repo_structure import (
    DEFAULT_PATH_PATTERN,
    RepoStructureExtractor,
)
from src.etl.transformer import DataTransformer
from src.utils.api import get_languages
from src.utils.db_handler import DBHandler


def get_db_session(db_config: dict = None):
    db_config = db_config or {}
    return DBHandler(**db_config)


@flow(
    name="repo-metadata-extraction",
    flow_run_name="Metadata extraction subflow",
    log_prints=True,
)
def extract_metadata(
    output_dir: str,
    start_date: str = "2020-01-01",
    end_date: str = None,
    languages: str | list = None,
    overwrite_existed_files: bool = False,
    pagination_timeout: int = consts.PAGINATION_TIMEOUT,
    min_stars_count: int = consts.MIN_STARS_COUNT,
    db_config: dict = None,
    api_token: str = None,
):
    db = get_db_session(db_config)
    extractor = RepoMetadataExtractor(
        output_dir=output_dir,
        min_stars_count=min_stars_count,
        pagination_timeout=pagination_timeout,
        api_token=api_token,
        db=db,
    )
    extractor.run(
        start_date=start_date,
        end_date=end_date,
        languages=languages,
        overwrite=overwrite_existed_files,
    )


@flow(
    name="repo-structure-extraction",
    flow_run_name="Structure extraction subflow",
    log_prints=True,
)
def extract_structure(
    output_dir: str,
    start_date: str = "2020-01-01",
    end_date: str = None,
    languages: str | list = None,
    min_stars_count: int = 1,
    retry_attempts: int = consts.RETRY_ATTEMPTS,
    pagination_timeout: int = consts.PAGINATION_TIMEOUT,
    path_pattern: str = DEFAULT_PATH_PATTERN,
    overwrite_existed_files: bool = False,
    limit: int = None,
    api_token: str = None,
    db_config: dict = None,
):
    db = get_db_session(db_config)
    extractor = RepoStructureExtractor(
        output_dir,
        min_stars_count=min_stars_count,
        retry_attempts=retry_attempts,
        pagination_timeout=pagination_timeout,
        api_token=api_token,
        db=db,
    )

    extractor.run(
        start_date=start_date,
        end_date=end_date,
        languages=languages,
        overwrite=overwrite_existed_files,
        path_pattern=path_pattern,
        limit=limit,
    )


@flow(
    name="repo-metadata-transformation",
    flow_run_name="Transformation subflow",
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


@flow(name="etl-flow", flow_run_name="ETL flow", log_prints=True)
def run_etl(
    source_dir: str = os.getenv("DOCKER_PATH_RAW_DATA", "/volumes/data"),
    start_date: str = "2020-01-01",
    end_date: str = "2020-01-01",
    languages: str | list = None,
    extract_repo_metadata: bool = True,
    transform_repo_metadata: bool = True,
    extract_repo_structure: bool = True,
    overwrite_existed_files: bool = False,
    metadata_min_stars_count: int = 1,
    structure_min_stars_count: int = 10,
    pagination_timeout: int = consts.PAGINATION_TIMEOUT,
    api_token: str = None,
    db_config: dict = None,
):
    languages = get_languages(languages)
    source_dir = os.path.join(source_dir, "repos")
    print(f"Source directory: {source_dir}")

    if extract_repo_metadata:
        extract_metadata(
            output_dir=source_dir,
            start_date=start_date,
            end_date=end_date,
            languages=languages,
            overwrite_existed_files=overwrite_existed_files,
            min_stars_count=metadata_min_stars_count,
            pagination_timeout=pagination_timeout,
            api_token=api_token,
            db_config=db_config,
        )

    if transform_repo_metadata:
        transform_load(
            input_dir=os.path.join(source_dir, "metadata"),
            start_date=start_date,
            end_date=end_date,
            languages=languages,
            db_config=db_config,
        )

    if extract_repo_structure:
        extract_structure(
            output_dir=source_dir,
            start_date=start_date,
            end_date=end_date,
            languages=languages,
            overwrite_existed_files=overwrite_existed_files,
            min_stars_count=structure_min_stars_count,
            pagination_timeout=pagination_timeout,
            api_token=api_token,
            db_config=db_config,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", default="2020-01-01")
    parser.add_argument("--end_date", default=None)
    parser.add_argument("--source_dir", default="./tmp/data")
    parser.add_argument("--languages", default=None)
    parser.add_argument("--extract_repo_metadata", action="store_true", default=True)
    parser.add_argument("--transform_repo_metadata", action="store_true", default=True)
    parser.add_argument("--overwrite_existed_files", action="store_true")
    parser.add_argument("--pagination_timeout", type=int, default=consts.PAGINATION_TIMEOUT)
    parser.add_argument("--min_stars_count", type=int, default=1)
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
        extract_repo_metadata=args.extract_repo_metadata,
        transform_repo_metadata=args.transform_repo_metadata,
        overwrite_existed_files=args.overwrite_existed_files,
        pagination_timeout=args.pagination_timeout,
        min_stars_count=args.min_stars_count,
        db_config={
            "db_username": args.db_username,
            "db_password": args.db_password,
            "db_host": args.db_host,
            "db_port": args.db_port,
            "db_name": args.db_name,
        },
    )
