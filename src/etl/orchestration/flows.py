import os
import time

from prefect import flow

import src.etl.extraction.constants as consts
from src.etl.extraction import (
    RepoContentExtractor,
    RepoInfoExtractor,
    RepoStructureExtractor,
)
from src.etl.transformation.aggregator import DataAggregator
from src.etl.transformation.transformer import DataTransformer
from src.utils.api import get_languages
from src.utils.db_handler import get_db_handler


@flow(
    name="repo-info-extraction",
    flow_run_name="Info extraction subflow",
    log_prints=True,
)
def extract_repo_info(
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
    db = get_db_handler(db_config)
    extractor = RepoInfoExtractor(db=db, api_token=api_token, pagination_timeout=pagination_timeout)
    extractor.run(
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date,
        languages=languages,
        min_stars_count=min_stars_count,
        overwrite=overwrite_existed_files,
    )


@flow(
    name="repo-info-transformation",
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
    db = get_db_handler(db_config)
    transformer = DataTransformer(db)
    transformer.run(
        input_dir=input_dir, start_date=start_date, end_date=end_date, languages=languages
    )

    aggregator = DataAggregator(db)
    aggregator.run(start_date, end_date)

    time.sleep(1)
    stats = db.get_table_stats()
    print(f"Tables info:\n{stats}")


@flow(name="etl-repo-info-flow", flow_run_name="ETL: Repo info flow", log_prints=True)
def run_repo_info_flow(
    source_dir: str = os.getenv("DOCKER_PATH_RAW_DATA", "/volumes/data"),
    start_date: str = "2020-01-01",
    end_date: str = "2020-01-01",
    languages: str | list = None,
    extract: bool = True,
    transform: bool = True,
    overwrite_existed_files: bool = False,
    min_stars_count: int = 1,
    pagination_timeout: int = consts.PAGINATION_TIMEOUT,
    api_token: str = None,
    db_config: dict = None,
):
    languages = get_languages(languages)
    source_dir = os.path.join(source_dir, "repos")
    print(f"Source directory: {source_dir}")

    if extract:
        extract_repo_info(
            output_dir=os.path.join(source_dir, "info"),
            start_date=start_date,
            end_date=end_date,
            languages=languages,
            overwrite_existed_files=overwrite_existed_files,
            min_stars_count=min_stars_count,
            pagination_timeout=pagination_timeout,
            api_token=api_token,
            db_config=db_config,
        )

    if transform:
        transform_load(
            input_dir=os.path.join(source_dir, "info"),
            start_date=start_date,
            end_date=end_date,
            languages=languages,
            db_config=db_config,
        )


@flow(
    name="repo-structure-extraction",
    flow_run_name="Structure extraction subflow",
    log_prints=True,
)
def extract_repo_structure(
    output_dir: str,
    start_date: str = "2020-01-01",
    end_date: str = None,
    languages: str | list = None,
    min_stars_count: int = 1,
    path_pattern: str = None,
    overwrite_existed_files: bool = False,
    limit: int = None,
    db_config: dict = None,
    **kwargs,
):
    db = get_db_handler(db_config)
    extractor = RepoStructureExtractor(db=db, **kwargs)

    extractor.run(
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date,
        languages=languages,
        min_stars_count=min_stars_count,
        overwrite=overwrite_existed_files,
        path_pattern=path_pattern,
        limit=limit,
    )


@flow(
    name="repo-content-extraction",
    flow_run_name="Content extraction subflow",
    log_prints=True,
)
def extract_repo_content(
    input_dir: str,
    output_dir: str,
    path_pattern: str,
    start_date: str = "2020-01-01",
    end_date: str = None,
    languages: list = None,
    limit: int = None,
    overwrite_existed_files: bool = False,
    **kwargs,
):
    extractor = RepoContentExtractor(**kwargs)

    extractor.run(
        input_dir=input_dir,
        output_dir=output_dir,
        path_pattern=path_pattern,
        start_date=start_date,
        end_date=end_date,
        languages=languages,
        limit=limit,
        overwrite_existed_files=overwrite_existed_files,
    )


@flow(name="etl-repo-content-flow", flow_run_name="ETL: Repo content flow", log_prints=True)
def run_repo_content_flow(
    source_dir: str = os.getenv("DOCKER_PATH_RAW_DATA", "/volumes/data"),
    start_date: str = "2020-01-01",
    end_date: str = "2020-01-01",
    languages: str | list = "python",
    extract_structure: bool = True,
    extract_content: bool = True,
    overwrite_existed_files: bool = False,
    min_stars_count: int = 10,
    structure_path_pattern: str = consts.FULL_PATH_PATTERN,
    content_path_pattern: str = consts.DEFAULT_PATH_PATTERN,
    db_config: dict = None,
    **kwargs,
):
    languages = get_languages(languages)
    source_dir = os.path.join(source_dir, "repos")
    print(f"Source directory: {source_dir}")

    if extract_structure:
        extract_repo_structure(
            output_dir=os.path.join(source_dir, "structure"),
            start_date=start_date,
            end_date=end_date,
            languages=languages,
            path_pattern=structure_path_pattern,
            overwrite_existed_files=overwrite_existed_files,
            min_stars_count=min_stars_count,
            db_config=db_config,
            **kwargs,
        )

    if extract_content:
        extract_repo_content(
            input_dir=os.path.join(source_dir, "structure"),
            output_dir=os.path.join(source_dir, "content"),
            path_pattern=content_path_pattern,
            start_date=start_date,
            end_date=end_date,
            languages=languages,
            overwrite_existed_files=overwrite_existed_files,
            **kwargs,
        )
