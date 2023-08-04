import os
import time

from prefect import flow, task

import src.etl.constants as consts
from src.etl.repo_content import (
    PythonPackagesExtractor,
    RepoContentExtractor,
    RepoStructureExtractor,
)
from src.etl.repo_info import RepoInfoExtractor, RepoInfoTransformer
from src.utils.api import get_current_date, get_languages
from src.utils.db_handler import get_db_handler


@task(
    name="repo-info-extraction",
    task_run_name="Repo info extraction task",
    log_prints=True,
)
def extract_repo_info(
    output_dir: str,
    start_date: str,
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


@task(
    name="repo-info-transformation",
    task_run_name="Repo info transformation task",
    log_prints=True,
)
def transform_load_repo_info(
    input_dir: str,
    start_date: str,
    end_date: str = None,
    languages: list = None,
    db_config: dict = None,
):
    db = get_db_handler(db_config)
    transformer = RepoInfoTransformer()
    transformer.run(
        db=db, input_dir=input_dir, start_date=start_date, end_date=end_date, languages=languages
    )

    time.sleep(1)
    stats = db.get_table_stats()
    print(f"Tables info:\n{stats}")


@flow(name="etl-repo-info-flow", flow_run_name="ETL: Repo info flow", log_prints=True)
def run_repo_info_flow(
    source_dir: str = os.getenv("DOCKER_PATH_RAW_DATA", "/volumes/data"),
    start_date: str = None,
    end_date: str = None,
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

    start_date = start_date or get_current_date()
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
        transform_load_repo_info(
            input_dir=os.path.join(source_dir, "info"),
            start_date=start_date,
            end_date=end_date,
            languages=languages,
            db_config=db_config,
        )


@task(
    name="repo-structure-extraction",
    task_run_name="Structure extraction task",
    log_prints=True,
)
def extract_repo_structure(
    output_dir: str,
    start_date: str,
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
    extractor = RepoStructureExtractor(**kwargs)

    extractor.run(
        db=db,
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date,
        languages=languages,
        min_stars_count=min_stars_count,
        overwrite=overwrite_existed_files,
        path_pattern=path_pattern,
        limit=limit,
    )


@task(
    name="repo-content-extraction",
    task_run_name="Content extraction task",
    log_prints=True,
)
def extract_repo_content(
    input_dir: str,
    output_dir: str,
    path_pattern: str,
    start_date: str = None,
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


@task(
    name="python-packages-extraction",
    task_run_name="Python packages extraction task",
    log_prints=True,
)
def extract_py_packages(
    input_dir: str,
    start_date: str,
    end_date: str = None,
    languages: list = None,
    db_config: dict = None,
):
    extractor = PythonPackagesExtractor()
    db = get_db_handler(db_config)

    extractor.run(
        db=db, input_dir=input_dir, start_date=start_date, end_date=end_date, languages=languages
    )


@flow(name="etl-repo-content-flow", flow_run_name="ETL: Repo content flow", log_prints=True)
def run_repo_content_flow(
    source_dir: str = os.getenv("DOCKER_PATH_RAW_DATA", "/volumes/data"),
    start_date: str = None,
    end_date: str = None,
    languages: str | list = "python",
    run_extract_structure_task: bool = True,
    run_extract_content_task: bool = True,
    run_extract_py_packages_task: bool = True,
    overwrite_existed_files: bool = False,
    min_stars_count: int = 10,
    structure_path_pattern: str = consts.FULL_PATH_PATTERN,
    content_path_pattern: str = consts.DEFAULT_PATH_PATTERN,
    db_config: dict = None,
    **kwargs,
):
    source_dir = os.path.join(source_dir, "repos")
    print(f"Source directory: {source_dir}")

    start_date = start_date or get_current_date()
    if run_extract_structure_task:
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

    if run_extract_content_task:
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

    if run_extract_py_packages_task:
        extract_py_packages(
            input_dir=os.path.join(source_dir, "content"),
            start_date=start_date,
            end_date=end_date,
            languages=languages,
            db_config=db_config,
        )


@flow(name="etl-flow", flow_run_name="ETL flow", log_prints=True)
def run_etl_flow(
    source_dir: str = os.getenv("DOCKER_PATH_RAW_DATA", "/volumes/data"),
    start_date: str = None,
    end_date: str = None,
    n_last_days: int = 0,
    run_info_flow: bool = True,
    run_contents_flow: bool = True,
    overwrite_existed_files: bool = False,
    languages: str | list = None,
    info_flow_config: dict = None,
    content_flow_config: dict = None,
    **kwargs,
):
    start_date = start_date or get_current_date(n_last_days)
    end_date = end_date or get_current_date()
    languages = get_languages(languages)

    if run_info_flow:
        info_flow_config = info_flow_config or {}
        run_repo_info_flow(
            source_dir,
            start_date=start_date,
            end_date=end_date,
            languages=languages,
            overwrite_existed_files=overwrite_existed_files,
            **info_flow_config,
            **kwargs,
        )

    if run_contents_flow:
        content_flow_config = content_flow_config or {}
        run_repo_content_flow(
            source_dir,
            start_date=start_date,
            end_date=end_date,
            languages=languages,
            overwrite_existed_files=overwrite_existed_files,
            **content_flow_config,
            **kwargs,
        )
