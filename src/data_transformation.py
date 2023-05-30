import argparse
import os
from glob import glob
import shutil

import pandas as pd
from tqdm import tqdm
from prefect import flow

from src.utils import logger


def process_file(filename):
    columns = [
        "id",
        "name",
        "full_name",
        "owner",
        "html_url",
        "language",
        "description",
        "topics",
        "size",
        "stargazers_count",
        "forks_count",
        "open_issues_count",
        "license",
        "has_wiki",
        "default_branch",
        "created_at",
        "updated_at",
        "pushed_at",
    ]
    data = pd.read_parquet(filename, columns=columns)
    data["owner"] = data["owner"].apply(lambda x: x.get("login") if x else x)
    data["license"] = data["license"].apply(lambda x: x.get("name") if x else x).fillna("__NA__")
    data["has_description"] = ~data["description"].isnull()
    data["description_len"] = data["description"].str.len()
    data["created_at"] = pd.to_datetime(data["created_at"]).dt.round("d")
    data["created_at_year"] = data["created_at"].dt.year
    data["created_at_month"] = data["created_at"].dt.month
    data = data.rename(columns={"language": "language_display_name"})

    return data


def get_topics_table(data: pd.DataFrame):
    columns = [
        "id",
        "topics",
        "language",
        "stargazers_count",
        "forks_count",
        "created_at_year",
        "created_at_month",
    ]
    return data[columns].explode("topics")


def get_language_from_path(filename):
    path_dir = os.path.dirname(filename)
    return path_dir.split("=")[-1]


@flow(flow_run_name="Github Data Transformation Flow", log_prints=True)
def transform(
    input_dir: str,
    output_dir: str,
    languages: str | list,
    erase_output_dir_if_exists: bool = False,
    limit=None,
):
    if isinstance(languages, str):
        languages = [languages]

    if erase_output_dir_if_exists and os.path.exists(output_dir):
        logger.info(f"Clearing output directory '{output_dir}'")
        shutil.rmtree(output_dir)
        logger.info("Output directory is cleared!")

    for lang in languages:
        files = glob(f"{input_dir}/language={lang}/*.parquet")

        if limit:
            files = files[:limit]

        for f in tqdm(files, total=len(files)):
            df = process_file(f)
            df.loc[:, "language"] = get_language_from_path(f)

            name = os.path.basename(f)
            for entity in ["repos", "topics"]:
                output_path = os.path.join(output_dir, entity, f"language={lang}", name)
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                df.to_parquet(output_path, index=False)

    logger.info(f"All data saved to output directory: {output_dir}")
    return output_dir


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir")
    parser.add_argument("--output_dir")
    parser.add_argument("--languages")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--erase_output_dir_if_exists", action="store_true")

    args = parser.parse_args()
    logger.info(f"Args: {args}")

    transform(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        languages=args.languages.split(","),
        limit=args.limit,
        erase_output_dir_if_exists=args.erase_output_dir_if_exists,
    )


if __name__ == "__main__":
    main()
