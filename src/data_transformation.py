import argparse
from datetime import datetime
import os
from glob import glob
import shutil

import pandas as pd
from tqdm import tqdm
from prefect import flow

from src.utils import logger, get_languages


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
        "watchers_count",
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
    data["repo_owner"] = data["owner"].apply(lambda x: x.get("login") if x else x)
    data["owner_url"] = data["owner"].apply(lambda x: x.get("html_url") if x else x)
    data["license"] = data["license"].apply(lambda x: x.get("name") if x else x).fillna("__NA__")
    data["has_license"] = data["license"] != "__NA__"
    data["has_description"] = ~data["description"].isnull()
    data["description"] = data["description"].astype("str")
    data["description_len"] = data["description"].str.len().astype("float")
    data["created_at"] = pd.to_datetime(data["created_at"]).dt.round("d").dt.tz_localize(None)
    data["pushed_at"] = pd.to_datetime(data["pushed_at"]).dt.round("d").dt.tz_localize(None)
    data["created_at_year"] = data["created_at"].dt.year
    data["created_at_month"] = data["created_at"].dt.month
    data["created_at_year_month"] = data.apply(
        lambda x: f"{x['created_at_year']}-{x['created_at_month']:02}", axis=1
    )
    data["days_since_date_created"] = (datetime.now() - data["created_at"]).dt.days
    data["days_between_creation_and_latest_push"] = (data["pushed_at"] - data["created_at"]).dt.days
    data = data.rename(columns={"language": "language_display_name"})

    return data


def get_topics_table(data: pd.DataFrame):
    columns = [
        "id",
        "topics",
        "language",
        "language_display_name",
        "stargazers_count",
        "forks_count",
        "created_at_year",
        "created_at_month",
    ]
    data = data[columns].explode("topics")
    data["topics"] = data["topics"].astype(str)
    data["topics"] = data["topics"].str.lower()
    data["topics"] = data["topics"].replace({"nan": "__NA__"})

    return data


def get_language_from_path(filename):
    path_dir = os.path.dirname(filename)
    return path_dir.split("=")[-1]


@flow(flow_run_name="Github Data Transformation Flow", log_prints=True)
def transform(
    input_dir: str,
    output_dir: str,
    languages: list,
    erase_output_dir_if_exists: bool = False,
    limit=None,
):
    if erase_output_dir_if_exists and os.path.exists(output_dir):
        logger.info(f"Clearing output directory '{output_dir}'")
        shutil.rmtree(output_dir)
        logger.info("Output directory is cleared!")

    for lang in languages:
        logger.info(f"🕐 Language: {lang}")
        files = glob(f"{input_dir}/language={lang}/*.parquet")

        if limit:
            files = files[:limit]

        for f in tqdm(files, total=len(files)):
            repos_df = process_file(f)
            repos_df.loc[:, "language"] = get_language_from_path(f)

            name = os.path.basename(f)
            output_path = os.path.join(output_dir, "topics", f"language={lang}", name)
            topics_df = get_topics_table(repos_df)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            topics_df.to_parquet(output_path, index=False)

            output_path = os.path.join(output_dir, "repos", f"language={lang}", name)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            del repos_df["topics"]
            repos_df.to_parquet(output_path, index=False)

    logger.info(f"All data saved to output directory: {output_dir}")
    return output_dir


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir")
    parser.add_argument("--output_dir")
    parser.add_argument("--languages", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--erase_output_dir_if_exists", action="store_true")

    args = parser.parse_args()
    logger.info(f"Args: {args}")

    transform(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        languages=get_languages(args.languages),
        limit=args.limit,
        erase_output_dir_if_exists=args.erase_output_dir_if_exists,
    )


if __name__ == "__main__":
    main()
