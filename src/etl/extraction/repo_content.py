"""
Github API scrapping
"""

import argparse
import os
import time

import pandas as pd
from tqdm import tqdm

import src.etl.extraction.constants as consts
from src.utils.api import get_date_range, get_headers, make_safe_request
from src.utils.logger import logger


class RepoContentExtractor:
    """Extraction of repository content"""

    def __init__(
        self,
        retry_attempts: int = consts.RETRY_ATTEMPTS,
        pagination_timeout: int = consts.PAGINATION_TIMEOUT,
        timeout: int = consts.TIMEOUT,
        api_token: str = None,
    ):
        self.headers = get_headers(api_token)
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        self.pagination_timeout = pagination_timeout

    # TODO: add authorization error handling
    def fetch_content(self, url: str, repo_id: str = None, path: str = None) -> dict:
        content = None
        try:
            response = make_safe_request(
                url,
                headers=self.headers,
                retry_attempts=self.retry_attempts,
                timeout=self.timeout,
            )
            content = response.json()["content"]
        except Exception:
            logger.exception(f"Fetching content failed! URL: {url}")

        return {"repo_id": repo_id, "path": path, "url": url, "content": content}

    def extract(
        self,
        url_df: pd.DataFrame,
        filename: str,
        path_pattern: str,
        limit: int = None,
        overwrite_existed_files: bool = False,
    ):
        if not overwrite_existed_files and os.path.exists(filename):
            logger.info(f"File '{filename}' already exists!")
            return filename

        mask = url_df["path"].str.lower().str.contains(path_pattern.lower())
        input_data = url_df[mask.fillna(False)]

        if limit:
            input_data = input_data[:limit]

        n_rows, n_cols = input_data.shape
        logger.info(f"Input data shape: {n_rows, n_cols}")

        output = []
        for _, row in tqdm(input_data.iterrows(), total=n_rows):
            try:
                record = self.fetch_content(
                    url=row["url"], repo_id=row["repo_id"], path=row["path"]
                )
                output.append(record)
            except Exception:
                logger.warning("Failed to fetch content!")

            time.sleep(self.pagination_timeout)

        data = pd.DataFrame(output)
        if data.empty:
            data = pd.DataFrame(url_df["repo_id"].drop_duplicates())
            logger.info("The extracted data is empty!")

        os.makedirs(os.path.dirname(filename), exist_ok=True)
        data.to_parquet(filename, index=False)
        logger.info(f"Data has been saved! Filename: {filename}")

        return data

    def run(
        self,
        input_dir: str,
        output_dir: str,
        path_pattern: str | list,
        start_date: str,
        end_date: str = None,
        languages: list = None,
        # max_items_per_pattern: str = None,
        **kwargs,
    ):
        if isinstance(path_pattern, str):
            path_pattern = path_pattern.split("|")

        languages = languages or ["python"]

        dates = get_date_range(start_date, end_date)
        for creation_date in dates:
            logger.info(f"🕐 Created date: '{creation_date}'")

            for language in languages:
                logger.info(f"🖲 Language: {language}")
                filename = os.path.join(
                    input_dir, f"language={language}", f"{creation_date}.parquet"
                )
                try:
                    url_df = pd.read_parquet(filename)
                except FileNotFoundError:
                    logger.warning(f"File {filename} is not found!")
                    continue

                # FIXME
                if "path" not in url_df.columns:
                    continue

                for key in path_pattern:
                    logger.info(f"Pattern in file path: '{key}'")

                    # TODO: limit files number
                    # if max_items_per_pattern:
                    #     input_data = input_data[:max_items_per_pattern]

                    filename = os.path.join(
                        output_dir,
                        f"language={language}",
                        f"key={key}",
                        f"{creation_date}.parquet",
                    )
                    self.extract(url_df, filename=filename, path_pattern=key, **kwargs)

        logger.info("All data has been extracted and saved!")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input_dir", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--path_pattern", required=True)
    parser.add_argument("--start_date", default="2020-01-01")
    parser.add_argument("--end_date", default=None)
    parser.add_argument("--languages", default=["python"])
    parser.add_argument("--timeout", type=int, default=consts.TIMEOUT)
    parser.add_argument("--pagination_timeout", type=int, default=consts.PAGINATION_TIMEOUT)
    parser.add_argument("--retry_attempts", type=int, default=consts.RETRY_ATTEMPTS)
    parser.add_argument("--api_token", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--overwrite_existed_files", action=argparse.BooleanOptionalAction)

    args = parser.parse_args()
    logger.info(f"Args: {args}")

    extractor = RepoContentExtractor(
        pagination_timeout=args.pagination_timeout,
        timeout=args.timeout,
        retry_attempts=args.retry_attempts,
        api_token=args.api_token,
    )

    extractor.run(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        path_pattern=args.path_pattern.split("|"),
        limit=args.limit,
        overwrite=args.overwrite_existed_files,
        languages=args.languages,
    )


if __name__ == "__main__":
    main()
