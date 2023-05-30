"""
Github API scrapping
"""

import argparse
import os

import pandas as pd
from tqdm import tqdm
from prefect import flow

from src.utils import make_safe_request, get_headers, logger


class RepoContentExtractor:
    def __init__(
        self,
        api_token: str = None,
        retry_attempts: int = 3,
        timeout: int = 30,
        language: str = "python",
    ):
        self.headers = get_headers(api_token)
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        self.language = language

    def _extract(self, data: pd.DataFrame):
        records = []
        for _, row in tqdm(data.iterrows(), total=len(data)):
            content = make_safe_request(
                row.url,
                headers=self.headers,
                retry_attempts=self.retry_attempts,
                timeout=self.timeout,
            ).json()["content"]

            record = {
                "full_name": row.full_name,
                "url": row.url,
                "path": row.path,
                "content": content,
            }
            records.append(record)

        return pd.DataFrame(records)

    def extract_repo_content(
        self,
        input_dir,
        created_at: str,
        output_dir: str,
        path_pattern: str,
        limit=None,
        overwrite_existed_files: bool = False,
    ):
        output_path = os.path.join(
            output_dir, f"language={self.language}", f"file={path_pattern}", f"{created_at}.parquet"
        )
        if not overwrite_existed_files and os.path.exists(output_path):
            logger.info(f"File '{output_path}' already exists!")
            return output_dir

        input_path = os.path.join(input_dir, f"language={self.language}", f"{created_at}.parquet")
        if not os.path.exists(input_path):
            logger.warning(f"Input file '{input_path}' does not exist!")
            return output_dir

        data = pd.read_parquet(input_path)
        data = data[data["path"].str.lower().str.contains(path_pattern.lower())]
        if limit:
            data = data[:limit]
        logger.info(f"Input data shape: {data.shape}")

        output = self._extract(data)
        if not output.empty:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            output.to_parquet(output_path, index=False)
            logger.info("Data has been extracted and saved!")

    def run(
        self,
        input_dir: str,
        output_dir: str,
        path_patterns: str | list,
        start_date: str,
        end_date: str = None,
        **kwargs,
    ):
        if isinstance(path_patterns, str):
            path_patterns = [path_patterns]

        end_date = end_date or start_date
        if end_date < start_date:
            raise ValueError("'end_date' must be greater than 'start_date'")

        dates = pd.date_range(start=start_date, end=end_date, freq="D").strftime("%Y-%m-%d")

        for filename in path_patterns:
            logger.info(f"Filename to download: '{filename}'")

            for created_at in dates:
                logger.info(f"Created date: '{created_at}'")
                self.extract_repo_content(
                    input_dir, created_at, output_dir, path_pattern=filename, **kwargs
                )

        logger.info("All data has been extracted and saved!")


@flow(
    name="repo-content-extraction",
    flow_run_name="Repository Content Extraction Flow",
    log_prints=True,
)
def extract_repo_content(
    input_dir: str,
    output_dir: str,
    path_patterns: str | list,
    start_date: str = "2020-01-01",
    end_date: str = None,
    limit: int = None,
    overwrite_existed_files: bool = False,
    **kwargs,
):
    extractor = RepoContentExtractor(**kwargs)

    extractor.run(
        input_dir=input_dir,
        output_dir=output_dir,
        path_patterns=path_patterns,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        overwrite_existed_files=overwrite_existed_files,
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input_dir")
    parser.add_argument("--output_dir")
    parser.add_argument("--path_patterns")
    parser.add_argument("--start_date", default="2020-01-01")
    parser.add_argument("--end_date", default=None)
    parser.add_argument("--language", default="python")
    parser.add_argument("--api_token", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--overwrite_existed_files", action=argparse.BooleanOptionalAction)

    args = parser.parse_args()
    logger.info(f"Args: {args}")

    extract_repo_content(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        path_patterns=args.path_patterns.split(","),
        limit=args.limit,
        overwrite_existed_files=args.overwrite_existed_files,
        api_token=args.api_token,
    )


if __name__ == "__main__":
    main()
