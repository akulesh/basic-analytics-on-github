"""
Github API scrapping
"""

import argparse
import os
import time
from glob import glob

import pandas as pd
from tqdm import tqdm

from src.utils.api import make_safe_request, get_headers, get_languages, SUPPORTED_LANGUAGES
from src.utils.logger import logger


BASE_URL = "https://api.github.com/search/repositories"


class RepoMetadataExtractor:
    """Extracting repositories metadata from Github"""

    def __init__(
        self,
        output_dir: str = "./tmp/data/repos",
        min_pushed_date: str = None,
        max_items_per_page: int = 100,
        retry_attempts: int = 3,
        timeout: int = 30,
        pagination_timeout: int = 5,
        search_items_limit: int = 1000,
        sort_by: str = "stars",
        order_by: str = "desc",
        api_token: str = None,
    ):
        self.headers = get_headers(api_token)
        self.output_dir = output_dir
        self.pagination_timeout = pagination_timeout
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.max_pages = search_items_limit // max_items_per_page
        self.max_items_per_page = max_items_per_page
        self.min_pushed_date = min_pushed_date
        self.sort_by = sort_by
        self.order_by = order_by

    def get_url(
        self,
        created_date: str,
        languages: str | list,
        min_stars_count: int = 1,
        page: int = 1,
    ) -> str:
        url = f"{BASE_URL}?q=created:{created_date}"

        if self.min_pushed_date:
            url = f"{url}+pushed:>={self.min_pushed_date}"

        if min_stars_count:
            url = f"{url}+stars:>={min_stars_count}"

        if isinstance(languages, list):
            for lang in languages:
                url = f"{url}+language:{lang}"

        if self.sort_by:
            url = f"{url}&sort={self.sort_by}&order={self.order_by}"

        if isinstance(self.max_items_per_page, int):
            url = f"{url}&per_page={self.max_items_per_page}"

        if isinstance(page, int):
            url = f"{url}&page={page}"

        return url

    def get_filename(self, language: str, created_at: str):
        path_dir = os.path.join(self.output_dir, f"language={language}")
        os.makedirs(path_dir, exist_ok=True)
        return os.path.join(path_dir, f"{created_at}.parquet")

    def extract_repos_by_language(
        self, language: str, created_at: str, min_stars_count: int = 1, overwrite: bool = False
    ):
        df = pd.DataFrame()

        filename = self.get_filename(language=language, created_at=created_at)
        if not overwrite and os.path.exists(filename):
            logger.info(f"File '{filename}' already exists!")
            return df

        for page in tqdm(range(1, self.max_pages + 1)):
            url = self.get_url(
                created_at, languages=[language], min_stars_count=min_stars_count, page=page
            )
            logger.info(f"Getting data from '{url}'")
            response = make_safe_request(
                url, headers=self.headers, timeout=self.timeout, retry_attempts=self.retry_attempts
            )
            items = response.json()["items"]
            df = pd.concat((df, pd.DataFrame(items)))
            time.sleep(self.pagination_timeout)

            if page >= self.max_pages or not items:
                break

        if not df.empty:
            df.to_parquet(filename, index=False)
            logger.info(f"✅ Data has been saved to '{filename}'!")
        else:
            logger.info("There is no data to save!")

        return df

    def extract_repos(self, created_at: str, languages: list, **kwargs):
        try:
            url = self.get_url(created_at, languages=languages, page=1)
            response = make_safe_request(
                url, headers=self.headers, timeout=self.timeout, retry_attempts=self.retry_attempts
            )
            total_count = response.json()["total_count"]
            logger.info(f"🔵 Created at: {created_at} | Total repositories: {total_count}")

            if total_count == 0:
                logger.info("There is no data to save!")
                return

            for language in languages:
                logger.info(f"🕐 Language: {language}")
                if language not in SUPPORTED_LANGUAGES:
                    raise ValueError("Language is not supported!")

                self.extract_repos_by_language(created_at=created_at, language=language, **kwargs)

            logger.info(f"✅ Processing completed for 'created_at={created_at}'!")

        except Exception as e:
            logger.exception(e)
            raise

    def run(self, languages: str | list, start_date: str, end_date: str = None, **kwargs):
        end_date = end_date or start_date

        if end_date < start_date:
            raise ValueError("'end_date' must be greater than 'start_date'")

        dates = pd.date_range(start=start_date, end=end_date, freq="D").strftime("%Y-%m-%d")

        for created_at in dates:
            self.extract_repos(created_at, languages=languages, **kwargs)
            total_count = len(glob(os.path.join(self.output_dir, "*", "*.parquet")))
            logger.info(f"Total number of files in the output directory: {total_count}")

            time.sleep(self.pagination_timeout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", default="2020-01-01")
    parser.add_argument("--end_date", default="2020-01-01")
    parser.add_argument("--output_dir")
    parser.add_argument("--languages", default=None)
    parser.add_argument("--min_pushed_date", default="2020-01-01")
    parser.add_argument("--min_stars_count", type=int, default=1)
    parser.add_argument("--api_token")
    parser.add_argument("--pagination_timeout", type=int, default=5)
    parser.add_argument("--overwrite_existed_files", action="store_true")

    args = parser.parse_args()
    logger.info(f"Args: {args}")

    extractor = RepoMetadataExtractor(
        output_dir=args.output_dir,
        min_pushed_date=args.min_pushed_date,
        pagination_timeout=args.pagination_timeout,
        api_token=args.api_token,
    )
    extractor.run(
        start_date=args.start_date,
        end_date=args.end_date,
        languages=get_languages(args.languages),
        min_stars_count=args.min_stars_count,
        overwrite=args.overwrite_existed_files,
    )


if __name__ == "__main__":
    main()
