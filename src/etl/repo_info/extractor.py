"""
Github API scrapping

https://docs.github.com/en/rest/overview/resources-in-the-rest-api?apiVersion=2022-11-28#rate-limiting
"""

import argparse
import os
import time
from glob import glob

import pandas as pd
from tqdm import tqdm

import src.etl.constants as consts
from src.utils.api import (
    SUPPORTED_LANGUAGES,
    get_date_range,
    get_headers,
    get_languages,
    make_safe_request,
)
from src.utils.db_handler import DBHandler
from src.utils.logger import logger


BASE_URL = "https://api.github.com/search/repositories"


class RepoInfoExtractor:
    """Extracting repositories info from Github"""

    def __init__(
        self,
        retry_attempts: int = consts.RETRY_ATTEMPTS,
        timeout: int = consts.TIMEOUT,
        pagination_timeout: int = consts.PAGINATION_TIMEOUT,
        max_requests_per_hour: int = consts.MAX_REQUESTS_PER_HOUR,
        api_token: str = None,
        db: DBHandler = None,
    ):
        self.headers = get_headers(api_token)
        self.pagination_timeout = pagination_timeout
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.max_pages = consts.SEARCH_ITEMS_LIMIT // consts.MAX_ITEMS_PER_PAGE
        self.max_requests_per_hour = max_requests_per_hour
        self.db = db
        if self.db is None:
            logger.warn("DB session is not specified. Files metadata will not be saved.")
        self._start = time.time()
        self._request_counter = 0

    def get_url(
        self,
        created_date: str,
        languages: list,
        page: int = 1,
        min_pushed_date: str = None,
        min_stars_count: int = consts.MIN_STARS_COUNT,
        sort_by: str = "stars",
        order_by: str = "desc",
    ) -> str:
        url = f"{BASE_URL}?q=created:{created_date}"

        if min_pushed_date:
            url = f"{url}+pushed:>={min_pushed_date}"

        if min_stars_count:
            url = f"{url}+stars:>={min_stars_count}"

        if isinstance(languages, str):
            languages = [languages]

        for lang in languages:
            url += f"+language:{lang}"

        url += f"&sort={sort_by}&order={order_by}"
        url += f"&per_page={consts.MAX_ITEMS_PER_PAGE}"
        url += f"&page={page}"

        return url

    def get_filename(self, output_dir: str, language: str, created_at: str) -> str:
        path_dir = os.path.join(output_dir, f"language={language}")
        os.makedirs(path_dir, exist_ok=True)
        return os.path.join(path_dir, f"{created_at}.parquet")

    def _save_file_metadata(self, values: tuple, table: str = "repo_file_metadata"):
        if self.db is None:
            return

        q = f"""
        INSERT INTO {self.db.schema}.{table} (repo_language, repo_creation_date, n_repos, file_path, elapsed_time_seconds)
        VALUES {values}
        ON CONFLICT (repo_language, repo_creation_date) DO UPDATE SET
            file_path = EXCLUDED.file_path,
            elapsed_time_seconds = EXCLUDED.elapsed_time_seconds,
            updated_at = CURRENT_TIMESTAMP
        """

        self.db.execute(q)

    @property
    def _activity_period(self):
        return time.time() - self._start

    def _update_request_counter(self):
        if self._activity_period < consts.RATE_LIMITER_WINDOW:
            self._request_counter += 1
        else:
            self._start = time.time()
            self._request_counter = 0

    def fetch_repos(self, language: str, created_at: str, **kwargs) -> pd.DataFrame:
        df = pd.DataFrame()
        for page in tqdm(range(1, self.max_pages + 1)):
            url = self.get_url(created_at, languages=[language], page=page, **kwargs)
            logger.info(f"Getting data from '{url}'")

            if self._request_counter >= self.max_requests_per_hour:
                timeout = consts.RATE_LIMITER_WINDOW - self._activity_period
                logger.warning(f"Request limit exceeded. Timeout: {int(timeout)}")
                while timeout > 0:
                    logger.warning(f"\tSleeping... {int(timeout)} seconds left.")
                    window = min(60, timeout)
                    time.sleep(window)
                    timeout -= window

            response = make_safe_request(
                url, headers=self.headers, timeout=self.timeout, retry_attempts=self.retry_attempts
            )
            response.raise_for_status()

            total_count = response.json()["total_count"]
            if total_count > consts.SEARCH_ITEMS_LIMIT:
                logger.warning(
                    f"The number of items in the search result ({total_count}) exceeds the limit"
                    f" ({consts.SEARCH_ITEMS_LIMIT})"
                )

            items = response.json()["items"]
            df = pd.concat((df, pd.DataFrame(items)))

            time.sleep(self.pagination_timeout)
            self._update_request_counter()

            if page >= self.max_pages or total_count < consts.MAX_ITEMS_PER_PAGE or not items:
                break

        return df

    def extract(
        self, created_at: str, languages: list, output_dir: str, overwrite: bool = False, **kwargs
    ):
        if isinstance(languages, str):
            languages = [languages]

        for language in languages:
            logger.info(f"🖲 Language: {language}")
            if language not in SUPPORTED_LANGUAGES:
                raise ValueError("Language is not supported!")

            filename = self.get_filename(
                output_dir=output_dir, language=language, created_at=created_at
            )
            if not overwrite and os.path.exists(filename):
                logger.info(f"File '{filename}' already exists!")
                continue

            start = time.time()
            df = self.fetch_repos(created_at=created_at, language=language, **kwargs)

            n_repos = df.shape[0]
            if n_repos > 0:
                df.to_parquet(filename, index=False)
                logger.info(f"✅ Data has been saved to '{filename}'!")

                elapsed_time = int(time.time() - start)
                path = os.path.abspath(filename)
                values = (language, created_at, n_repos, path, elapsed_time)
                self._save_file_metadata(values)
                logger.info("✅ File metadata has been inserted into the DB!")

            else:
                logger.info("There is no data to save!")

            logger.info(f"Total number of repositories: {n_repos}")

        logger.info(f"✅ Processing completed for 'created_at={created_at}'!")

    def run(
        self,
        output_dir: str,
        languages: str | list,
        start_date: str,
        end_date: str = None,
        **kwargs,
    ):
        dates = get_date_range(start_date, end_date)

        for created_at in dates:
            try:
                logger.info(f"🕐 Created at: {created_at}")
                self.extract(created_at, languages=languages, output_dir=output_dir, **kwargs)
                logger.info(
                    f"{self._request_counter} requests made during the last"
                    f" {int(self._activity_period)} seconds."
                )
            except Exception as e:
                logger.exception(e)
                raise

        total_count = len(glob(os.path.join(output_dir, "*", "*.parquet")))
        logger.info(f"Total number of files in the output directory: {total_count}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", default="2020-01-01")
    parser.add_argument("--end_date", default="2020-01-01")
    parser.add_argument("--output_dir")
    parser.add_argument("--languages")
    parser.add_argument("--min_pushed_date", default="2020-01-01")
    parser.add_argument("--min_stars_count", type=int, default=consts.MIN_STARS_COUNT)
    parser.add_argument("--api_token")
    parser.add_argument("--pagination_timeout", type=int, default=consts.PAGINATION_TIMEOUT)
    parser.add_argument("--timeout", type=int, default=consts.TIMEOUT)
    parser.add_argument("--max_requests_per_hour", type=int, default=consts.MAX_REQUESTS_PER_HOUR)
    parser.add_argument("--retry_attempts", type=int, default=consts.RETRY_ATTEMPTS)
    parser.add_argument("--overwrite_existed_files", action="store_true")

    args = parser.parse_args()
    logger.info(f"Args: {args}")

    extractor = RepoInfoExtractor(
        timeout=args.timeout,
        pagination_timeout=args.pagination_timeout,
        max_requests_per_hour=args.max_requests_per_hour,
        api_token=args.api_token,
    )
    extractor.run(
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        languages=get_languages(args.languages),
        min_pushed_date=args.min_pushed_date,
        min_stars_count=args.min_stars_count,
        overwrite=args.overwrite_existed_files,
    )


if __name__ == "__main__":
    main()
