"""
Github API scrapping

- https://docs.github.com/en/rest/overview/resources-in-the-rest-api?apiVersion=2022-11-28#rate-limiting
"""

import argparse
import os
import time
from glob import glob

import pandas as pd
from tqdm import tqdm

from src.utils.api import make_safe_request, get_headers, get_languages, SUPPORTED_LANGUAGES
from src.utils.db_handler import DBHandler
from src.utils.logger import logger


BASE_URL = "https://api.github.com/search/repositories"
MAX_ITEMS_PER_PAGE = 100
SEARCH_ITEMS_LIMIT = 1000
MAX_REQUESTS_PER_HOUR = 5000
RATE_LIMITER_WINDOW = 3600
TIMEOUT = 60
PAGINATION_TIMEOUT = 2
RETRY_ATTEMPTS = 3
MIN_STARS_COUNT = 1


class RepoMetadataExtractor:
    """Extracting repositories metadata from Github"""

    def __init__(
        self,
        output_dir: str = "./tmp/data/repos",
        min_pushed_date: str = None,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = TIMEOUT,
        pagination_timeout: int = PAGINATION_TIMEOUT,
        min_stars_count: int = MIN_STARS_COUNT,
        max_requests_per_hour: int = MAX_REQUESTS_PER_HOUR,
        sort_by: str = "stars",
        order_by: str = "desc",
        api_token: str = None,
        db: DBHandler = None,
    ):
        self.headers = get_headers(api_token)
        self.output_dir = output_dir
        self.pagination_timeout = pagination_timeout
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.max_pages = SEARCH_ITEMS_LIMIT // MAX_ITEMS_PER_PAGE
        self.max_requests_per_hour = max_requests_per_hour
        self.min_pushed_date = min_pushed_date
        self.min_stars_count = min_stars_count
        self.sort_by = sort_by
        self.order_by = order_by
        self.db = db
        if self.db is None:
            logger.warn("DB session is not specified. Files metadata will not be saved.")
        self._start = time.time()
        self._request_counter = 0

    def get_url(self, created_date: str, languages: list, page: int = 1) -> str:
        url = f"{BASE_URL}?q=created:{created_date}"

        if self.min_pushed_date:
            url = f"{url}+pushed:>={self.min_pushed_date}"

        if self.min_stars_count:
            url = f"{url}+stars:>={self.min_stars_count}"

        if isinstance(languages, str):
            languages = [languages]

        for lang in languages:
            url += f"+language:{lang}"

        url += f"&sort={self.sort_by}&order={self.order_by}"
        url += f"&per_page={MAX_ITEMS_PER_PAGE}"
        url += f"&page={page}"

        return url

    def get_filename(self, language: str, created_at: str) -> str:
        path_dir = os.path.join(self.output_dir, f"language={language}")
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
        if self._activity_period < RATE_LIMITER_WINDOW:
            self._request_counter += 1
        else:
            self._start = time.time()
            self._request_counter = 0

    def fetch_repos(self, language: str, created_at: str) -> pd.DataFrame:
        df = pd.DataFrame()
        for page in tqdm(range(1, self.max_pages + 1)):
            url = self.get_url(created_at, languages=[language], page=page)
            logger.info(f"Getting data from '{url}'")

            if self._request_counter >= self.max_requests_per_hour:
                timeout = RATE_LIMITER_WINDOW - self._activity_period
                logger.warning(f"Request limit exceeded. Timeout: {int(timeout)}")
                while timeout > 0:
                    logger.warning(f"\tSleeping... {int(timeout)} seconds left.")
                    window = min(60, timeout)
                    time.sleep(window)
                    timeout -= window

            response = make_safe_request(
                url, headers=self.headers, timeout=self.timeout, retry_attempts=self.retry_attempts
            )

            total_count = response.json()["total_count"]
            if total_count > SEARCH_ITEMS_LIMIT:
                logger.warning(
                    f"The number of items in the search result ({total_count}) exceeds the limit"
                    f" ({SEARCH_ITEMS_LIMIT})"
                )

            items = response.json()["items"]
            df = pd.concat((df, pd.DataFrame(items)))

            time.sleep(self.pagination_timeout)
            self._update_request_counter()

            if page >= self.max_pages or total_count < MAX_ITEMS_PER_PAGE or not items:
                break

        return df

    def extract_repos(self, created_at: str, languages: list, overwrite: bool = False, **kwargs):
        for language in languages:
            logger.info(f"🕐 Language: {language}")
            if language not in SUPPORTED_LANGUAGES:
                raise ValueError("Language is not supported!")

            filename = self.get_filename(language=language, created_at=created_at)
            if not overwrite and os.path.exists(filename):
                logger.info(f"File '{filename}' already exists!")
                return

            start = time.time()
            df = self.fetch_repos(created_at=created_at, language=language, **kwargs)

            n_repos = df.shape[0]
            if n_repos > 0:
                df.to_parquet(filename, index=False)
                logger.info(f"✅ Data has been saved to '{filename}'!")

                elapsed_time = int(time.time() - start)
                values = (language, created_at, n_repos, filename, elapsed_time)
                self._save_file_metadata(values)
                logger.info("✅ File metadata has been inserted into the DB!")

            else:
                logger.info("There is no data to save!")

            logger.info(f"Total number of repositories: {n_repos}")

        logger.info(f"✅ Processing completed for 'created_at={created_at}'!")

    def run(self, languages: str | list, start_date: str, end_date: str = None, **kwargs):
        end_date = end_date or start_date

        if end_date < start_date:
            raise ValueError("'end_date' must be greater than 'start_date'")

        dates = pd.date_range(start=start_date, end=end_date, freq="D").strftime("%Y-%m-%d")

        for created_at in dates:
            try:
                logger.info(f"🔵 Created at: {created_at}")
                self.extract_repos(created_at, languages=languages, **kwargs)
                logger.info(
                    f"{self._request_counter} requests made during the last"
                    f" {int(self._activity_period)} seconds."
                )
            except Exception as e:
                logger.exception(e)
                raise

        total_count = len(glob(os.path.join(self.output_dir, "*", "*.parquet")))
        logger.info(f"Total number of files in the output directory: {total_count}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", default="2020-01-01")
    parser.add_argument("--end_date", default="2020-01-01")
    parser.add_argument("--output_dir")
    parser.add_argument("--languages")
    parser.add_argument("--min_pushed_date", default="2020-01-01")
    parser.add_argument("--min_stars_count", type=int, default=MIN_STARS_COUNT)
    parser.add_argument("--api_token")
    parser.add_argument("--pagination_timeout", type=int, default=PAGINATION_TIMEOUT)
    parser.add_argument("--max_requests_per_hour", type=int, default=MAX_REQUESTS_PER_HOUR)
    parser.add_argument("--retry_attempts", type=int, default=RETRY_ATTEMPTS)
    parser.add_argument("--overwrite_existed_files", action="store_true")

    args = parser.parse_args()
    logger.info(f"Args: {args}")

    extractor = RepoMetadataExtractor(
        output_dir=args.output_dir,
        min_pushed_date=args.min_pushed_date,
        pagination_timeout=args.pagination_timeout,
        max_requests_per_hour=args.max_requests_per_hour,
        min_stars_count=args.min_stars_count,
        api_token=args.api_token,
    )
    extractor.run(
        start_date=args.start_date,
        end_date=args.end_date,
        languages=get_languages(args.languages),
        overwrite=args.overwrite_existed_files,
    )


if __name__ == "__main__":
    main()
