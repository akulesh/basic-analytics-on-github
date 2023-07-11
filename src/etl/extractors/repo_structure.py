"""
Github API scrapping
"""

import argparse
import os
import time

import pandas as pd
from tqdm import tqdm

import src.etl.extractors.constants as consts
from src.utils.api import get_headers, make_safe_request
from src.utils.db_handler import DBHandler
from src.utils.logger import logger


SUPPORTED_LANGUAGES = ["python", "jupyter-notebook"]
DEFAULT_PATH_PATTERN = (
    r"^readme.md$|^pyproject.toml$|requirements.*\.txt|pipfile|poetry|dockerfile|docker-compose"
)


class RepoStructureExtractor:
    """Extraction of repository structure"""

    def __init__(
        self,
        output_dir: str,
        api_token: str = None,
        min_stars_count: int = consts.MIN_STARS_COUNT,
        retry_attempts: int = consts.RETRY_ATTEMPTS,
        pagination_timeout: int = consts.PAGINATION_TIMEOUT,
        timeout: int = consts.TIMEOUT,
        db: DBHandler = None,
    ):
        self.headers = get_headers(api_token)
        self.retry_attempts = retry_attempts
        self.pagination_timeout = pagination_timeout
        self.timeout = timeout
        self.output_dir = os.path.join(output_dir, "structure")
        self.min_stars_count = min_stars_count
        self.db = db

    def fetch_repo_structure(self, owner: str, repo: str, branch: str, path_pattern: str = None):
        url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
        response = make_safe_request(
            url,
            headers=self.headers,
            retry_attempts=self.retry_attempts,
            timeout=self.timeout,
        ).json()

        data = pd.DataFrame(response["tree"])
        data = data[data["type"] == "blob"]
        data["path"] = data["path"].str.lower()

        if path_pattern:
            data = data[data["path"].str.contains(path_pattern)]

        return data

    @staticmethod
    def select_repo_info(
        db, language: str, creation_date: str, min_stars: int = 1, limit: int = None
    ):
        q = f"""
        SELECT id, owner, name AS repo, default_branch AS branch
        FROM github.repo
        WHERE lang_alias = '{language}'
            AND creation_date = '{creation_date}'
            AND stars >= {min_stars}
        """

        if limit:
            q += f"LIMIT {limit}"

        data = db.read_sql(q)
        data = data.drop_duplicates()

        return data

    def extract(
        self,
        repo_df: pd.DataFrame,
        language: str,
        creation_date: str = None,
        path_pattern: str = DEFAULT_PATH_PATTERN,
        overwrite: bool = False,
    ):
        filename = os.path.join(self.output_dir, f"language={language}", f"{creation_date}.parquet")
        if not overwrite and os.path.exists(filename):
            logger.info(f"File '{filename}' already exists!")
            return self.output_dir

        n_rows, n_cols = repo_df.shape
        logger.info(f"Repo data shape: {n_rows, n_cols}")

        output = []
        for _, row in tqdm(repo_df.iterrows(), total=n_rows):
            try:
                df = self.fetch_repo_structure(
                    owner=row["owner"],
                    repo=row["repo"],
                    branch=row["branch"],
                    path_pattern=path_pattern,
                )
                is_failed = False
            except Exception:
                is_failed = True
                df = pd.DataFrame()

            if not df.empty:
                df.loc[:, "repo_id"] = row["id"]
                columns = ["repo_id", "path", "url", "size"]
                df = df[columns]
            else:
                df = pd.DataFrame([{"repo_id": row["id"], "is_failed": is_failed}])

            output.append(df)

            time.sleep(self.pagination_timeout)

        output = pd.concat(output)
        if not output.empty:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            output.to_parquet(filename, index=False)
            logger.info("Data has been extracted and saved!")
        else:
            logger.info("The output is empty!")

        return self.output_dir

    def run(
        self,
        start_date: str,
        end_date: str = None,
        languages: list = None,
        limit: int = None,
        **kwargs,
    ):
        languages = languages or SUPPORTED_LANGUAGES
        languages = [lang for lang in languages if lang in SUPPORTED_LANGUAGES]

        end_date = end_date or start_date
        if end_date < start_date:
            raise ValueError("'end_date' must be greater than 'start_date'")

        dates = pd.date_range(start=start_date, end=end_date, freq="D").strftime("%Y-%m-%d")

        for creation_date in dates:
            logger.info(f"Created date: '{creation_date}'")

            for language in languages:
                logger.info(f"🕐 Language: {language}")
                repo_df = self.select_repo_info(
                    self.db, language, creation_date, min_stars=self.min_stars_count, limit=limit
                )
                if not repo_df.empty:
                    self.extract(repo_df, language, creation_date=creation_date, **kwargs)

            logger.info(f"✅ Processing completed for 'creation_date={creation_date}'!")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", default="2020-01-01")
    parser.add_argument("--end_date", default="2020-01-01")
    parser.add_argument("--output_dir")
    parser.add_argument("--languages")
    parser.add_argument("--min_stars_count", type=int, default=consts.MIN_STARS_COUNT)
    parser.add_argument("--api_token")
    parser.add_argument("--timeout", type=int, default=consts.TIMEOUT)
    parser.add_argument("--pagination_timeout", type=int, default=consts.PAGINATION_TIMEOUT)
    parser.add_argument("--retry_attempts", type=int, default=consts.RETRY_ATTEMPTS)
    parser.add_argument("--overwrite_existed_files", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--db_username", default=None)
    parser.add_argument("--db_password", default=None)
    parser.add_argument("--db_host", default="0.0.0.0")
    parser.add_argument("--db_port", type=int, default=5432)
    parser.add_argument("--db_name", default="postgres")

    args = parser.parse_args()
    logger.info(f"Args: {args}")

    db = DBHandler(
        db_username=args.db_username,
        db_password=args.db_password,
        db_host=args.db_host,
        db_port=args.db_port,
        db_name=args.db_name,
        db_schema="github",
    )

    extractor = RepoStructureExtractor(
        output_dir=args.output_dir,
        pagination_timeout=args.pagination_timeout,
        timeout=args.timeout,
        min_stars_count=args.min_stars_count,
        api_token=args.api_token,
        db=db,
    )

    extractor.run(
        start_date=args.start_date,
        end_date=args.end_date,
        languages=args.languages,
        overwrite=args.overwrite_existed_files,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
