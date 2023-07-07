"""
Github API scrapping
"""

import argparse
import os

import pandas as pd
from tqdm import tqdm
from prefect import flow

from src.utils.api import make_safe_request, get_headers
from src.utils.db_handler import DBHandler
from src.utils.logger import logger


SUPPORTED_LANGUAGES = ["Python", "Jupyter Notebook"]


class RepoStructureExtractor:
    """Extraction of repository structure"""

    def __init__(
        self,
        output_dir: str,
        api_token: str = None,
        retry_attempts: int = 3,
        timeout: int = 30,
        languages: list = None,
    ):
        self.headers = get_headers(api_token)
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        self.languages = languages or SUPPORTED_LANGUAGES
        self.output_dir = output_dir

    def fetch_repo_structure(self, owner: str, repo: str, branch: str):
        url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
        response = make_safe_request(
            url,
            headers=self.headers,
            retry_attempts=self.retry_attempts,
            timeout=self.timeout,
        ).json()

        data = pd.DataFrame(response["tree"])
        data["owner"] = owner
        data["repo"] = repo
        data["branch"] = branch
        columns = ["owner", "repo", "branch", "path", "type", "url", "size"]

        return data[columns]

    @staticmethod
    def select_repo_info(
        db, language: str, creation_date: str, min_stars: int = 1, limit: int = None
    ):
        q = f"""
        SELECT owner, name AS repo, default_branch AS branch
        FROM github.repo
        WHERE language = '{language}'
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
        path_pattern: str = None,
        overwrite_existed_files: bool = False,
    ):
        filename = os.path.join(self.output_dir, f"language={language}", f"{creation_date}.parquet")
        if not overwrite_existed_files and os.path.exists(filename):
            logger.info(f"overwrite_existed_files: {overwrite_existed_files}")
            logger.info(f"File '{filename}' already exists!")
            return self.output_dir

        n_rows, n_cols = repo_df.shape
        logger.info(f"Repo data shape: {n_rows, n_cols}")

        output = []
        for _, row in tqdm(repo_df.iterrows(), total=n_rows):
            df = self.fetch_repo_structure(
                owner=row["owner"], repo=row["repo"], branch=row["branch"]
            )
            output.append(df)

        output = pd.concat(output)
        if path_pattern:
            output = output[output["path"].str.contains(path_pattern)]

        if not output.empty:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            output.to_parquet(filename, index=False)
            logger.info("Data has been extracted and saved!")
        else:
            logger.info("The output is empty!")

        return self.output_dir

    def run(
        self,
        db: DBHandler,
        start_date: str,
        end_date: str = None,
        min_stars: int = 1,
        limit: int = None,
        **kwargs,
    ):
        end_date = end_date or start_date

        if end_date < start_date:
            raise ValueError("'end_date' must be greater than 'start_date'")

        dates = pd.date_range(start=start_date, end=end_date, freq="D").strftime("%Y-%m-%d")

        for creation_date in dates:
            logger.info(f"Created date: '{creation_date}'")

            for language in self.languages:
                logger.info(f"🕐 Language: {language}")
                repo_df = self.select_repo_info(
                    db, language, creation_date, min_stars=min_stars, limit=limit
                )
                if not repo_df.empty:
                    self.extract(repo_df, language, creation_date=creation_date, **kwargs)

            logger.info(f"✅ Processing completed for 'creation_date={creation_date}'!")


@flow(
    name="repo-structure-extraction",
    flow_run_name="Repo Structure Extraction Flow",
    log_prints=True,
)
def extract_repo_structure(
    input_dir: str,
    output_dir: str,
    start_date: str = "2020-01-01",
    end_date: str = None,
    min_stars_count: int = 1,
    limit: int = None,
    path_pattern: str = None,
    overwrite_existed_files: bool = False,
    **kwargs,
):
    extractor = RepoStructureExtractor(**kwargs)

    extractor.run(
        input_dir=input_dir,
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date,
        min_stars_count=min_stars_count,
        limit=limit,
        path_pattern=path_pattern,
        overwrite_existed_files=overwrite_existed_files,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir")
    parser.add_argument("--output_dir")
    parser.add_argument("--path_pattern")
    parser.add_argument("--start_date", default="2020-01-01")
    parser.add_argument("--end_date", default=None)
    parser.add_argument("--languages", default=None)
    parser.add_argument("--min_stars_count", type=int, default=1)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--api_token", default=None)
    parser.add_argument("--overwrite_existed_files", action="store_true")

    args = parser.parse_args()
    logger.info(f"Args: {args}")

    extract_repo_structure(
        start_date=args.start_date,
        end_date=args.end_date,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        languages=args.languages,
        min_stars_count=args.min_stars_count,
        path_pattern=args.path_pattern,
        limit=args.limit,
        api_token=args.api_token,
        overwrite_existed_files=args.overwrite_existed_files,
    )


if __name__ == "__main__":
    main()
