"""
Github API scrapping
"""

import argparse
import os

import pandas as pd
from tqdm import tqdm
from prefect import flow

from src.utils import make_safe_request, get_headers, logger


SUPPORTED_LANGUAGES = ["python", "jupyter-notebook"]


class RepoStructureExtractor:
    def __init__(
        self,
        api_token: str = None,
        retry_attempts: int = 3,
        timeout: int = 30,
        languages: list = None,
    ):
        self.headers = get_headers(api_token)
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        self.languages = self.get_languages(languages)

    @staticmethod
    def get_languages(language) -> list:
        language = language or SUPPORTED_LANGUAGES
        if isinstance(language, str):
            language = [language]

        return language

    def get_repo_structure(self, full_name: str, branch: str):
        url = f"https://api.github.com/repos/{full_name}/git/trees/{branch}?recursive=1"
        response = make_safe_request(
            url,
            headers=self.headers,
            retry_attempts=self.retry_attempts,
            timeout=self.timeout,
        ).json()

        data = pd.DataFrame(response["tree"])
        data["full_name"] = full_name
        data["branch"] = branch
        columns = ["full_name", "branch", "path", "type", "url", "size"]

        return data[columns]

    def extract_repo_structure(
        self,
        input_dir,
        created_at: str,
        output_dir: str,
        language: str = "python",
        min_stars_count: int = 1,
        path_pattern: str = None,
        limit=None,
        overwrite_existed_files: bool = False,
    ):
        filename = os.path.join(output_dir, f"language={language}", f"{created_at}.parquet")
        if not overwrite_existed_files and os.path.exists(filename):
            logger.info(f"overwrite_existed_files: {overwrite_existed_files}")
            logger.info(f"File '{filename}' already exists!")
            return output_dir

        input_path = os.path.join(input_dir, f"language={language}", f"{created_at}.parquet")
        if not os.path.exists(input_path):
            logger.warning(f"File '{input_path}' does not exist!")
            return output_dir

        columns = ["full_name", "default_branch", "stargazers_count"]
        data = pd.read_parquet(input_path, columns=columns)
        data = data[data["stargazers_count"] >= min_stars_count]
        logger.info(f"Input data shape: {data.shape}")

        if limit:
            data = data[:limit]
        logger.info(f"Limit: {limit}")

        output = []
        for _, row in tqdm(data.iterrows(), total=len(data)):
            df = self.get_repo_structure(full_name=row["full_name"], branch=row["default_branch"])
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

        return output_dir

    def run(
        self,
        input_dir: str,
        output_dir: str,
        start_date: str,
        end_date: str = None,
        **kwargs,
    ):
        end_date = end_date or start_date

        if end_date < start_date:
            raise ValueError("'end_date' must be greater than 'start_date'")

        dates = pd.date_range(start=start_date, end=end_date, freq="D").strftime("%Y-%m-%d")

        for created_at in dates:
            logger.info(f"Created date: '{created_at}'")

            for language in self.languages:
                logger.info(f"🕐 Language: {language}")
                self.extract_repo_structure(
                    input_dir=input_dir,
                    created_at=created_at,
                    output_dir=output_dir,
                    language=language,
                    **kwargs,
                )

            logger.info(f"✅ Processing completed for 'created_at={created_at}'!")


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
