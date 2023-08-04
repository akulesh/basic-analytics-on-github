import os
import re
from glob import glob

import pandas as pd
from tqdm import tqdm

from src.utils.api import (
    SUPPORTED_LANGUAGES,
    decode_content,
    get_date_range,
    get_languages_from_path,
)
from src.utils.db_handler import DBHandler
from src.utils.logger import logger


TARGET_TABLE = "package"


class PythonPackagesExtractor:
    """Parsing, transformation and loading of Python requirements into DB"""

    def __init__(self):
        pass

    @staticmethod
    def extract_version(input_str):
        version_pattern = r"\b\d+(\.\d+){0,2}\b"
        match = re.search(version_pattern, input_str)

        return match.group() if match else str()

    @staticmethod
    def extract_package_info(line):
        # Define the regular expression pattern
        pattern = r"^\s*([\w\-]+)\s*([><=~]+)?\s*([\w\d\.]+)?\s*(.*?)\s*(;.*)?$"
        # Remove comments from the line
        line_without_comment = re.sub(r"#.*$", "", line)

        # Match the pattern against the cleaned line
        match = re.match(pattern, line_without_comment)

        if not match:
            return (None, None, None)
        package = match[1]
        operator = match[2] or None
        version = match[3] or None

        version_operators = ["~=", ">=", "<=", "==", ">", "<"]

        if operator and operator not in version_operators:
            operator = None

        return (package, operator, version)

    def parse_content(self, content: str, repo_id: str = None, language: str = None) -> list:
        packages_list = []
        content = decode_content(content)

        if not content:
            return [{"repo_id": repo_id, "language": language}]

        # Split the input string by newline character to get individual lines
        lines = content.strip().lower().split("\n")

        for line in lines:
            # Ignore lines that do not start with a letter (ignoring comments and empty lines)
            if line.strip() and line[0].isalpha():
                package, operator, version = self.extract_package_info(line)

                # Check if the line contains package and version information
                if package:
                    package_dict = {
                        "repo_id": repo_id,
                        "language": language,
                        "package": package,
                        "version": version,
                        "operator": operator,
                    }
                    packages_list.append(package_dict)

        return packages_list

    def prepare(self, data: pd.DataFrame, language: str = None) -> pd.DataFrame:
        df = pd.concat(
            (
                pd.DataFrame(
                    self.parse_content(content=r.content, repo_id=r.repo_id, language=language)
                )
                for r in data.itertuples()
            ),
            axis=0,
            ignore_index=True,
        )
        df["version"] = df["version"].fillna("0.0.0").astype(str)
        return (
            df.sort_values("version")
            .drop_duplicates(subset=["repo_id", "package"], keep="last")
            .reset_index(drop=True)
        )

    def get_filenames(
        self, input_dir: str, language: str, start_date: str = None, end_date: str = None
    ):
        key_path = "key=requirements.*\\.txt"
        if start_date is None and end_date is None:
            return glob(os.path.join(input_dir, f"language={language}", key_path, "*.parquet"))
        return [
            os.path.join(input_dir, f"language={language}", key_path, f"{date}.parquet")
            for date in get_date_range(start_date, end_date)
        ]

    def run(
        self,
        db: DBHandler,
        input_dir: str,
        start_date: str,
        end_date: str = None,
        languages: list = None,
    ):
        languages = languages or get_languages_from_path(input_dir)
        for language in languages:
            logger.info(f"🕐 Language: {language}")

            filenames = self.get_filenames(
                input_dir, language, start_date=start_date, end_date=end_date
            )
            for filename in tqdm(filenames, total=len(filenames)):
                if os.path.exists(filename):
                    input_df = pd.read_parquet(filename)
                    output_df = self.prepare(
                        input_df, language=SUPPORTED_LANGUAGES.get(language, language)
                    )
                    db.update_table(output_df, TARGET_TABLE)

                else:
                    logger.warning(f"File '{filename}' does not exist.")

            logger.info(f"✅ Processing completed for '{language}'!")

        db.drop_duplicates(table=TARGET_TABLE, key_columns=["repo_id", "package"])
