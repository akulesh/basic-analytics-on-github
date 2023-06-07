import base64
import logging
import sys
import os
from time import time

import requests
from pyspark.sql import SparkSession


os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
API_TOKEN = os.getenv("API_TOKEN")
SUPPORTED_LANGUAGES = [
    "python",
    "jupyter-notebook",
    "markdown",
    "html",
    "shell",
    "java",
    "javascript",
    "typescript",
    "c",
    "cpp",
    "csharp",
    "rust",
    "go",
]


class Logger:
    _instance = None

    def __init__(self, name=None, filename="log.log", level=logging.INFO):
        if Logger._instance is None:
            Logger._instance = logging.getLogger(name)
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler = logging.FileHandler(filename)
            handler.setFormatter(formatter)
            Logger._instance.addHandler(handler)
            Logger._instance.setLevel(level)

    def __getattr__(self, attr):
        return getattr(Logger._instance, attr)


logger = Logger(name="my_logger", level=logging.INFO)


def get_spark_session(app_name="github", spark_memory="4g"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.executor.memory", spark_memory)
        .config("spark.driver.memory", spark_memory)
        .config("spark.sql.debug.maxToStringFields", 1000)
    ).getOrCreate()


def make_safe_request(url, headers=None, retry_attempts=3, timeout=30):
    counter = 0
    while counter <= retry_attempts:
        response = requests.get(url, headers=headers, timeout=timeout)
        if response.status_code == 200:
            return response

        logger.exception(
            f"🚷 Error occurred while making request to {url}: {response.text}. Retrying..."
        )
        counter += 1
        time.sleep(timeout)
        continue

    response.raise_for_status()
    raise requests.exceptions.HTTPError(
        f"❌ Failed to make safe request to {url} after {retry_attempts} attempts."
    )


def get_headers(api_token=None):
    api_token = api_token or API_TOKEN
    return {"Authorization": f"Bearer {api_token}"}


def decode_content(content):
    return base64.b64decode(content).decode("utf-8")


def get_languages(value) -> list:
    output = value or SUPPORTED_LANGUAGES
    if isinstance(output, str):
        output = output.split(",")

    return output
