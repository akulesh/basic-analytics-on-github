import base64
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from src.utils.logger import logger


API_TOKEN = os.getenv("API_TOKEN")
SUPPORTED_LANGUAGES = {
    "python": "Python",
    "jupyter-notebook": "Jupyter Notebook",
    "markdown": "Markdown",
    "html": "HTML",
    "shell": "Shell",
    "java": "Java",
    "javascript": "JavaScript",
    "typescript": "TypeScript",
    "c": "C",
    "cpp": "C++",
    "csharp": "C#",
    "rust": "Rust",
    "go": "Go",
}


def make_safe_request(url, headers=None, retry_attempts=12, timeout=30):
    counter = 0
    while counter <= retry_attempts:
        response = requests.get(url, headers=headers, timeout=timeout)

        if response.status_code == 200:
            return response

        if response.status_code == 403:
            logger.exception(
                f"🚷 403 Client Error: rate limit exceeded for url: {url}. Retrying..."
            )
            counter += 1
            time.sleep(timeout)
            continue

        break

    return response


def get_headers(api_token=None):
    api_token = api_token or API_TOKEN
    return {"Authorization": f"Bearer {api_token}"}


def decode_content(content):
    try:
        text = base64.b64decode(content).decode("utf-8")
    except Exception:
        text = None

    return text


def get_languages(values: list | str = None) -> list:
    values = values or SUPPORTED_LANGUAGES

    if isinstance(values, str):
        values = values.split(",")

    values = [lang for lang in values if lang in SUPPORTED_LANGUAGES]
    if not values:
        raise ValueError("Language list is empty!")

    return values


def get_current_date(shift=0):
    current_date = datetime.now().date()
    previous_date = current_date - timedelta(days=shift)
    return previous_date.strftime("%Y-%m-%d")


def get_date_range(start_date: str, end_date: str = None):
    end_date = end_date or start_date
    if end_date < start_date:
        raise ValueError("'end_date' must be >= than 'start_date'")

    return pd.date_range(start=start_date, end=end_date, freq="D").strftime("%Y-%m-%d")


def get_languages_from_path(path: str):
    values = [x.split("=")[-1] for x in os.listdir(path)]
    return [x for x in values if x in SUPPORTED_LANGUAGES]
