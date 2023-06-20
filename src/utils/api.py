import base64
import os
from time import time

import requests

from src.utils.logger import logger


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
