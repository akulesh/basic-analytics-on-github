MAX_ITEMS_PER_PAGE = 100
SEARCH_ITEMS_LIMIT = 1000
MAX_REQUESTS_PER_HOUR = 5000
RATE_LIMITER_WINDOW = 3600
TIMEOUT = 60
PAGINATION_TIMEOUT = 1
RETRY_ATTEMPTS = 3
MIN_STARS_COUNT = 1

FULL_PATH_PATTERN = (
    r"^readme.md$|^pyproject.toml$|requirements.*\.txt|"
    r"setup.py|environment.yaml|pipfile|poetry|dockerfile|docker-compose"
)

DEFAULT_PATH_PATTERN = r"^readme.md$|requirements.*\.txt"
