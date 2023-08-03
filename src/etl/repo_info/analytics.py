# TODO: refactor tables update (check 'drop_duplicates' method)

from src.utils.db_handler import DBHandler
from src.utils.logger import logger


class RepoInfoAnalytics:
    """Aggregate data and create tables for analytics"""

    def __init__(self, db: DBHandler):
        self.db = db
        self.schema = db.schema

    def update_repo_analytics_table(
        self, start_date: str, end_date: str, table: str = "repo_analytics"
    ):
        q = f"""
        INSERT INTO {self.schema}.{table} (
            language,
            license,
            default_branch,
            creation_date,
            creation_year,
            last_commit_date,
            last_commit_year,
            n_repos,
            n_owners,
            n_archived_repos,
            n_repos_with_license,
            n_repos_with_topic,
            n_repos_with_desc,
            n_repos_with_wiki,
            size,
            stars,
            watchers,
            forks,
            open_issues,
            days_since_creation,
            days_since_last_commit
        )
        SELECT
            "language",
            license,
            default_branch,
            DATE_TRUNC('month', creation_date) AS creation_date,
            DATE_PART('year', creation_date) AS creation_year,
            DATE_TRUNC('month', last_commit_date) AS last_commit_date,
            DATE_PART('year', last_commit_date) AS last_commit_year,
            COUNT(DISTINCT id) AS n_repos,
            COUNT(DISTINCT owner) AS n_owners,
            SUM(archived) AS n_archived_repos,
            SUM(has_license) AS n_repos_with_license,
            SUM(has_topic) AS n_repos_with_topic,
            SUM(has_description) AS n_repos_with_desc,
            SUM(has_wiki) AS n_repos_with_wiki,
            ROUND(AVG(size)) AS size,
            ROUND(AVG(stars), 1) AS stars,
            ROUND(AVG(watchers), 1) AS watchers,
            ROUND(AVG(forks), 1) AS forks,
            ROUND(AVG(open_issues), 1) AS open_issues,
            CEIL(AVG(days_since_creation)) AS days_since_creation,
            CEIL(AVG(days_since_last_commit)) AS days_since_last_commit
        FROM {self.schema}.repo AS r
        WHERE creation_date >= '{start_date}'
            AND creation_date <= '{end_date}'
        GROUP BY "language",
                 license,
                 default_branch,
                 creation_date,
                 creation_year,
                 last_commit_date,
                 last_commit_year
        """
        self.db.execute(q)
        self.db.drop_duplicates(
            table,
            key_columns=[
                "language",
                "license",
                "default_branch",
                "creation_date",
                "last_commit_date",
            ],
            date_col="updated_at",
        )

    def update_topic_analytics_table(
        self, start_date: str, end_date: str, table: str = "topic_analytics"
    ):
        q = f"""
        INSERT INTO {self.schema}.{table} (
            language,
            topic,
            creation_date,
            last_commit_date,
            n_repos
        )
        SELECT
            r."language",
            topic,
            DATE_TRUNC('month', creation_date) AS creation_date,
            DATE_TRUNC('month', last_commit_date) AS last_commit_date,
            COUNT(DISTINCT rt.repo_id) AS n_repos
        FROM {self.schema}.repo_topic AS rt
        JOIN {self.schema}.repo AS r ON r.id = rt.repo_id
        WHERE creation_date >= '{start_date}'
            AND creation_date <= '{end_date}'
        GROUP BY r."language", topic, creation_date, last_commit_date
        """
        self.db.execute(q)
        self.db.drop_duplicates(
            table,
            key_columns=["language", "topic", "creation_date", "last_commit_date"],
            date_col="updated_at",
        )

    def run(self, start_date: str, end_date: str = None):
        end_date = end_date or start_date
        logger.info("Updating 'topic_analytics' table...")
        self.update_topic_analytics_table(start_date, end_date)
        logger.info("✅ Table has been updated!")

        logger.info("Updating 'repo_analytics' table...")
        self.update_repo_analytics_table(start_date, end_date)
        logger.info("✅ Table has been updated!")
