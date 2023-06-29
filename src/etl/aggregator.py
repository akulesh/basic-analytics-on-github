from src.utils.db_handler import DBHandler


class DataAggregator:
    """Aggregate data and create tables for analytics"""

    def __init__(self, db: DBHandler):
        self.db = db
        self.schema = db.schema

    def clear_table(self, table):
        q = f"""DELETE FROM {self.schema}.{table}"""
        self.db.execute(q)

    def create_repo_analytics_table(self, table="repo_analytics"):
        self.clear_table(table)

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
        GROUP BY "language",
                 license,
                 default_branch,
                 creation_date,
                 creation_year,
                 last_commit_date,
                 last_commit_year
        """
        self.db.execute(q)

    def create_topic_analytics_table(self, table="topic_analytics"):
        self.clear_table(table)

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
        GROUP BY r."language", topic, creation_date, last_commit_date
        ORDER BY n_repos DESC;
        """
        self.db.execute(q)

    def run(self):
        self.create_topic_analytics_table()
        self.create_repo_analytics_table()
