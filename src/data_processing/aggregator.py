from src.data_processing.db_handler import DBHandler


class DataAggregator:
    """Aggregate data and create tables for analytics"""

    def __init__(self, db: DBHandler):
        self.db = db
        self.schema = db.schema

    def create_repo_analytics_table(self):
        q = f"""DROP TABLE IF EXISTS {self.schema}.repo_analytics"""
        self.db.execute(q)

        q = f"""
        CREATE TABLE {self.schema}.repo_analytics AS
        SELECT
            "language",
            license,
            default_branch,
            DATE_TRUNC('month', creation_date) AS creation_date,
            DATE_PART('year', creation_date) AS creation_year,
            DATE_PART('month', creation_date) AS creation_month,
            DATE_PART('year', last_commit_date) AS last_commit_year,
            DATE_PART('month', last_commit_date) AS last_commit_month,
            COUNT(DISTINCT id) AS n_repos,
            COUNT(DISTINCT owner) AS n_owners,
            SUM(archived) AS n_archived_repos,
            SUM(has_license) AS n_repos_with_license,
            SUM(has_topic) AS n_repos_with_topic,
            SUM(has_description) AS n_repos_with_desc,
            SUM(has_wiki) AS n_repos_with_wiki,
            ROUND(AVG(size)) AS SIZE,
            ROUND(AVG(stars), 2) AS stars,
            ROUND(AVG(watchers), 2) AS watchers,
            ROUND(AVG(forks), 2) AS forks,
            ROUND(AVG(open_issues), 2) AS open_issues,
            CEIL(AVG(days_since_creation)) AS days_since_creation,
            CEIL(AVG(days_since_last_commit)) AS days_since_last_commit
        FROM {self.schema}.repo AS r
        GROUP BY "language",
                 license,
                 default_branch,
                 creation_date,
                 creation_year,
                 creation_month,
                 last_commit_year,
                 last_commit_month;
        """
        self.db.execute(q)

    def create_topic_analytics_table(self):
        q = f"""DROP TABLE IF EXISTS {self.schema}.topic_analytics"""
        self.db.execute(q)

        q = f"""
        CREATE TABLE {self.schema}.topic_analytics AS
        SELECT
            r."language",
            topic,
            DATE_TRUNC('month', creation_date) AS creation_date,
            DATE_PART('year', creation_date) AS creation_year,
            DATE_PART('month', creation_date) AS creation_month,
            COUNT(DISTINCT rt.repo_id) AS n_repos
        FROM {self.schema}.repo_topic AS rt
        JOIN {self.schema}.repo AS r ON r.id = rt.repo_id
        GROUP BY r."language", topic, creation_date, creation_year, creation_month
        ORDER BY n_repos DESC;
        """
        self.db.execute(q)

    def run(self):
        self.create_topic_analytics_table()
        self.create_repo_analytics_table()
