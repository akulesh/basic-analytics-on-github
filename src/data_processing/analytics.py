from src.data_processing.db_handler import DBHandler


class DataAnalytics:
    """Aggregate data and create tables for analytics"""

    def __init__(self, db: DBHandler):
        self.db = db
        self.schema = db.schema

    @staticmethod
    def _fix_values(values):
        if not isinstance(values, tuple):
            values = tuple(values)

        if len(values) == 1:
            values += values

        return values

    def get_kpi_report(self, min_creation_date, max_creation_date, languages=None) -> dict:
        min_creation_year, min_creation_month = min_creation_date
        max_creation_year, max_creation_month = max_creation_date

        condition = f"""
        WHERE creation_year >= {min_creation_year}
            AND creation_year <= {max_creation_year}
            AND creation_month >= {min_creation_month}
            AND creation_month <= {max_creation_month}
        """

        if languages:
            languages = self._fix_values(languages)
            condition += f'AND "language" IN {languages}'

        query = f"""
        SELECT SUM(n_repos) AS n_repos,
            SUM(n_owners) AS n_owners,
            SUM(n_archived_repos) AS n_archived_repos,
            SUM(n_repos_with_license) AS n_repos_with_license,
            SUM(n_repos_with_topic) AS n_repos_with_topic,
            SUM(n_repos_with_desc) AS n_repos_with_desc,
            SUM(n_repos_with_wiki) AS n_repos_with_wiki,
            ROUND(AVG(size)) AS size,
            ROUND(AVG(stars), 1) AS stars,
            ROUND(AVG(watchers), 1) AS watchers,
            ROUND(AVG(forks), 1) AS forks,
            ROUND(AVG(open_issues), 1) AS open_issues,
            CEIL(AVG(days_since_creation)) AS days_since_creation,
            CEIL(AVG(days_since_last_commit)) AS days_since_last_commit
        FROM {self.schema}.repo_analytics
        """
        query += condition
        data = self.db.read_sql(query).iloc[0]
        output = data.to_dict()

        query = f"""
        SELECT COUNT(DISTINCT topic) AS n_topics
        FROM {self.schema}.topic_analytics
        """

        query += condition
        data = self.db.read_sql(query).iloc[0]
        output = {**output, **data.to_dict()}

        return output

    def get_repo_report(
        self, min_date, max_date, languages=None, topics=None, sort_by="stars", limit=None
    ):
        query = f"""
        SELECT r."name",
            r."language",
            r.topics,
            r."owner",
            r.url,
            r.license,
            r.description,
            r.archived,
            r.has_wiki,
            r.stars,
            r.forks,
            r.open_issues,
            r.days_since_creation,
            r.days_since_last_commit,
            r.creation_date,
            r.last_commit_date
        FROM {self.schema}.repo AS r
        """

        conditions = [
            f"r.creation_date > '{min_date}'",
            f"r.creation_date < '{max_date}'",
        ]

        if languages:
            languages = self._fix_values(languages)
            conditions.append(f'r."language" IN {languages}')

        if topics:
            topics = self._fix_values(topics)
            conditions.append(f"rt.topic IN {topics}")
            query += f"JOIN {self.schema}.repo_topic AS rt ON r.id = rt.repo_id\n"

        query += "\tWHERE " + "\n\t\tAND ".join(conditions)
        query += f"\n\tORDER BY r.{sort_by} DESC"

        if limit:
            query += f"\n\tLIMIT {limit}"

        return self.db.read_sql(query)

    def run(self):
        pass
