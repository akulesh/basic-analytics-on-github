import pandas as pd

from src.utils.db_handler import DBHandler


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

    def _get_conditions(
        self,
        creation_date_range=None,
        last_commit_date_range=None,
        languages=None,
        licenses=None,
        exclude_archived=None,
        exclude_without_wiki=None,
        exclude_without_license=None,
    ):
        conditions = []

        if creation_date_range:
            start_date, end_date = creation_date_range
            conditions.extend(
                (
                    f"creation_date >= '{start_date}'",
                    f"creation_date <= '{end_date}'",
                )
            )
        if last_commit_date_range:
            start_date, end_date = last_commit_date_range
            conditions.extend(
                (
                    f"last_commit_date >= '{start_date}'",
                    f"last_commit_date <= '{end_date}'",
                )
            )
        if languages:
            languages = self._fix_values(languages)
            conditions.append(f'"language" IN {languages}')

        if licenses:
            licenses = self._fix_values(licenses)
            conditions.append(f"license IN {licenses}")

        if exclude_archived:
            conditions.append("archived = 0")

        if exclude_without_wiki:
            conditions.append("has_wiki = 1")

        if exclude_without_license:
            conditions.append("license != '__NA__'")

        if exclude_without_wiki:
            conditions.append("r.has_wiki = 1")

        return conditions

    def get_filters_info(self):
        query = f"""
        SELECT "language",
            SUM(n_repos) AS n_repos,
            MIN(creation_year) AS creation_start_year,
            MAX(creation_year) AS creation_end_year,
            MIN(last_commit_year) AS last_commit_start_year,
            MAX(last_commit_year) AS last_commit_end_year
        FROM {self.schema}.repo_analytics
        GROUP BY "language"
        ORDER BY n_repos DESC;
        """

        return self.db.read_sql(query)

    def get_analytics_data(
        self, table, creation_date_range=None, last_commit_date_range=None, languages=None
    ) -> dict:
        conditions = self._get_conditions(
            creation_date_range=creation_date_range,
            last_commit_date_range=last_commit_date_range,
            languages=languages,
        )

        query = f"SELECT * FROM {self.schema}.{table}\n"
        if conditions:
            query += "\tWHERE " + "\n\t\tAND ".join(conditions)

        return self.db.read_sql(query)

    def get_kpi_report(
        self, creation_date_range=None, last_commit_date_range=None, languages=None
    ) -> dict:
        query = f"""
        SELECT SUM(n_repos) AS n_repos,
            SUM(n_owners) AS n_owners,
            COUNT(DISTINCT(language)) AS n_languages,
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

        conditions = self._get_conditions(
            creation_date_range=creation_date_range,
            last_commit_date_range=last_commit_date_range,
            languages=languages,
        )
        if conditions:
            query += "\tWHERE " + "\n\t\tAND ".join(conditions)

        data = self.db.read_sql(query).iloc[0]
        output = data.to_dict()

        query = f"""
        SELECT COUNT(DISTINCT topic) AS n_topics
        FROM {self.schema}.topic_analytics
        """
        if conditions:
            query += "\tWHERE " + "\n\t\tAND ".join(conditions)

        data = self.db.read_sql(query).iloc[0]
        output = {**output, **data.to_dict()}

        return output

    def get_repo_report(
        self,
        creation_date_range: str = None,
        last_commit_date_range: str = None,
        languages: list = None,
        topics: list = None,
        licenses: list = None,
        sort_by: str = "stars",
        exclude_archived: bool = False,
        exclude_without_wiki: bool = False,
        exclude_without_topic: bool = False,
        exclude_without_license: bool = False,
        limit=None,
    ):
        query = f"""
        SELECT r."name",
            r."language",
            r.stars,
            r.forks,
            r.open_issues,
            r.archived,
            r.has_wiki,
            r.topics,
            r.description,
            r.url,
            r.license,
            r."owner",
            r.days_since_creation,
            r.days_since_last_commit,
            r.creation_date,
            r.last_commit_date
        FROM {self.schema}.repo AS r
        """

        select_topics = f"""
            SELECT DISTINCT repo_id
            FROM {self.schema}.repo_topic AS rt
            WHERE TRUE
            """
        if exclude_without_topic:
            select_topics += "AND topic != '__NA__'\n"

        if topics:
            topics = self._fix_values(topics)
            select_topics += f"AND topic IN {topics}"

        if topics or exclude_without_topic:
            query += f"""
            JOIN ({select_topics}
            ) AS selected_repos ON selected_repos.repo_id = r.id
            """

        conditions = self._get_conditions(
            creation_date_range=creation_date_range,
            last_commit_date_range=last_commit_date_range,
            languages=languages,
            licenses=licenses,
            exclude_archived=exclude_archived,
            exclude_without_license=exclude_without_license,
            exclude_without_wiki=exclude_without_wiki,
        )
        if conditions:
            query += "\tWHERE " + "\n\t\tAND ".join(conditions)

        query += f"\n\tORDER BY r.{sort_by} DESC"

        if limit:
            query += f"\n\tLIMIT {limit}"

        data = self.db.read_sql(query)
        data["topics"] = data["topics"].replace("", "__NA__").str.split("|")

        return data

    @staticmethod
    def get_categorical_data(data, group="license", metric="n_repos", top_n: int = 10):
        grouped = (
            data.groupby(group).agg({metric: sum}).sort_values(by=metric, ascending=False)
        ).reset_index()
        grouped = grouped[grouped[group] != "__NA__"]
        values = set(grouped[:top_n][group])
        grouped.loc[~grouped[group].isin(values), group] = "Other"
        return (
            grouped.groupby(group).agg({metric: sum}).sort_values(by=metric, ascending=False)
        ).reset_index()

    @staticmethod
    def get_has_property_report(data, has_property_col: str, label: str):
        n_repos = data["n_repos"].sum()
        n_repos_with_property = int(data[has_property_col].sum())
        n_repos_without_property = n_repos - n_repos_with_property
        output = {"Yes": n_repos_with_property, "No": n_repos_without_property}
        return pd.DataFrame(output.items(), columns=[label, "n_repos"])

    @staticmethod
    def get_topic_report(data, threshold: int = 10):
        data = data.groupby("topic")["n_repos"].sum().value_counts().sort_index()
        data.name = "n_repos"
        output = data[data.index < threshold]
        value = min(threshold, max(data.index))
        output.loc[f"{value}+"] = data[data.index >= value].sum()
        output.index.name = "freq"
        return output.reset_index()
