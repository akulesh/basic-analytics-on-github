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
        table=None,
        creation_date_range=None,
        last_commit_date_range=None,
        languages: list = None,
        licenses: list = None,
        topics: str = None,
        exclude_archived: bool = False,
        exclude_without_wiki: bool = False,
        exclude_without_license: bool = False,
        exclude_without_topic: bool = False,
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
            conditions.append("has_license = 1")

        if exclude_without_topic:
            conditions.append("topic != '__NA__'")

        if topics:
            topics = self._fix_values(topics)
            conditions.append(f"topic IN {topics}")

        if table:
            conditions = [f"{table}.{cond}" for cond in conditions]

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

    def get_topics_data(self, min_repos: int = 1):
        query = f"""
        SELECT
            "language",
            topic,
            SUM(n_repos) AS n_repos
        FROM {self.schema}.topic_analytics AS ta
        GROUP BY "language", topic
        HAVING SUM(n_repos) >= {min_repos}
        ORDER BY n_repos DESC
        """

        return self.db.read_sql(query)

    def get_packages_data(self):
        query = f"""
        SELECT
            "language",
            package,
            COUNT(*) AS freq
        FROM {self.schema}.package AS ta
        GROUP BY "language", package
        ORDER BY freq DESC
        """

        return self.db.read_sql(query)

    def get_repos_with_packages(self, packages: tuple):
        query = f"""
        SELECT repo_id FROM {self.schema}.package
        WHERE package IN {self._fix_values(packages)}
        GROUP BY repo_id
        HAVING COUNT(*) = {len(packages)}
        """

        df = self.db.read_sql(query)
        return set() if df.empty else set(df.repo_id)

    def get_repo_analytics_data(
        self, creation_date_range=None, last_commit_date_range=None, languages=None
    ) -> dict:
        conditions = self._get_conditions(
            creation_date_range=creation_date_range,
            last_commit_date_range=last_commit_date_range,
            languages=languages,
        )
        query = f"""
        SELECT
            language,
            license,
            creation_year,
            last_commit_year,
            SUM(n_repos) AS n_repos,
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
        if conditions:
            query += "\tWHERE " + "\n\t\tAND ".join(conditions)

        query += "GROUP BY language, license, creation_year, last_commit_year"

        return self.db.read_sql(query)

    def get_kpi_report(self, repos_df: pd.DataFrame, topics_df: pd.DataFrame) -> dict:
        data = repos_df.agg(
            {
                "n_repos": sum,
                "n_owners": sum,
                "language": "nunique",
                "stars": "mean",
                "watchers": "mean",
                "forks": "mean",
                "open_issues": "mean",
            }
        )

        output = data.to_dict()
        output["topics"] = topics_df["topic"].nunique()

        return output

    def get_repo_report(
        self,
        creation_date_range: str = None,
        last_commit_date_range: str = None,
        languages: list = None,
        topics: list = None,
        licenses: list = None,
        packages: list = None,
        sort_by: str = "stars",
        exclude_archived: bool = False,
        exclude_without_wiki: bool = False,
        exclude_without_topic: bool = False,
        exclude_without_license: bool = False,
        limit=None,
    ):
        query = f"""
        SELECT DISTINCT r.id AS repo_id,
            r."name",
            r.url,
            r."language",
            r.stars,
            r.forks,
            r.open_issues,
            r.archived,
            r.has_wiki,
            r.description,
            r.license,
            r.topics,
            r."owner",
            r.days_since_creation,
            r.days_since_last_commit,
            r.creation_date,
            r.last_commit_date
        FROM {self.schema}.repo AS r
        JOIN {self.schema}.repo_topic AS rt ON r.id = rt.repo_id
        """

        conditions = self._get_conditions(
            table="r",
            creation_date_range=creation_date_range,
            last_commit_date_range=last_commit_date_range,
            languages=languages,
            licenses=licenses,
            exclude_archived=exclude_archived,
            exclude_without_license=exclude_without_license,
            exclude_without_wiki=exclude_without_wiki,
        )

        topic_conditions = self._get_conditions(
            table="rt", topics=topics, exclude_without_topic=exclude_without_topic
        )
        conditions.extend(topic_conditions)

        if conditions:
            query += "\tWHERE " + "\n\t\tAND ".join(conditions)

        query += f"\n\tORDER BY r.{sort_by} DESC"

        if limit:
            query += f"\n\tLIMIT {limit}"

        data = self.db.read_sql(query)
        data["topics"] = data["topics"].replace("", "__NA__").str.split("|")

        if packages:
            repos_with_packages = self.get_repos_with_packages(packages)
            data = data[data["repo_id"].isin(repos_with_packages)]

        del data["repo_id"]

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

    def get_branch_data(self):
        pass

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
        data.name = "count"
        output = data[data.index < threshold]
        value = min(threshold, max(data.index))
        if value >= threshold:
            output.loc[f"{value}+"] = data[data.index >= value].sum()
        output.index.name = "freq"
        return output.reset_index()
