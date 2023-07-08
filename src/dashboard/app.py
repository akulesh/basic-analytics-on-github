"""
TODO: owner report
TODO: check caching
"""

import argparse
from datetime import datetime

import streamlit as st

from src.dashboard.analytics import DataAnalytics
from src.dashboard.components import (
    add_date_picker,
    add_multiselect,
    plot_bar_chart,
    plot_corr_matrix_heatmap,
    plot_pie_chart,
    plot_wordcloud,
)
from src.utils.db_handler import DBHandler


@st.cache_resource
def create_data_session(db_username=None, db_password=None, **kwargs):
    db = DBHandler(db_schema="github", db_username=db_username, db_password=db_password, **kwargs)
    return DataAnalytics(db)


@st.cache_data
def get_filters_info(_da: DataAnalytics):
    return _da.get_filters_info()


@st.cache_data
def get_topics_data(_da: DataAnalytics):
    return _da.get_topics_data()


@st.cache_data
def get_last_updated_date():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class Dashboard:
    """Streamlit-based dashboard with GitHub analytics"""

    def __init__(self, db_username=None, db_password=None, **kwargs):
        self.da = create_data_session(db_username=db_username, db_password=db_password, **kwargs)
        self.filters_df = get_filters_info(self.da)
        self.topics_df = get_topics_data(self.da)

    @staticmethod
    def add_kpi_block(kpi_report):
        cols = st.columns(4)
        cols[0].metric("Number of Repositories", int(kpi_report.get("n_repos") or 0))
        cols[1].metric("Number of Owners", int(kpi_report.get("n_owners") or 0))
        cols[2].metric("Number of Languages", int(kpi_report.get("language") or 0))
        cols[3].metric("Number of Topics", int(kpi_report.get("n_topics") or 0))

        cols[0].metric("Avg. Number of Stars", round(kpi_report.get("stars") or 0, 1))
        cols[1].metric("Avg. Number of Watchers", "--")
        cols[2].metric("Avg. Number of Forks", round(kpi_report.get("forks") or 0, 1))
        cols[3].metric("Avg. Number of Open Issues", round(kpi_report.get("open_issues") or 0, 1))

    @staticmethod
    def plot_has_license(data):
        df = DataAnalytics.get_has_property_report(
            data, has_property_col="n_repos_with_license", label="has_license"
        )
        plot_pie_chart(
            df,
            group="has_license",
            metric="n_repos",
            hole=0.5,
            color_discrete_map={"Yes": "green", "No": "lightblue"},
            title="Does a repo have a license?",
        )

    @staticmethod
    def plot_repos_by_license(data, top_n: int = 5):
        df = DataAnalytics.get_categorical_data(
            data, group="license", metric="n_repos", top_n=top_n
        )
        plot_pie_chart(
            df,
            group="license",
            metric="n_repos",
            showlegend=True,
            textinfo="percent",
            title=f"Top-{top_n} Most Common Licenses",
            labels={"license": "License", "n_repos": "Number of Repositories"},
        )

    @staticmethod
    def plot_repos_by_creation_year(data):
        # Number of repositories grouped by the year of creation
        df = data.groupby("creation_year").agg({"n_repos": sum}).reset_index()
        plot_bar_chart(
            df,
            group="creation_year",
            metric="n_repos",
            title="Repository creation date",
            labels={"creation_year": "Date created", "n_repos": "Number of repositories"},
        )

    @staticmethod
    def plot_repos_by_last_commit_year(data):
        # Number of repositories grouped by the year of the last push
        df = data.groupby("last_commit_year").agg({"n_repos": sum}).reset_index()
        plot_bar_chart(
            df,
            group="last_commit_year",
            metric="n_repos",
            title="Repository last commit date",
            labels={
                "last_commit_year": "Last commit date",
                "n_repos": "Number of repositories",
            },
            marker_color="royalblue",
        )

    @staticmethod
    def plot_repos_by_language(data):
        # Language
        df = data.groupby("language").agg({"n_repos": sum}).sort_values(by="n_repos").reset_index()
        plot_bar_chart(
            df,
            group="n_repos",
            metric="language",
            update_xaxes=False,
            title="Repository primary language",
            orientation="h",
            color="n_repos",
            marker_color="rebeccapurple",
        )

    @staticmethod
    def plot_repos_by_branch(data, top_n=5):
        # Default branch
        df = DataAnalytics.get_categorical_data(
            data, group="default_branch", metric="n_repos", top_n=top_n
        )
        plot_pie_chart(
            df,
            group="default_branch",
            metric="n_repos",
            textinfo="percent+label",
            showlegend=True,
            title="Repository default branch",
        )

    @staticmethod
    def plot_is_archived(data):
        df = DataAnalytics.get_has_property_report(
            data, has_property_col="n_archived_repos", label="is_archived"
        )
        plot_pie_chart(
            df,
            group="is_archived",
            metric="n_repos",
            hole=0.5,
            color_discrete_map={"Yes": "green", "No": "lightblue"},
            title="Is a repo archived?",
        )

    @staticmethod
    def plot_wiki(data):
        df = DataAnalytics.get_has_property_report(
            data, has_property_col="n_repos_with_wiki", label="has_wiki"
        )
        plot_pie_chart(
            df,
            group="has_wiki",
            metric="n_repos",
            hole=0.5,
            color_discrete_map={"Yes": "green", "No": "lightblue"},
            title="Does a repo have wiki?",
        )

    @staticmethod
    def plot_has_topic(data):
        df = DataAnalytics.get_has_property_report(
            data, has_property_col="n_repos_with_topic", label="has_topic"
        )
        plot_pie_chart(
            df,
            group="has_topic",
            metric="n_repos",
            hole=0.5,
            color_discrete_map={"Yes": "green", "No": "lightblue"},
            title="Does a repo have a topic?",
        )

    @staticmethod
    def plot_topics_freq(data, threshold=10):
        df = DataAnalytics.get_topic_report(data, threshold=threshold)
        plot_bar_chart(
            df,
            group="freq",
            metric="count",
            title="How many times a topic is mentioned?",
            labels={"freq": "Topic frequency", "count": "Number of topics"},
        )

    @staticmethod
    def plot_topic_cloud(data, top_n=100):
        data = data[:top_n]
        data = dict(zip(data["topic"], data["n_repos"]))
        data.pop("__NA__", None)
        plot_wordcloud(data, title="The most popular topics")

    @staticmethod
    def get_topics(data, threshold: int = 10):
        data = data[data["n_repos"] >= threshold]
        return list(data["topic"])

    @staticmethod
    def get_licenses(data):
        stats = data.groupby("license")["n_repos"].sum().sort_values(ascending=False)
        return list(stats.keys())

    def plot_repo_table(self, creation_date_range=None, last_commit_date_range=None, **kwargs):
        data = self.da.get_repo_report(
            creation_date_range=creation_date_range,
            last_commit_date_range=last_commit_date_range,
            **kwargs,
        )

        st.dataframe(
            data,
            column_config={
                "name": st.column_config.TextColumn("Repository Name"),
                "language": st.column_config.TextColumn("Primary Language"),
                "stars": st.column_config.NumberColumn("Stars", format="%d ⭐"),
                "forks": st.column_config.NumberColumn("Forks", format="%d 🍴"),
                "open_issues": st.column_config.NumberColumn("Open Issues", format="%d ❓"),
                "archived": st.column_config.CheckboxColumn("Archived"),
                "has_wiki": st.column_config.CheckboxColumn("Has Wiki"),
                "topics": st.column_config.ListColumn("Topics"),
                "description": st.column_config.TextColumn("Repository Description"),
                "url": st.column_config.LinkColumn("Link"),
                "license": st.column_config.TextColumn("License"),
                "owner": st.column_config.TextColumn("Owner"),
                "days_since_creation": st.column_config.NumberColumn("Days Since Creation"),
                "days_since_last_commit": st.column_config.NumberColumn("Days Since Last Commit"),
                "creation_date": st.column_config.DateColumn("Creation Date", format="YYYY-MM-DD"),
                "last_commit_date": st.column_config.DateColumn(
                    "Last Commit Date", format="YYYY-MM-DD"
                ),
            },
        )
        n_records = data.shape[0]
        st.write(f"Number of records: {n_records}")

    def add_repo_report(
        self,
        creation_date_range=None,
        last_commit_date_range=None,
        topics: list = None,
        licenses: list = None,
        languages: list = None,
    ):
        st.header("⛵ Repository Exploration")
        menu = st.columns([1, 1, 1, 1, 1, 1])
        params = {}

        with menu[0]:
            params["exclude_without_topic"] = st.checkbox("Exclude repos without topics")

        with menu[1]:
            params["exclude_without_license"] = st.checkbox("Exclude repos without license")

        with menu[2]:
            params["exclude_archived"] = st.checkbox("Exclude archived repos")

        with menu[3]:
            params["exclude_without_wiki"] = st.checkbox("Exclude repos without wiki")

        menu = st.columns([2, 2, 1, 1, 1, 1])

        col = menu[0]
        with col:
            params["topic_filter"] = add_multiselect(
                col, topics, entity="topic", exclude_empty=params["exclude_without_topic"]
            )

        col = menu[1]
        with col:
            params["license_filter"] = add_multiselect(
                col, licenses, entity="license", exclude_empty=params["exclude_without_license"]
            )

        with menu[4]:
            params["sort_field"] = st.selectbox(
                "Order By",
                options=[
                    "stars",
                    "forks",
                    "open_issues",
                    "days_since_created",
                    "days_last_commit",
                ],
            )

        with menu[5]:
            params["limit"] = st.number_input(
                "Limit", min_value=1, max_value=None, value=100, step=50
            )

        self.plot_repo_table(
            creation_date_range=creation_date_range,
            last_commit_date_range=last_commit_date_range,
            languages=languages,
            **params,
        )

    def add_main_block(self, repos_df, topics_df):
        # sourcery skip: extract-method
        cols = st.columns(2)
        with cols[0]:
            self.plot_repos_by_creation_year(repos_df)
            self.plot_repos_by_language(repos_df)
            self.plot_has_license(repos_df)
            self.plot_is_archived(repos_df)
            # self.plot_repos_by_branch(repos_df)
            self.plot_has_topic(repos_df)

        with cols[1]:
            self.plot_repos_by_last_commit_year(repos_df)
            plot_corr_matrix_heatmap(
                repos_df,
                columns=[
                    "stars",
                    "forks",
                    "open_issues",
                    "size",
                    "days_since_creation",
                    "days_since_last_commit",
                ],
                title="How different columns are correlated?",
            )
            self.plot_repos_by_license(repos_df, top_n=5)
            self.plot_wiki(repos_df)
            if not topics_df.empty:
                self.plot_topic_cloud(topics_df)

    def build(self):
        st.title("📊 Github Analytics Dashboard")
        st.info(
            body="""
            #### Note
            Put some information about the app, their limitations, data collection process, etc.
            """,
            icon="ℹ️",
        )

        if self.filters_df.empty:
            st.markdown("⛔ Data is not prepared! Please load the data into the DB.")

        st.sidebar.title("")

        min_year, max_year = int(self.filters_df["creation_start_year"].min()), int(
            self.filters_df["creation_end_year"].max()
        )
        creation_date_range = add_date_picker(min_year, max_year)

        min_year, max_year = int(self.filters_df["last_commit_start_year"].min()), int(
            self.filters_df["last_commit_end_year"].max()
        )
        last_commit_date_range = add_date_picker(min_year, max_year, key_prefix="last_commit_date")

        st.sidebar.header("🌐 Primary Language")
        language_list = list(self.filters_df["language"])
        lang_filter = add_multiselect(
            f=st.sidebar,
            options=language_list,
            entity="language",
            default=language_list,
            add_reset_button=True,
            reset_button_name="Select All",
        )

        repos_df = self.da.get_repo_analytics_data(
            creation_date_range=creation_date_range,
            last_commit_date_range=last_commit_date_range,
            languages=lang_filter,
        )

        kpi_report = self.da.get_kpi_report(repos_df)
        self.add_kpi_block(kpi_report)

        if len(language_list) != len(lang_filter):
            topics_df = self.topics_df[self.topics_df["language"].isin(lang_filter)]
        else:
            topics_df = self.topics_df

        if not repos_df.empty:
            self.add_main_block(repos_df, topics_df)

        topics = self.get_topics(topics_df)
        licenses = self.get_licenses(repos_df)
        self.add_repo_report(
            creation_date_range=creation_date_range,
            last_commit_date_range=last_commit_date_range,
            topics=topics,
            licenses=licenses,
            languages=lang_filter,
        )

        st.sidebar.markdown("---")
        if st.sidebar.button("Clear Cache"):
            st.cache_resource.clear()
            st.cache_data.clear()

        updated_at = get_last_updated_date()
        st.sidebar.markdown(f"> *Last cache update*: `{updated_at}`")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_username")
    parser.add_argument("--db_password")
    parser.add_argument("--db_host")
    parser.add_argument("--db_port", type=int)
    parser.add_argument("--db_name")
    args = parser.parse_args()

    st.set_page_config(page_title="Github Analytics Dashboard", page_icon="🖥️", layout="wide")

    dashboard = Dashboard(
        db_username=args.db_username,
        db_password=args.db_password,
        db_host=args.db_host,
        db_port=args.db_port,
        db_name=args.db_name,
    )
    dashboard.build()

    # add_owners_report(repos)


# def add_owners_report(repos):
#     st.header("Owners Exploration")
#     data = (
#         repos.groupby(["repo_owner", "language_display_name"])
#         .agg({"stargazers_count": "sum", "id": "nunique"})
#         .reset_index()
#     )
#     top_owners = data.groupby("repo_owner")["stargazers_count"].sum().sort_values()[-10:]
#     data = data[data["repo_owner"].isin(top_owners.index)].sort_values(by="stargazers_count")
#     fig = px.bar(
#         data, y="repo_owner", x="stargazers_count", color="language_display_name", orientation="h"
#     )
#     fig.update_yaxes(tickmode="array", tickvals=data["repo_owner"])
#     st.plotly_chart(fig)


if __name__ == "__main__":
    main()
