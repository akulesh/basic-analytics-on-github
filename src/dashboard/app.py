"""
TODO: owner report
TODO: check caching
"""

import argparse

import streamlit as st

from src.dashboard.analytics import DataAnalytics
from src.dashboard.blocks import (
    add_intro_block,
    add_kpi_block,
    add_main_block,
    add_package_explorer,
    add_repo_explorer,
)
from src.dashboard.utils import add_date_selector, add_multiselect, clear_cache
from src.utils.api import SUPPORTED_LANGUAGES
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
def get_packages_data(_da: DataAnalytics):
    return _da.get_packages_data()


class Dashboard:
    """Streamlit-based dashboard with GitHub analytics"""

    def __init__(self, db_username=None, db_password=None, min_topic_freq=1, **kwargs):
        self.da = create_data_session(db_username=db_username, db_password=db_password, **kwargs)
        self.filters_df = get_filters_info(self.da)
        self.topics_df = get_topics_data(self.da)
        self.packages_df = get_packages_data(self.da)
        self.min_topic_freq = min_topic_freq

    @staticmethod
    def get_packages(data, threshold: int = 1):
        data = data[data["freq"] >= threshold]
        return list(data["package"].dropna())

    @staticmethod
    def get_licenses(data):
        stats = data.groupby("license")["n_repos"].sum().sort_values(ascending=False)
        return list(stats.keys())

    def filter_topics_df(self, language_list, lang_filter):
        if len(language_list) != len(lang_filter):
            topics_df = self.topics_df[self.topics_df["language"].isin(lang_filter)]
        else:
            topics_df = self.topics_df

        return topics_df[topics_df["n_repos"] >= self.min_topic_freq]

    def build(self):
        add_intro_block(self.filters_df)

        clear_cache()

        creation_date_range = add_date_selector()
        last_commit_date_range = add_date_selector(key_prefix="last_commit_date")

        language_list = [
            lang for lang in self.filters_df["language"] if lang in SUPPORTED_LANGUAGES.values()
        ]
        lang_filter = add_multiselect(
            f=st.sidebar,
            options=language_list,
            entity="language",
            title="🌐 Primary Language",
            default=None,
            add_reset_button=True,
            reset_button_name="Select All",
        )

        repos_df = self.da.get_repo_analytics_data(
            creation_date_range=creation_date_range,
            last_commit_date_range=last_commit_date_range,
            languages=lang_filter,
        )
        topics_df = self.filter_topics_df(language_list, lang_filter)

        kpi_report = self.da.get_kpi_report(repos_df, topics_df)
        add_kpi_block(kpi_report)

        if not repos_df.empty:
            add_main_block(repos_df, topics_df)

        topics = list(topics_df["topic"].drop_duplicates())
        licenses = self.get_licenses(repos_df)
        packages = self.get_packages(self.packages_df)

        add_repo_explorer(
            self.da,
            creation_date_range=creation_date_range,
            last_commit_date_range=last_commit_date_range,
            topics=topics,
            licenses=licenses,
            languages=lang_filter,
            packages=packages,
        )

        add_package_explorer()


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
