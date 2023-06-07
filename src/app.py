import os
from datetime import datetime

import plotly.express as px
import streamlit as st
import pandas as pd
from wordcloud import WordCloud
import matplotlib.pyplot as plt

from src.utils import get_spark_session


OUTPUT_DIR = (
    "/Users/a_kulesh/Workspace/education/pet-projects/basic-analytics-on-github/data/processed"
)


@st.cache_data
def load_data():
    spark = get_spark_session()
    repos = spark.read.parquet(os.path.join(OUTPUT_DIR, "repos")).toPandas()
    repos = repos.drop_duplicates()
    repos["has_license"] = repos["license"] != "__NA__"
    repos["last_pushed_date"] = pd.to_datetime(repos["pushed_at"]).dt.year
    topics = spark.read.parquet(os.path.join(OUTPUT_DIR, "topics")).toPandas()

    return repos, topics


# @st.cache_data
def get_base_metrics(repos, topics):
    config = {
        "id": {"f": "nunique", "format": int},
        "repo_owner": {"f": "nunique", "format": int},
        "language": {"f": "nunique", "format": int},
        "created_at": {"f": ["min", "max"], "format": pd.Timestamp},
        "pushed_at": {"f": ["min", "max"], "format": pd.Timestamp},
        "stargazers_count": {"f": "mean", "format": lambda x: round(float(x), 1)},
        "watchers_count": {"f": "mean", "format": lambda x: round(float(x), 1)},
        "forks_count": {"f": "mean", "format": lambda x: round(float(x), 1)},
        "open_issues_count": {"f": "mean", "format": lambda x: round(float(x), 1)},
    }

    f_mapping = {col: val["f"] for col, val in config.items()}
    repos_stats = repos.agg(f_mapping).fillna("NA").to_dict()

    metrics = {}
    for column, stats in repos_stats.items():
        filtered_stats = {key: value for key, value in stats.items() if value != "NA"}
        for prefix, value in filtered_stats.items():
            metric_name = f"{prefix}_{column}"
            value = config[column]["format"](value)
            metrics[metric_name] = value

    metrics["nunique_topics"] = topics["topics"].nunique()

    return metrics


# @st.cache_data
def get_license_data(repos, top_n=10):
    license_df = repos[repos["has_license"]].groupby("license").agg({"id": "nunique"})
    license_df.columns = ["n_repos"]
    license_df = license_df.reset_index()

    all_lang_df = license_df.groupby("license").sum().reset_index()
    license_list = all_lang_df.sort_values("n_repos", ascending=False)[:top_n]["license"].to_numpy()
    license_df.loc[~license_df["license"].isin(license_list), "license"] = "Other"

    return license_df


def add_date_picker(repos):
    min_date, max_date = get_date_range(repos)
    st.sidebar.write("Repository creation date range")

    def clear():
        st.session_state.start_date = min_date
        st.session_state.end_date = max_date

    # Initialize the date range in session state
    if ("start_date" not in st.session_state) or ("end_date" not in st.session_state):
        clear()

    cols = st.sidebar.columns(2)
    cols[0].date_input(
        label="Select start date",
        value=st.session_state.start_date,
        min_value=min_date,
        max_value=max_date,
        key="start_date",
    )
    cols[1].date_input(
        label="Select end date",
        value=st.session_state.end_date,
        min_value=min_date,
        max_value=max_date,
        key="end_date",
    )

    # Add a reset button
    st.sidebar.button("Reset", on_click=clear)

    return pd.Timestamp(st.session_state.start_date), pd.Timestamp(st.session_state.end_date)


def add_kpi_block(repos, topics):
    base_metrics = get_base_metrics(repos, topics)

    cols = st.columns(4)
    cols[0].metric("Number of Repositories", base_metrics["nunique_id"])
    cols[1].metric("Number of Owners", base_metrics["nunique_repo_owner"])
    cols[2].metric("Number of Languages", base_metrics["nunique_language"])
    cols[3].metric("Number of Topics", base_metrics["nunique_topics"])

    cols[0].metric("Avg. Number of Stars", base_metrics["mean_stargazers_count"])
    cols[1].metric("Avg. Number of Watchers", "--")
    cols[2].metric("Avg. Number of Forks", base_metrics["mean_forks_count"])
    cols[3].metric("Avg. Number of Open Issues", base_metrics["mean_open_issues_count"])


def add_multiselect(
    f,
    options: list,
    entity: str = "topic",
    default=None,
    add_reset_button: bool = False,
    reset_button_name="Reset",
):
    values = (
        f.multiselect(
            f"Select the {entity.capitalize()}",
            options,
            default=default,
            key=f"{entity}_multiselect",
        )
        or options
    )

    def clear_multi():
        st.session_state[f"{entity}_multiselect"] = default or []

    if add_reset_button:
        f.button(reset_button_name, on_click=clear_multi, key=f"{entity}_reset")

    return values


def create_has_description_chart(repos):
    df = repos["has_description"].value_counts().reset_index().replace({False: "No", True: "Yes"})
    df.columns = ["Has description", "Number of Repositories"]
    fig = px.pie(
        df,
        values="Number of Repositories",
        names="Has description",
        color="Has description",
        title="Does a repo have description?",
        color_discrete_map={"Yes": "green", "No": "lightblue"},
        hole=0.5,
    )
    fig.update_traces(textposition="inside", textinfo="percent+label", showlegend=False)

    return fig


def add_basic_report(repos):
    cols = st.columns(2)

    with cols[0]:
        # Number of repositories grouped by the year of creation
        df = repos.groupby("created_at_year")["id"].nunique().reset_index()
        df.columns = ["Date created", "Number of repositories"]
        fig = px.bar(
            df,
            x="Date created",
            y="Number of repositories",
            text="Number of repositories",
            title="Repository creation date",
        )
        fig.update_traces(marker_color="steelblue")
        fig.update_xaxes(tickmode="array", tickvals=df["Date created"])
        st.plotly_chart(fig, use_container_width=True)

        # Languages
        df = repos["language_display_name"].value_counts().reset_index()
        df.columns = ["Language", "Number of Repositories"]
        fig = px.pie(
            df,
            values="Number of Repositories",
            names="Language",
            color="Language",
            title="Repository primary language",
        )
        fig.update_traces(textposition="inside", textinfo="percent+label", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with cols[1]:
        # Number of repositories grouped by the year of the last push
        df = repos.groupby("last_pushed_date")["id"].nunique().reset_index()
        df.columns = ["Date updated", "Number of repositories"]
        fig = px.bar(
            df,
            x="Date updated",
            y="Number of repositories",
            text="Number of repositories",
            title="Repository last update date",
        )
        fig.update_traces(marker_color="royalblue")
        fig.update_xaxes(tickmode="array", tickvals=df["Date updated"])
        st.plotly_chart(fig, use_container_width=True)

        # Heatmap
        fig = create_corr_matrix(repos)
        st.plotly_chart(fig, use_container_width=True)


def add_license_report(repos):
    cols = st.columns(2)

    with cols[0]:
        df = repos["has_license"].value_counts().reset_index().replace({False: "No", True: "Yes"})
        df.columns = ["Has License", "Number of Repositories"]
        fig = px.pie(
            df,
            values="Number of Repositories",
            names="Has License",
            color="Has License",
            title="Does a repo have a license?",
            color_discrete_map={"Yes": "green", "No": "lightblue"},
            hole=0.5,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with cols[1]:
        df = get_license_data(repos)
        df.columns = ["License", "Number of Repositories"]
        cols[1].write(
            px.pie(
                df,
                names="License",
                values="Number of Repositories",
                title="The Most Popular Licenses",
            )
        )


def beautify_topics(topics):
    return " ".join(f'<span class="topic">{topic}</span>' for topic in topics)


def make_clickable(row):
    return f'<a target="_blank" href="{row["html_url"]}">{row["full_name"]}</a>'


def add_repos_report(
    repos: pd.DataFrame,
    topics: pd.DataFrame,
    topics_stats: pd.DataFrame,
    min_repos_per_topic: int = 10,
):
    st.header("Repositories Exploration")
    top_menu = st.columns([1.5, 1, 1, 1, 1])
    with top_menu[0]:
        topics_list = list(
            topics_stats[topics_stats["id"] >= min_repos_per_topic].sort_values(
                by="id", ascending=False
            )["topics"]
        )
        topic_filter = add_multiselect(top_menu[0], topics_list, entity="topic")
        repo_ids = set(topics[topics["topics"].isin(topic_filter)]["id"])
        topics = topics[(topics["id"].isin(repo_ids)) & (topics["topics"].isin(set(topics_list)))]
        topics = topics[["id", "topics"]].groupby("id")["topics"].apply(list).reset_index()
        repos = repos.merge(topics, how="inner", on="id")

    with top_menu[1]:
        sort_field = st.selectbox(
            "Sort By",
            options=[
                "stargazers_count",
                "forks_count",
                "open_issues_count",
                "created_at",
                "pushed_at",
            ],
        )

    with top_menu[-2]:
        batch_size = st.selectbox("Items per Page", options=[5, 10, 25])

    with top_menu[-1]:
        total_pages = int(len(repos) / batch_size) if int(len(repos) / batch_size) > 0 else 1
        current_page = st.number_input("Page", min_value=1, max_value=total_pages, step=1)

    top_repos = repos.sort_values(by=sort_field, ascending=False)
    top_repos["topics"] = top_repos["topics"].apply(beautify_topics)
    top_repos = top_repos[(current_page - 1) * batch_size : current_page * batch_size]
    top_repos["repository"] = top_repos.apply(make_clickable, axis=1)

    # Define the CSS style for the topics
    css_style = """
        .topic {
            background-color: lightgrey;
            border-radius: 10px;
            padding: 2px 6px;
        }
    """
    columns = {
        "repository": "Repository",
        "language_display_name": "Language",
        "topics": "Topic",
        "description": "Description",
        "stargazers_count": "Stars",
        "forks_count": "Forks",
        "open_issues_count": "Open Issues",
        "created_at": "Created",
        "pushed_at": "Updated",
    }

    top_repos = top_repos.rename(columns=columns)[columns.values()]
    top_repos = top_repos.to_html(escape=False, index=False)
    top_repos = f"<style>{css_style}</style>\n{top_repos}"

    st.write(top_repos, unsafe_allow_html=True)

    bottom_menu = st.columns(7)
    with bottom_menu[-1]:
        st.markdown(f"Page № **{current_page}** of **{total_pages}** ")


def topics_coverage(stats, threshold=10):
    output = stats[stats.index < threshold].sort_index()
    output = output.to_dict()
    output[f"{threshold}+"] = stats[stats.index >= threshold].sum()
    output = pd.DataFrame(output.items(), columns=["Number of Repositories", "Number of Topics"])
    output = output.astype(str)

    return output


def add_topics_report(topics, topics_stats, repos, threshold=10):
    has_topic = (
        (topics["topics"] == "__NA__")
        .value_counts()
        .reset_index()
        .replace({False: "No", True: "Yes"})
        .rename(columns={"index": "has_topic", "topics": "num_repos"})
    )

    # st.header("Repository Topics")
    cols = st.columns(2)

    with cols[0]:
        fig = create_has_description_chart(repos)
        st.plotly_chart(fig, use_container_width=True)

        fig = px.pie(
            has_topic,
            values="num_repos",
            names="has_topic",
            hole=0.5,
            color="has_topic",
            color_discrete_map={"Yes": "green", "No": "lightblue"},
            title="Does a repos have a topic?",
        )
        fig.update_traces(textposition="inside", textinfo="percent+label", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with cols[1]:
        fig = create_wordcloud(topics_stats)
        st.pyplot(fig, use_container_width=True)

        counts = topics_stats["id"].value_counts()
        coverage_df = topics_coverage(counts, threshold=threshold)
        fig = px.bar(
            coverage_df,
            x="Number of Repositories",
            y="Number of Topics",
            text="Number of Topics",
            title="How many times a topic is mentioned?",
        )

        fig.update_xaxes(type="category", tickvals=coverage_df["Number of Repositories"])
        st.plotly_chart(fig, use_container_width=True)


def add_owners_report(repos):
    st.header("Owners Exploration")
    data = (
        repos.groupby(["repo_owner", "language_display_name"])
        .agg({"stargazers_count": "sum", "id": "nunique"})
        .reset_index()
    )
    top_owners = data.groupby("repo_owner")["stargazers_count"].sum().sort_values()[-10:]
    data = data[data["repo_owner"].isin(top_owners.index)].sort_values(by="stargazers_count")
    fig = px.bar(
        data, y="repo_owner", x="stargazers_count", color="language_display_name", orientation="h"
    )
    fig.update_yaxes(tickmode="array", tickvals=data["repo_owner"])
    st.plotly_chart(fig)


# @st.cache_data
def get_date_range(repos):
    min_date = pd.Timestamp(repos["created_at"].min())
    max_date = pd.Timestamp(repos["created_at"].max())

    return min_date, max_date


# @st.cache_data
def filter_repos_by_date(repos, min_created_at, max_created_at):
    min_date, max_date = get_date_range(repos)
    if min_created_at and min_created_at > min_date:
        repos = repos[repos["created_at"] >= min_created_at]

    if max_created_at and max_created_at < max_date:
        repos = repos[repos["created_at"] <= max_created_at]

    return repos


# @st.cache_data
def filter_data_by_language(repos, topics, lang_filter):
    repos = repos[repos["language_display_name"].isin(lang_filter)]
    topics = topics[topics["language_display_name"].isin(lang_filter)]

    return repos, topics


def create_corr_matrix(repos):
    columns = [
        "stargazers_count",
        "forks_count",
        "open_issues_count",
        "size",
        "days_since_date_created",
        # "days_between_creation_and_latest_push",
    ]
    df = repos[columns].corr()
    annotation = df.applymap(lambda x: f"{round(x*100)}%")
    fig = px.imshow(
        df, color_continuous_scale="mint", title="How different columns are correlated?"
    )
    fig.update_traces(text=annotation, texttemplate="%{text}")
    fig.update_coloraxes(showscale=False)

    return fig


def create_wordcloud(topics_stats):
    data = topics_stats.set_index("topics")["id"].to_dict()
    del data["__NA__"]

    wordcloud = WordCloud(
        colormap="gnuplot", width=680, height=320, background_color="white", random_state=42
    ).generate_from_frequencies(data, max_font_size=None)

    fig, ax = plt.subplots()
    ax.imshow(wordcloud, interpolation="bilinear")
    ax.axis("off")

    return fig


def main():
    st.set_page_config(page_title="Github Analytics Dashboard", page_icon="🖥️", layout="wide")
    st.title("Github Analytics Dashboard")
    st.info(
        body="""
        #### About this app
        Put some information about limitations and data collection process
        """,
        icon="ℹ️",
    )

    repos, topics = load_data()

    min_created_at, max_created_at = add_date_picker(repos)
    repos = filter_repos_by_date(repos, min_created_at, max_created_at)

    language_list = sorted(repos["language_display_name"].unique())
    lang_filter = add_multiselect(
        f=st.sidebar,
        options=language_list,
        entity="language",
        default=language_list,
        add_reset_button=True,
        reset_button_name="Select All",
    )

    if lang_filter:
        repos, topics = filter_data_by_language(repos, topics, lang_filter)

    add_kpi_block(repos, topics)

    add_basic_report(repos)

    add_license_report(repos)

    topics_stats = topics.groupby(["topics"])["id"].nunique().reset_index()
    add_topics_report(topics, topics_stats, repos)

    add_repos_report(repos, topics, topics_stats)

    add_owners_report(repos)


if __name__ == "__main__":
    main()
