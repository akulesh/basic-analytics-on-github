import streamlit as st

from src.dashboard.analytics import DataAnalytics
from src.dashboard.utils import (
    add_multiselect,
    clear_cache,
    plot_bar_chart,
    plot_corr_matrix_heatmap,
    plot_pie_chart,
    plot_wordcloud,
)


def add_kpi_block(kpi_report):
    cols = st.columns(4)
    cols[0].metric("Number of Repositories", int(kpi_report.get("n_repos") or 0))
    cols[1].metric("Number of Owners", int(kpi_report.get("n_owners") or 0))
    cols[2].metric("Number of Languages", int(kpi_report.get("language") or 0))
    cols[3].metric("Number of Topics", int(kpi_report.get("topics") or 0))

    cols[0].metric("Avg. Number of Stars", round(kpi_report.get("stars") or 0, 1))
    cols[1].metric("Avg. Number of Watchers", "--")
    cols[2].metric("Avg. Number of Forks", round(kpi_report.get("forks") or 0, 1))
    cols[3].metric("Avg. Number of Open Issues", round(kpi_report.get("open_issues") or 0, 1))

    st.markdown("---")


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


def plot_repos_by_license(data, top_n: int = 5):
    df = DataAnalytics.get_categorical_data(data, group="license", metric="n_repos", top_n=top_n)
    plot_pie_chart(
        df,
        group="license",
        metric="n_repos",
        showlegend=True,
        textinfo="percent",
        title=f"Top-{top_n} Most Common Licenses",
        labels={"license": "License", "n_repos": "Number of Repositories"},
    )


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


def plot_topics_freq(data, threshold=10):
    df = DataAnalytics.get_topic_report(data, threshold=threshold)
    plot_bar_chart(
        df,
        group="freq",
        metric="count",
        title="How many times a topic is mentioned?",
        labels={"freq": "Topic frequency", "count": "Number of topics"},
    )


def plot_topic_cloud(data, top_n=100):
    data = data[:top_n]
    data = dict(zip(data["topic"], data["n_repos"]))
    data.pop("__NA__", None)
    plot_wordcloud(data, title="The most popular topics")


def add_main_block(repos_df, topics_df):
    # sourcery skip: extract-method
    cols = st.columns(2)
    with cols[0]:
        plot_repos_by_creation_year(repos_df)
        plot_repos_by_language(repos_df)
        plot_has_license(repos_df)
        plot_is_archived(repos_df)
        # plot_repos_by_branch(repos_df)
        plot_has_topic(repos_df)

    with cols[1]:
        plot_repos_by_last_commit_year(repos_df)
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
        plot_repos_by_license(repos_df, top_n=5)
        plot_wiki(repos_df)
        if not topics_df.empty:
            plot_topic_cloud(topics_df)


def add_intro_block(filters_df):
    st.title("📊 Github Analytics Dashboard")
    st.info(body="""
        #### ℹ️ Notes
        - Supported languages: TODO
        - Only repos with at least one star were indexed
        - Filtering by required packages is available only for Python
        - Min repo creation date: 2012-01-01
        - Update frequency: TODO
        """)

    if filters_df.empty:
        st.markdown(
            "⛔ Data is not prepared! Please load the data into the DB and press `Clear"
            " Cache` button."
        )
        clear_cache()
        return


def display_repo_table(data, limit=None):
    if limit:
        data = data[:limit]

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
            "topics": st.column_config.ListColumn("Topics", width="medium"),
            "description": st.column_config.TextColumn("Repository Description", width="medium"),
            "url": st.column_config.LinkColumn("Link", width="medium"),
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


def add_repo_explorer(
    da: DataAnalytics,
    creation_date_range=None,
    last_commit_date_range=None,
    topics: list = None,
    licenses: list = None,
    languages: list = None,
    packages: list = None,
):
    st.markdown("---")
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

    menu = st.columns([2, 2, 2, 1, 1, 1])

    col = menu[0]
    with col:
        params["topics"] = add_multiselect(
            col,
            topics,
            entity="topic",
            title="Select Topics",
            exclude_empty=params["exclude_without_topic"],
        )

    col = menu[1]
    with col:
        params["licenses"] = add_multiselect(
            col,
            licenses,
            entity="license",
            title="Select Licenses",
            exclude_empty=params["exclude_without_license"],
        )

    if packages:
        col = menu[2]
        with col:
            params["packages"] = add_multiselect(
                col, packages, title="Select Packages (Only Python)", entity="package"
            )

    with menu[4]:
        params["sort_by"] = st.selectbox(
            "Order By",
            options=["stars", "forks", "open_issues", "creation_date", "last_commit_date"],
        )

    with menu[5]:
        limit = st.number_input("Limit", min_value=1, max_value=None, value=100, step=50)

    data = da.get_repo_report(
        creation_date_range=creation_date_range,
        last_commit_date_range=last_commit_date_range,
        languages=languages,
        **params,
    )

    display_repo_table(data, limit=limit)


def add_package_explorer():
    st.markdown("---")
    st.header("🐍 Python Package Exploration")
    st.write("TODO")
