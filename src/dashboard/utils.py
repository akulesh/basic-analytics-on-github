import calendar
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import streamlit as st
from wordcloud import WordCloud


def add_date_picker(min_date_year, max_date_year, key_prefix="creation_date"):
    st.sidebar.header(f"📆 Repository `{key_prefix}`")

    def clear():
        st.session_state[f"{key_prefix}_start_year"] = min_date_year
        st.session_state[f"{key_prefix}_end_year"] = max_date_year
        st.session_state[f"{key_prefix}_start_month"] = 1
        st.session_state[f"{key_prefix}_end_month"] = 12

    # Initialize the date range in session state
    if (f"{key_prefix}_start_year" not in st.session_state) or (
        f"{key_prefix}_end_year" not in st.session_state
    ):
        clear()

    cols = st.sidebar.columns(2)
    with cols[0]:
        st.selectbox(
            "Start Year", range(min_date_year, max_date_year + 1), key=f"{key_prefix}_start_year"
        )
        st.selectbox(
            "End Year", range(min_date_year, max_date_year + 1), key=f"{key_prefix}_end_year"
        )

    with cols[1]:
        st.selectbox("Start Month", range(1, 13), key=f"{key_prefix}_start_month")
        st.selectbox("End Month", range(1, 13), key=f"{key_prefix}_end_month")

    # Add a reset button
    st.sidebar.button("Select All", on_click=clear, key=f"{key_prefix}_date_reset")

    (start_year, start_month), (end_year, end_month) = (
        int(st.session_state[f"{key_prefix}_start_year"]),
        int(st.session_state[f"{key_prefix}_start_month"]),
    ), (
        int(st.session_state[f"{key_prefix}_end_year"]),
        int(st.session_state[f"{key_prefix}_end_month"]),
    )

    start_date = datetime(year=start_year, month=start_month, day=1)
    last_day = calendar.monthrange(end_year, end_month)[1]
    end_date = datetime(year=end_year, month=end_month, day=last_day)

    if start_date > end_date:
        raise ValueError("'start_date' must be less then the 'end_date'")

    return start_date, end_date


def add_multiselect(
    f,
    options: list,
    entity: str = "topic",
    default=None,
    title: str = None,
    add_reset_button: bool = False,
    reset_button_name="Reset",
    exclude_empty: bool = False,
    **kwargs,
):
    options = options.copy()
    if options and exclude_empty and "__NA__" in options:
        options.remove("__NA__")

    values = (
        f.multiselect(
            title or f"Select the {entity.capitalize()}",
            options,
            default=default,
            key=f"{entity}_multiselect",
            **kwargs,
        )
        or []
    )

    def clear_multi():
        st.session_state[f"{entity}_multiselect"] = default or []

    if add_reset_button:
        f.button(reset_button_name, on_click=clear_multi, key=f"{entity}_reset")

    return values


def plot_corr_matrix_heatmap(data: pd.DataFrame, columns=None, title=None):
    df = (data[columns].corr() * 100).round(1)
    annotation = df.applymap(lambda x: f"{x}%")
    fig = px.imshow(df, color_continuous_scale="mint")
    fig.update_traces(text=annotation, texttemplate="%{text}")
    fig.update_coloraxes(showscale=False)

    if title:
        st.markdown(f"##### {title}")
    st.plotly_chart(fig, use_container_width=True)


def plot_bar_chart(
    df,
    group,
    metric,
    labels=None,
    title=None,
    update_xaxes=True,
    marker_color="steelblue",
    **kwargs,
):
    fig = px.bar(df, x=group, y=metric, text=metric, labels=labels, **kwargs)
    fig.update_traces(marker_color=marker_color)
    if update_xaxes:
        fig.update_xaxes(tickmode="auto", tickvals=list(df[group]), type="category")

    if title:
        st.markdown(f"##### {title}")
    st.plotly_chart(fig, use_container_width=True)


def plot_pie_chart(
    df, group, metric, labels=None, title=None, showlegend=False, textinfo="percent+label", **kwargs
):
    fig = px.pie(df, values=metric, names=group, color=group, labels=labels, **kwargs)
    fig.update_traces(textinfo=textinfo, showlegend=showlegend)

    if title:
        st.markdown(f"##### {title}")
    st.plotly_chart(fig, use_container_width=True)


def plot_wordcloud(word_freq: dict, title=None, **kwargs):
    wordcloud = WordCloud(
        colormap="gnuplot",
        width=680,
        height=320,
        background_color="white",
        random_state=42,
        **kwargs,
    ).generate_from_frequencies(word_freq, max_font_size=None)

    fig, ax = plt.subplots()
    ax.imshow(wordcloud, interpolation="bilinear")
    ax.axis("off")

    if title:
        st.markdown(f"##### {title}")
        st.markdown("#")
    st.pyplot(fig, use_container_width=True)


@st.cache_data
def get_last_updated_date():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def clear_cache():
    cols = st.columns(3)

    st.markdown("---")

    with cols[0]:
        updated_at = get_last_updated_date()
        st.markdown(f"*Last cache update*: `{updated_at}`")

    with cols[0]:
        if st.button("Clear Cache"):
            st.cache_resource.clear()
            st.cache_data.clear()
