"""Streamlit dashboard — browse daily music streaming KPIs from DynamoDB."""

from __future__ import annotations

import os
from datetime import date

import altair as alt
import pandas as pd
import streamlit as st
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    LoginRefreshRequired,
    MissingDependencyException,
)

from src.utils.kpi_queries import get_genre_kpi, get_table, get_top_genres, get_top_songs

DEFAULT_TABLE = "music-streaming-kpis"
DEFAULT_REGION = "eu-central-1"


def _default_query_date() -> date:
    override = os.environ.get("DEFAULT_LISTEN_DATE")
    if override:
        return date.fromisoformat(override)
    return date(2024, 6, 25)  # streams1.csv sample data


def _ms_to_minutes(ms: int | float) -> float:
    return round(float(ms) / 60_000, 1)


def _bar_chart(
    df: pd.DataFrame,
    *,
    category_col: str,
    value_col: str,
    category_title: str,
    value_title: str,
    height: int = 320,
) -> alt.Chart:
    """Bar chart with horizontal category labels (Streamlit defaults rotate to 90°)."""
    return (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(
                f"{category_col}:N",
                sort="-y",
                axis=alt.Axis(labelAngle=0, title=category_title),
            ),
            y=alt.Y(f"{value_col}:Q", title=value_title),
        )
        .properties(height=height)
    )


st.set_page_config(
    page_title="Music Streaming KPIs",
    page_icon="🎵",
    layout="wide",
)

st.title("Music Streaming KPI Dashboard")
st.caption("Served KPIs written by the ingest Glue job into DynamoDB.")

with st.sidebar:
    st.header("Connection")
    table_name = st.text_input("DynamoDB table", value=os.environ.get("TABLE_NAME", DEFAULT_TABLE))
    region = st.text_input("AWS region", value=os.environ.get("AWS_REGION", DEFAULT_REGION))
    query_date = st.date_input("Listen date (UTC)", value=_default_query_date())
    st.caption("Sample `streams1.csv` listen date: **2024-06-25**")
    st.divider()
    st.markdown(
        "Uses your local AWS credentials (`AWS_PROFILE`, default chain). "
        "Run after a successful pipeline execution for the selected date."
    )

date_str = query_date.isoformat()

try:
    table = get_table(table_name=table_name, region=region)
    top_genres = get_top_genres(table, date=date_str)
except MissingDependencyException:
    st.error(
        "AWS login/SSO profiles need the CRT extra. Run `uv sync` "
        "(installs `awscrt`) and restart the dashboard."
    )
    st.stop()
except LoginRefreshRequired:
    st.error("AWS session expired. Re-authenticate, then refresh this page:")
    st.code("aws login", language="bash")
    st.stop()
except (ClientError, BotoCoreError) as exc:
    st.error(f"Could not read DynamoDB: {exc}")
    st.stop()

if not top_genres:
    st.info(
        f"No KPI data for {date_str}. Upload a streams file for that day "
        "and wait for the pipeline to finish."
    )
    st.stop()

genres_df = pd.DataFrame(top_genres)

st.subheader(f"Top genres on {date_str}")
st.altair_chart(
    _bar_chart(
        genres_df,
        category_col="genre",
        value_col="listen_count",
        category_title="Genre",
        value_title="Listen count",
        height=320,
    ),
    width="stretch",
)
st.dataframe(genres_df, width="stretch", hide_index=True)

genre_options = [row["genre"] for row in top_genres]
selected_genre = st.selectbox("Genre detail", options=genre_options, index=0)

st.subheader(f"{selected_genre} — daily KPI")
genre_kpi = get_genre_kpi(table, genre=selected_genre, date=date_str)
if genre_kpi is None:
    st.warning(f"No genre KPI row for {selected_genre!r} on {date_str}.")
else:
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Listen events", f"{genre_kpi['listen_count']:,}")
    m2.metric("Unique listeners", f"{genre_kpi['unique_listeners']:,}")
    m3.metric(
        "Total listening (min)",
        f"{_ms_to_minutes(genre_kpi['total_listening_time_ms']):,.1f}",
    )
    m4.metric(
        "Avg per listener (min)",
        f"{_ms_to_minutes(genre_kpi['avg_listening_time_per_user_ms']):,.1f}",
    )

st.subheader(f"Top songs in {selected_genre}")
top_songs = get_top_songs(table, genre=selected_genre, date=date_str)
if not top_songs:
    st.warning(f"No top-songs rows for {selected_genre!r} on {date_str}.")
else:
    songs_df = pd.DataFrame(top_songs)
    chart_col, table_col = st.columns([1, 1])
    with chart_col:
        st.altair_chart(
            _bar_chart(
                songs_df,
                category_col="track_name",
                value_col="play_count",
                category_title="Track",
                value_title="Plays",
                height=280,
            ),
            width="stretch",
        )
    with table_col:
        st.dataframe(
            songs_df[["rank", "track_name", "artists", "play_count"]],
            width="stretch",
            hide_index=True,
        )
