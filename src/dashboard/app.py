"""Music streaming KPI dashboard — analyst-facing view of the pipeline output."""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from src.dashboard.dynamo_client import get_genre_kpis, get_top_genres, get_top_songs

st.set_page_config(
    page_title="Music Streaming KPIs",
    layout="wide",
)

st.title("Music Streaming KPI Dashboard")


@st.cache_data(ttl=300)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load all KPI data from DynamoDB, cached for 5 minutes."""
    return get_genre_kpis(), get_top_songs(), get_top_genres()


genre_df, songs_df, top_genres_df = load_data()

if genre_df.empty:
    st.warning("No GENRE_KPI data found in DynamoDB. Check your AWS credentials.")
    st.stop()

processing_date = genre_df["date"].iloc[0]
st.caption(f"Processing date: **{processing_date}** | Source: `prod-music-streaming-kpis`")

# ── Metric cards ──────────────────────────────────────────────────────────────
total_streams = int(genre_df["listen_count"].sum())
total_unique = int(genre_df["unique_listeners"].sum())
total_hrs = genre_df["total_listening_time_ms"].sum() / (1_000 * 60 * 60)
num_genres = genre_df["genre"].nunique()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Streams", f"{total_streams:,}")
c2.metric("Unique Listeners", f"{total_unique:,}")
c3.metric("Listening Time", f"{total_hrs:.1f} hrs")
c4.metric("Genres Active", str(num_genres))

st.divider()

# ── Genre KPI charts ──────────────────────────────────────────────────────────
st.header("Genre KPIs")

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Streams & Unique Listeners by Genre")
    melted = genre_df.melt(
        id_vars="genre",
        value_vars=["listen_count", "unique_listeners"],
        var_name="Metric",
        value_name="Count",
    )
    melted["Metric"] = melted["Metric"].str.replace("_", " ").str.title()
    chart = (
        alt.Chart(melted)
        .mark_bar()
        .encode(
            x=alt.X("genre:N", title="Genre", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("Count:Q", title="Count"),
            color=alt.Color("Metric:N", legend=alt.Legend(orient="top")),
            xOffset="Metric:N",
            tooltip=["genre:N", "Metric:N", "Count:Q"],
        )
        .properties(height=320)
    )
    st.altair_chart(chart, use_container_width=True)

with col_right:
    st.subheader("Avg Listening Time per User")
    avg_df = genre_df.copy()
    avg_df["minutes"] = (avg_df["avg_listening_time_ms"] / 60_000).round(1)
    bar = (
        alt.Chart(avg_df)
        .mark_bar(color="#e45756")
        .encode(
            x=alt.X("minutes:Q", title="Minutes"),
            y=alt.Y("genre:N", sort="-x", title="Genre"),
            tooltip=["genre:N", alt.Tooltip("minutes:Q", title="Avg minutes")],
        )
        .properties(height=320)
    )
    st.altair_chart(bar, use_container_width=True)

st.divider()

# ── Top genres leaderboard ────────────────────────────────────────────────────
if not top_genres_df.empty:
    st.header("Top Genres Leaderboard")
    st.dataframe(
        top_genres_df[["rank", "genre", "listen_count"]],
        column_config={
            "rank": st.column_config.NumberColumn("#", width="small"),
            "genre": st.column_config.TextColumn("Genre"),
            "listen_count": st.column_config.NumberColumn("Streams"),
        },
        hide_index=True,
        use_container_width=True,
    )
    st.divider()

# ── Top songs per genre ───────────────────────────────────────────────────────
st.header("Top Songs by Genre")

if songs_df.empty:
    st.info("No TOP_SONGS data available.")
else:
    genres = sorted(songs_df["genre"].unique().tolist())
    selected = st.selectbox("Select a genre", genres)
    subset = (
        songs_df[songs_df["genre"] == selected][
            ["rank", "track_name", "artists", "play_count"]
        ]
        .reset_index(drop=True)
    )
    st.dataframe(
        subset,
        column_config={
            "rank": st.column_config.NumberColumn("#", width="small"),
            "track_name": st.column_config.TextColumn("Track", width="large"),
            "artists": st.column_config.TextColumn("Artists"),
            "play_count": st.column_config.NumberColumn("Plays", width="small"),
        },
        hide_index=True,
        use_container_width=True,
    )
