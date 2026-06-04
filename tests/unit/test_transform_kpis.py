"""Unit tests for the PySpark transform job.

Exercises the pure functions (``build_enriched``, ``compute_genre_partials``,
``compute_song_partials``) directly with the session-scoped SparkSession fixture.
The S3-wired ``transform()`` and the Glue entrypoint ``main()`` are exercised by
the integration tests. Ranking and derived metrics now live in the ingest job
(post-merge), so they are tested there.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

import pytest

from src.glue_jobs.transform_kpis import (
    build_enriched,
    compute_genre_partials,
    compute_song_partials,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# build_enriched
# ---------------------------------------------------------------------------
def test_build_enriched_inner_join_drops_unmatched_user_id(spark: SparkSession) -> None:
    streams = spark.createDataFrame(
        [
            ("u1", "t1", datetime(2026, 5, 18, 10, 0, 0)),
            ("u_unknown", "t1", datetime(2026, 5, 18, 11, 0, 0)),
        ],
        schema=["user_id", "track_id", "listen_time"],
    )
    songs = spark.createDataFrame(
        [("t1", "Song", "Artist", "pop", 180_000)],
        schema=["track_id", "track_name", "artists", "track_genre", "duration_ms"],
    )
    users = spark.createDataFrame([("u1",)], schema=["user_id"])
    enriched = build_enriched(streams, songs, users)
    assert enriched.count() == 1
    assert enriched.collect()[0]["user_id"] == "u1"


def test_build_enriched_inner_join_drops_unmatched_track_id(spark: SparkSession) -> None:
    streams = spark.createDataFrame(
        [
            ("u1", "t1", datetime(2026, 5, 18, 10, 0, 0)),
            ("u1", "t_unknown", datetime(2026, 5, 18, 11, 0, 0)),
        ],
        schema=["user_id", "track_id", "listen_time"],
    )
    songs = spark.createDataFrame(
        [("t1", "Song", "Artist", "pop", 180_000)],
        schema=["track_id", "track_name", "artists", "track_genre", "duration_ms"],
    )
    users = spark.createDataFrame([("u1",)], schema=["user_id"])
    enriched = build_enriched(streams, songs, users)
    assert enriched.count() == 1


def test_build_enriched_derives_listen_date_in_utc(
    spark: SparkSession, make_streams_df, make_songs_df, make_users_df
) -> None:
    enriched = build_enriched(make_streams_df(), make_songs_df(), make_users_df())
    rows = enriched.collect()
    assert "listen_date" in enriched.columns
    dates = {row["listen_date"] for row in rows}
    assert date(2026, 5, 18) in dates
    assert date(2026, 5, 19) in dates


# ---------------------------------------------------------------------------
# compute_genre_partials
# ---------------------------------------------------------------------------
def test_compute_genre_partials_aggregates_per_genre_per_day(
    spark: SparkSession, make_streams_df, make_songs_df, make_users_df
) -> None:
    enriched = build_enriched(make_streams_df(), make_songs_df(), make_users_df())
    rows = {
        (r["track_genre"], r["listen_date"]): r for r in compute_genre_partials(enriched).collect()
    }

    # pop / 2026-05-18: t1(u1), t1(u2), t2(u1) -> 3 plays, users {u1, u2}
    pop_d1 = rows[("pop", date(2026, 5, 18))]
    assert pop_d1["listen_count"] == 3
    # total_listening_time_ms = SUM(duration_ms): 180k + 180k + 200k
    assert pop_d1["total_listening_time_ms"] == 180_000 + 180_000 + 200_000
    assert set(pop_d1["user_ids"]) == {"u1", "u2"}


def test_compute_genre_partials_uses_duration_not_listen_time(
    spark: SparkSession, make_streams_df, make_songs_df, make_users_df
) -> None:
    enriched = build_enriched(make_streams_df(), make_songs_df(), make_users_df())
    for row in compute_genre_partials(enriched).collect():
        # Summing listen_time epochs would be ~1.7e18; real durations are small.
        assert row["total_listening_time_ms"] < 10_000_000


def test_compute_genre_partials_output_columns_match_schema(
    spark: SparkSession, make_streams_df, make_songs_df, make_users_df
) -> None:
    enriched = build_enriched(make_streams_df(), make_songs_df(), make_users_df())
    partials = compute_genre_partials(enriched)
    assert set(partials.columns) == {
        "track_genre",
        "listen_date",
        "listen_count",
        "total_listening_time_ms",
        "user_ids",
    }


def test_compute_genre_partials_total_is_null_safe(spark: SparkSession) -> None:
    # A matched song with a NULL duration must not produce a NULL sum.
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    streams = spark.createDataFrame(
        [("u1", "t1", datetime(2026, 5, 18, 10, 0, 0))],
        schema=["user_id", "track_id", "listen_time"],
    )
    # Explicit schema: an all-NULL duration_ms column cannot be type-inferred.
    songs = spark.createDataFrame(
        [("t1", "S", "A", "pop", None)],
        schema=StructType(
            [
                StructField("track_id", StringType()),
                StructField("track_name", StringType()),
                StructField("artists", StringType()),
                StructField("track_genre", StringType()),
                StructField("duration_ms", IntegerType()),
            ]
        ),
    )
    users = spark.createDataFrame([("u1",)], schema=["user_id"])
    enriched = build_enriched(streams, songs, users)
    row = compute_genre_partials(enriched).collect()[0]
    assert row["total_listening_time_ms"] == 0


# ---------------------------------------------------------------------------
# compute_song_partials
# ---------------------------------------------------------------------------
def test_compute_song_partials_counts_plays_per_track(
    spark: SparkSession, make_streams_df, make_songs_df, make_users_df
) -> None:
    enriched = build_enriched(make_streams_df(), make_songs_df(), make_users_df())
    rows = {
        (r["track_genre"], r["listen_date"], r["track_id"]): r
        for r in compute_song_partials(enriched).collect()
    }
    # pop / 2026-05-18: t1 played by u1 and u2 -> play_count 2
    assert rows[("pop", date(2026, 5, 18), "t1")]["play_count"] == 2
    assert rows[("pop", date(2026, 5, 18), "t2")]["play_count"] == 1


def test_compute_song_partials_output_columns_match_schema(
    spark: SparkSession, make_streams_df, make_songs_df, make_users_df
) -> None:
    enriched = build_enriched(make_streams_df(), make_songs_df(), make_users_df())
    partials = compute_song_partials(enriched)
    assert set(partials.columns) == {
        "track_genre",
        "listen_date",
        "track_id",
        "track_name",
        "artists",
        "play_count",
    }


_ = pytest  # keep import live for the spark fixture
