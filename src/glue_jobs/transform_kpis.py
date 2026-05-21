"""Glue PySpark (4.0) — compute daily KPIs from streams + reference data.

Pipeline:

1. Read the incoming streams CSV plus the two reference CSVs from S3.
2. Inner-join streams with broadcast(songs) and broadcast(users) — rows whose
   ``track_id`` or ``user_id`` are missing from the reference tables are
   intentionally dropped (per spec §Assumptions).
3. Derive ``listen_date`` in UTC from the ``listen_time`` timestamp.
4. Compute three KPI datasets per calendar day:
     * Genre-Level KPI (one row per genre+day)
     * Top-3 Songs per genre per day (rank 1..3, ties broken by track_id ASC)
     * Top-5 Genres per day (rank 1..5, ties broken by track_genre ASC)
5. Write each dataset as Parquet to ``s3://<bucket>/processed/<dataset>/``
   with ``mode="overwrite"`` so re-runs are idempotent (the whole prefix is
   replaced atomically).

The transform is split into pure DataFrame -> DataFrame helpers so unit
tests can exercise the aggregation logic with the SparkSession fixture
without touching S3 or boto3.
"""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col, count, countDistinct, dense_rank, to_date
from pyspark.sql.functions import round as _round
from pyspark.sql.functions import sum as _sum
from pyspark.sql.window import Window

from src.utils.logger import get_logger, log
from src.utils.schema_registry import (
    SONGS_SELECT_COLUMNS,
    USERS_SELECT_COLUMNS,
)

if TYPE_CHECKING:
    pass

_JOB_NAME = "transform_kpis"
_TOP_SONGS_PER_GENRE = 3
_TOP_GENRES_PER_DAY = 5


# ---------------------------------------------------------------------------
# Pure DataFrame transforms — unit-tested independently of S3
# ---------------------------------------------------------------------------
def build_enriched(streams_df: DataFrame, songs_df: DataFrame, users_df: DataFrame) -> DataFrame:
    """Inner-join streams with broadcast songs + users, derive UTC ``listen_date``."""
    return (
        streams_df.join(broadcast(songs_df), on="track_id", how="inner")
        .join(broadcast(users_df), on="user_id", how="inner")
        .withColumn("listen_date", to_date(col("listen_time")))
    )


def compute_genre_kpis(enriched_df: DataFrame) -> DataFrame:
    """Aggregate genre-level KPIs per calendar day.

    Columns: track_genre, listen_date, listen_count, unique_listeners,
    total_listening_time_ms, avg_listening_time_per_user_ms (rounded to 2 dp).
    """
    return (
        enriched_df.groupBy("track_genre", "listen_date")
        .agg(
            count("*").alias("listen_count"),
            countDistinct("user_id").alias("unique_listeners"),
            _sum("duration_ms").alias("total_listening_time_ms"),
        )
        .withColumn(
            "avg_listening_time_per_user_ms",
            _round(
                col("total_listening_time_ms") / col("unique_listeners"),
                2,
            ),
        )
    )


def rank_top_songs(enriched_df: DataFrame, top_n: int = _TOP_SONGS_PER_GENRE) -> DataFrame:
    """Top-N songs per genre per day by play_count (tie-break track_id ASC)."""
    play_counts = enriched_df.groupBy(
        "track_genre", "listen_date", "track_id", "track_name", "artists"
    ).agg(count("*").alias("play_count"))

    window = Window.partitionBy("track_genre", "listen_date").orderBy(
        col("play_count").desc(), col("track_id").asc()
    )
    return (
        play_counts.withColumn("rank", dense_rank().over(window))
        .filter(col("rank") <= top_n)
        .select(
            "track_genre",
            "listen_date",
            "rank",
            "track_id",
            "track_name",
            "artists",
            "play_count",
        )
    )


def rank_top_genres(genre_kpi_df: DataFrame, top_n: int = _TOP_GENRES_PER_DAY) -> DataFrame:
    """Top-N genres per day by listen_count (tie-break track_genre ASC)."""
    window = Window.partitionBy("listen_date").orderBy(
        col("listen_count").desc(), col("track_genre").asc()
    )
    return (
        genre_kpi_df.select("track_genre", "listen_date", "listen_count")
        .withColumn("rank", dense_rank().over(window))
        .filter(col("rank") <= top_n)
        .select("listen_date", "rank", "track_genre", "listen_count")
    )


# ---------------------------------------------------------------------------
# S3-wired orchestration
# ---------------------------------------------------------------------------
def _write_parquet(df: DataFrame, bucket: str, dataset_name: str) -> None:  # pragma: no cover
    """Coalesce, convert to pandas, write to s3://bucket/processed/<dataset>/ as Parquet."""
    # Covered by integration tests (Phase 8) — unit-testing this requires
    # mocking awswrangler's S3 client, which adds noise without value.
    import awswrangler as wr  # local import — only required at runtime, not at unit-test time

    pandas_df = df.coalesce(1).toPandas()
    wr.s3.to_parquet(
        df=pandas_df,
        path=f"s3://{bucket}/processed/{dataset_name}/",
        dataset=True,
        mode="overwrite",
    )


def transform(
    *, bucket: str, s3_key: str, spark: SparkSession | None = None
) -> None:  # pragma: no cover
    """Read inputs, compute KPIs, and write three Parquet datasets to S3."""
    logger = get_logger(_JOB_NAME)
    log(logger, "info", "Starting transform", bucket=bucket, s3_key=s3_key)

    session = spark if spark is not None else SparkSession.builder.getOrCreate()
    session.conf.set("spark.sql.session.timeZone", "UTC")

    streams_df = session.read.csv(f"s3://{bucket}/{s3_key}", header=True, inferSchema=True)
    songs_df = session.read.csv(
        f"s3://{bucket}/reference/songs/songs.csv", header=True, inferSchema=True
    ).select(*SONGS_SELECT_COLUMNS)
    users_df = session.read.csv(
        f"s3://{bucket}/reference/users/users.csv", header=True, inferSchema=True
    ).select(*USERS_SELECT_COLUMNS)

    enriched = build_enriched(streams_df, songs_df, users_df).cache()
    enriched_count = enriched.count()
    log(logger, "info", "Enriched dataframe materialised", row_count=enriched_count)

    genre_kpis = compute_genre_kpis(enriched)
    top_songs = rank_top_songs(enriched, top_n=_TOP_SONGS_PER_GENRE)
    top_genres = rank_top_genres(genre_kpis, top_n=_TOP_GENRES_PER_DAY)

    _write_parquet(genre_kpis, bucket, "genre_kpis")
    log(logger, "info", "Wrote genre_kpis Parquet", dataset="genre_kpis")

    _write_parquet(top_songs, bucket, "top_songs")
    log(logger, "info", "Wrote top_songs Parquet", dataset="top_songs")

    _write_parquet(top_genres, bucket, "top_genres")
    log(logger, "info", "Wrote top_genres Parquet", dataset="top_genres")

    enriched.unpersist()
    log(logger, "info", "Transform complete")


def main() -> None:  # pragma: no cover
    """Glue entrypoint — parse args, propagate execution_id, run :func:`transform`."""
    # awsglue.utils is only available inside the Glue runtime, so this function
    # is exercised by deploys + integration tests, not unit tests.
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, ["s3_key", "bucket", "execution_id"])
    os.environ["EXECUTION_ID"] = args["execution_id"]
    transform(bucket=args["bucket"], s3_key=args["s3_key"])


if __name__ == "__main__":
    main()
