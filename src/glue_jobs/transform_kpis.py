"""Glue PySpark (4.0) — compute per-file KPI *partials* from streams + reference data.

Why partials (not final KPIs)
-----------------------------
Batch files arrive at unpredictable intervals and **multiple files can belong to
the same day**. Daily KPIs must therefore be *aggregated across every file for a
day*, not computed per-file-and-overwritten. To make that possible, this job
emits mergeable partials that the ingest job folds into DynamoDB:

* ``genre_partials/`` -> one row per (genre, day) with this file's
  ``listen_count``, ``total_listening_time_ms`` and the **set of user_ids**
  (``collect_set``) so the ingest can union users for a correct distinct count.
* ``song_partials/``  -> one row per (genre, day, track) with this file's
  ``play_count``.

Ranking (top-3 songs, top-5 genres) and the derived metrics (unique_listeners,
average listening time) are computed by the ingest job *after* merging, so they
always reflect the full day.

The transform is split into pure DataFrame -> DataFrame helpers so unit tests can
exercise the aggregation logic with the SparkSession fixture without touching S3.
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, collect_set, count, lit, to_date
from pyspark.sql.functions import sum as _sum


def _ensure_src_importable() -> None:
    """Make the shared ``src`` package importable under Glue Python Shell.

    Glue Python Shell does not add a ``.zip`` passed via ``--extra-py-files``
    to ``sys.path`` (unlike Glue Spark), so ``import src.utils`` fails there.
    When it does, download the utils archive from S3 and prepend it to
    ``sys.path``. A no-op for Spark jobs, local runs, and tests.
    """
    try:
        import src.utils  # noqa: F401
    except ModuleNotFoundError:  # pragma: no cover - Glue Python Shell only
        import argparse
        import tempfile

        import boto3

        parser = argparse.ArgumentParser()
        parser.add_argument("--bucket")
        bucket = parser.parse_known_args(sys.argv[1:])[0].bucket
        if bucket:
            archive = os.path.join(tempfile.gettempdir(), "utils.zip")
            boto3.client("s3").download_file(bucket, "glue-assets/utils.zip", archive)
            sys.path.insert(0, archive)


_ensure_src_importable()

from src.utils.logger import get_logger, log
from src.utils.schema_registry import (
    SONGS_SELECT_COLUMNS,
    USERS_SELECT_COLUMNS,
)

_JOB_NAME = "transform_kpis"


# ---------------------------------------------------------------------------
# Pure DataFrame transforms — unit-tested independently of S3
# ---------------------------------------------------------------------------
def build_enriched(streams_df: DataFrame, songs_df: DataFrame, users_df: DataFrame) -> DataFrame:
    """Inner-join streams with broadcast songs + users, derive UTC ``listen_date``."""
    from pyspark.sql.functions import broadcast

    return (
        streams_df.join(broadcast(songs_df), on="track_id", how="inner")
        .join(broadcast(users_df), on="user_id", how="inner")
        .withColumn("listen_date", to_date(col("listen_time")))
    )


def compute_genre_partials(enriched_df: DataFrame) -> DataFrame:
    """Per-file genre aggregates that the ingest job merges into the daily KPI.

    Columns: track_genre, listen_date, listen_count, total_listening_time_ms,
    user_ids (array of distinct users in *this file* for the genre+day).

    ``unique_listeners`` and ``avg_listening_time_per_user_ms`` are intentionally
    NOT computed here — they are derived in the ingest after the per-day user
    sets from all files are unioned (distinct counts are not additive).
    """
    return enriched_df.groupBy("track_genre", "listen_date").agg(
        count("*").alias("listen_count"),
        # Null-safe: a group whose matched songs all have a null duration_ms
        # would otherwise sum to NULL, which DynamoDB ingest can't cast to int.
        coalesce(_sum("duration_ms"), lit(0)).alias("total_listening_time_ms"),
        collect_set("user_id").alias("user_ids"),
    )


def compute_song_partials(enriched_df: DataFrame) -> DataFrame:
    """Per-file play counts per (genre, day, track), merged + ranked by the ingest.

    Columns: track_genre, listen_date, track_id, track_name, artists, play_count.
    Top-3 ranking is applied in the ingest against the *cumulative* counts.
    """
    return enriched_df.groupBy(
        "track_genre", "listen_date", "track_id", "track_name", "artists"
    ).agg(count("*").alias("play_count"))


# ---------------------------------------------------------------------------
# S3-wired orchestration
# ---------------------------------------------------------------------------
def _write_parquet(
    df: DataFrame, bucket: str, execution_id: str, dataset_name: str
) -> None:  # pragma: no cover
    """Overwrite s3://bucket/processed/<execution_id>/<dataset>/ as Parquet.

    The path is execution-scoped so concurrent same-day executions never clobber
    each other's handoff. Uses native Spark Parquet output rather than
    awswrangler: Glue 4.0 Spark does not bundle awswrangler, and pip-installing
    it pulls a numpy build that is ABI incompatible with Glue's pandas/pyarrow.
    """
    df.coalesce(1).write.mode("overwrite").parquet(
        f"s3://{bucket}/processed/{execution_id}/{dataset_name}/"
    )


def transform(
    *, bucket: str, s3_key: str, execution_id: str, spark: SparkSession | None = None
) -> None:  # pragma: no cover
    """Read inputs, compute per-file KPI partials, and write them to S3."""
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

    genre_partials = compute_genre_partials(enriched)
    song_partials = compute_song_partials(enriched)

    _write_parquet(genre_partials, bucket, execution_id, "genre_partials")
    log(logger, "info", "Wrote genre_partials Parquet", dataset="genre_partials")

    _write_parquet(song_partials, bucket, execution_id, "song_partials")
    log(logger, "info", "Wrote song_partials Parquet", dataset="song_partials")

    enriched.unpersist()
    log(logger, "info", "Transform complete")


def main() -> None:  # pragma: no cover
    """Glue entrypoint — parse args, propagate execution_id, run :func:`transform`."""
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, ["s3_key", "bucket", "execution_id"])
    os.environ["EXECUTION_ID"] = args["execution_id"]
    transform(bucket=args["bucket"], s3_key=args["s3_key"], execution_id=args["execution_id"])


if __name__ == "__main__":
    main()
