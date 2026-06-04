"""Glue Python Shell — merge per-file KPI partials into the daily DynamoDB KPIs.

Reads two partial datasets written by the transform under an execution-scoped
prefix ``s3://<bucket>/processed/<execution_id>/`` (execution-scoped so that
concurrent same-day files never overwrite each other's handoff):

* ``genre_partials/`` -> per (genre, day): listen_count, total_listening_time_ms, user_ids[]
* ``song_partials/``  -> per (genre, day, track): play_count

Each execution records its contribution as overwrite-keyed partial items, then
the served daily KPIs are recomputed from *all* partials for the affected days.
This is idempotent (re-running an execution overwrites identical partials) and
correct across multiple files per day (sum of counters, union of user sets).
"""

from __future__ import annotations

import os
import sys
from typing import Any


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

from src.utils.dynamodb_helpers import (
    put_genre_partial,
    put_song_partials,
    recompute_genre_kpi,
    recompute_top_genres,
    recompute_top_songs,
)
from src.utils.logger import get_logger, log

_JOB_NAME = "ingest_to_dynamodb"


def _format_date(value: Any) -> str:
    """Return ``YYYY-MM-DD`` from a date / Timestamp / str input."""
    if hasattr(value, "strftime"):
        formatted: str = value.strftime("%Y-%m-%d")
        return formatted
    return str(value)


def _to_str_set(value: Any) -> set[str]:
    """Coerce a parquet array cell (list / numpy array / scalar) to a set of str."""
    if value is None:
        return set()
    if isinstance(value, str | bytes):
        return {value.decode() if isinstance(value, bytes) else value}
    try:
        return {str(item) for item in value}
    except TypeError:
        return {str(value)}


def merge_partials(
    table: Any, genre_partials: Any, song_partials: Any, *, execution_id: str
) -> None:
    """Write this execution's partials, then recompute affected daily KPIs.

    ``genre_partials`` / ``song_partials`` are pandas DataFrames (parquet rows).
    """
    logger = get_logger(_JOB_NAME)

    affected_dates: set[str] = set()
    affected_genre_dates: set[tuple[str, str]] = set()

    genre_rows = genre_partials.to_dict(orient="records")
    for row in genre_rows:
        genre = str(row["track_genre"])
        date = _format_date(row["listen_date"])
        put_genre_partial(
            table,
            genre=genre,
            date=date,
            execution_id=execution_id,
            listen_count=int(row["listen_count"]),
            total_listening_time_ms=int(row["total_listening_time_ms"]),
            user_ids=_to_str_set(row["user_ids"]),
        )
        affected_genre_dates.add((genre, date))
        affected_dates.add(date)

    song_rows = song_partials.to_dict(orient="records")
    song_partial_items: list[dict[str, Any]] = []
    for row in song_rows:
        genre = str(row["track_genre"])
        date = _format_date(row["listen_date"])
        song_partial_items.append(
            {
                "genre": genre,
                "date": date,
                "execution_id": execution_id,
                "track_id": str(row["track_id"]),
                "track_name": str(row["track_name"]),
                "artists": str(row["artists"]),
                "play_count": int(row["play_count"]),
            }
        )
        affected_genre_dates.add((genre, date))
        affected_dates.add(date)
    put_song_partials(table, song_partial_items)

    for genre, date in affected_genre_dates:
        recompute_genre_kpi(table, genre=genre, date=date)
        recompute_top_songs(table, genre=genre, date=date)
    for date in affected_dates:
        recompute_top_genres(table, date=date)

    log(
        logger,
        "info",
        "Merge complete",
        execution_id=execution_id,
        genre_partials=len(genre_rows),
        song_partials=len(song_rows),
        days=len(affected_dates),
        genre_days=len(affected_genre_dates),
    )


def ingest(*, bucket: str, table_name: str, execution_id: str) -> None:  # pragma: no cover
    """Read the execution's partial Parquet datasets from S3 and merge them in."""
    import awswrangler as wr
    import boto3

    logger = get_logger(_JOB_NAME)
    log(logger, "info", "Starting ingest", bucket=bucket, table_name=table_name)

    base = f"s3://{bucket}/processed/{execution_id}"
    genre_partials = wr.s3.read_parquet(path=f"{base}/genre_partials/", dataset=True)
    song_partials = wr.s3.read_parquet(path=f"{base}/song_partials/", dataset=True)

    table = boto3.resource("dynamodb").Table(table_name)
    merge_partials(table, genre_partials, song_partials, execution_id=execution_id)


def main() -> None:  # pragma: no cover
    """Glue entrypoint — parse args, propagate execution_id, run :func:`ingest`."""
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, ["bucket", "table_name", "execution_id"])
    os.environ["EXECUTION_ID"] = args["execution_id"]
    ingest(bucket=args["bucket"], table_name=args["table_name"], execution_id=args["execution_id"])


if __name__ == "__main__":
    main()
