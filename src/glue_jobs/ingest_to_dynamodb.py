"""Glue Python Shell — load KPI Parquet datasets into the single DynamoDB table.

Reads three datasets from ``s3://<bucket>/processed/``:

* ``genre_kpis/``  -> ``pk = GENRE_KPI#<genre>#<date>``, ``sk = METADATA``
* ``top_songs/``   -> ``pk = TOP_SONGS#<genre>#<date>``, ``sk = RANK#<nn>``
* ``top_genres/``  -> ``pk = TOP_GENRES#<date>``,         ``sk = RANK#<nn>``

Items are sanitised (float -> Decimal) and batch-written with
``overwrite_by_pkeys=["pk", "sk"]`` so re-runs replace existing rows in place.
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

from src.utils.dynamodb_helpers import batch_write_items
from src.utils.logger import get_logger, log

_JOB_NAME = "ingest_to_dynamodb"


def _format_date(value: Any) -> str:
    """Return ``YYYY-MM-DD`` from a date / Timestamp / str input."""
    if hasattr(value, "strftime"):
        formatted: str = value.strftime("%Y-%m-%d")
        return formatted
    return str(value)


def build_genre_kpi_item(row: dict[str, Any]) -> dict[str, Any]:
    """Build a single Genre-Level KPI DynamoDB item from a Parquet row."""
    date = _format_date(row["listen_date"])
    return {
        "pk": f"GENRE_KPI#{row['track_genre']}#{date}",
        "sk": "METADATA",
        "genre": row["track_genre"],
        "date": date,
        "listen_count": int(row["listen_count"]),
        "unique_listeners": int(row["unique_listeners"]),
        "total_listening_time_ms": int(row["total_listening_time_ms"]),
        "avg_listening_time_per_user_ms": float(row["avg_listening_time_per_user_ms"]),
    }


def build_top_songs_item(row: dict[str, Any]) -> dict[str, Any]:
    """Build a single Top Songs DynamoDB item (one row per rank slot)."""
    date = _format_date(row["listen_date"])
    return {
        "pk": f"TOP_SONGS#{row['track_genre']}#{date}",
        "sk": f"RANK#{int(row['rank']):02d}",
        "rank": int(row["rank"]),
        "track_id": row["track_id"],
        "track_name": row["track_name"],
        "artists": row["artists"],
        "play_count": int(row["play_count"]),
    }


def build_top_genres_item(row: dict[str, Any]) -> dict[str, Any]:
    """Build a single Top Genres DynamoDB item (one row per rank slot)."""
    date = _format_date(row["listen_date"])
    return {
        "pk": f"TOP_GENRES#{date}",
        "sk": f"RANK#{int(row['rank']):02d}",
        "rank": int(row["rank"]),
        "genre": row["track_genre"],
        "listen_count": int(row["listen_count"]),
    }


def ingest_dataframes(
    *,
    table: Any,
    genre_kpis: Any,
    top_songs: Any,
    top_genres: Any,
) -> None:
    """Build items from the three DataFrames and batch-write them to ``table``."""
    logger = get_logger(_JOB_NAME)

    items: list[dict[str, Any]] = []
    for row in genre_kpis.to_dict(orient="records"):
        items.append(build_genre_kpi_item(row))
    for row in top_songs.to_dict(orient="records"):
        items.append(build_top_songs_item(row))
    for row in top_genres.to_dict(orient="records"):
        items.append(build_top_genres_item(row))

    log(
        logger,
        "info",
        "Built KPI items",
        genre_kpis=len(genre_kpis),
        top_songs=len(top_songs),
        top_genres=len(top_genres),
        total=len(items),
    )

    if not items:
        log(logger, "warning", "Nothing to ingest (all three datasets are empty)")
        return

    batch_write_items(table, items, overwrite_by_pkeys=["pk", "sk"])
    log(logger, "info", "Batch write complete", items_written=len(items))


def ingest(*, bucket: str, table_name: str) -> None:  # pragma: no cover
    """Read three Parquet datasets from S3 and ingest into ``table_name``."""
    # Integration-tested in Phase 8 — unit tests use ingest_dataframes() directly.
    import awswrangler as wr
    import boto3

    logger = get_logger(_JOB_NAME)
    log(logger, "info", "Starting ingest", bucket=bucket, table_name=table_name)

    genre_kpis = wr.s3.read_parquet(path=f"s3://{bucket}/processed/genre_kpis/", dataset=True)
    top_songs = wr.s3.read_parquet(path=f"s3://{bucket}/processed/top_songs/", dataset=True)
    top_genres = wr.s3.read_parquet(path=f"s3://{bucket}/processed/top_genres/", dataset=True)

    table = boto3.resource("dynamodb").Table(table_name)
    ingest_dataframes(
        table=table,
        genre_kpis=genre_kpis,
        top_songs=top_songs,
        top_genres=top_genres,
    )


def main() -> None:  # pragma: no cover
    """Glue entrypoint — parse args, propagate execution_id, run :func:`ingest`."""
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, ["bucket", "table_name", "execution_id"])
    os.environ["EXECUTION_ID"] = args["execution_id"]
    ingest(bucket=args["bucket"], table_name=args["table_name"])


if __name__ == "__main__":
    main()
