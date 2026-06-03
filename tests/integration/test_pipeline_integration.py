"""End-to-end-ish integration tests for each Glue job + a multi-file scenario.

Each test wires the job's pure logic against moto-backed AWS services (and the
session-scoped SparkSession from conftest.py for the transform). The real
``main()`` Glue entrypoints aren't exercised here — they read awsglue.utils
which only exists inside the Glue runtime; those paths are covered by CI
deploy + smoke runs.
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from datetime import date, datetime
from typing import TYPE_CHECKING

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from src.glue_jobs.archive_files import archive
from src.glue_jobs.ingest_to_dynamodb import ingest_dataframes
from src.glue_jobs.transform_kpis import (
    build_enriched,
    compute_genre_kpis,
    rank_top_genres,
    rank_top_songs,
)
from src.glue_jobs.validate_schema import validate

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_BUCKET = "integration-music-streaming"
_TABLE = "integration-kpi-table"
_VALID_CSV = (
    b"user_id,track_id,listen_time\n" b"u1,t1,2026-05-18T10:00:00\n" b"u2,t2,2026-05-18T10:05:00\n"
)
_INVALID_CSV = b"user_id,listen_time\nu1,2026-05-18T10:00:00\n"  # missing track_id


@pytest.fixture
def s3_bucket(aws_credentials: None) -> Generator[boto3.client, None, None]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=_BUCKET)
        yield client


@pytest.fixture
def kpi_table(aws_credentials: None) -> Generator[boto3.resource, None, None]:
    with mock_aws():
        resource = boto3.resource("dynamodb", region_name="us-east-1")
        resource.create_table(
            TableName=_TABLE,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield resource.Table(_TABLE)


@pytest.fixture(autouse=True)
def _reset_named_loggers() -> Generator[None, None, None]:
    """Re-bind logger handlers each test so capsys-based assertions stay reliable."""
    for name in ("validate_schema", "transform_kpis", "ingest_to_dynamodb", "archive_files"):
        logging.getLogger(name).handlers.clear()
    yield


# ===========================================================================
# 1. validate_schema integration — exits 0 on valid, exits 1 on invalid
# ===========================================================================
def test_validate_schema_integration(s3_bucket: boto3.client) -> None:
    s3_bucket.put_object(Bucket=_BUCKET, Key="raw/streams/valid.csv", Body=_VALID_CSV)
    s3_bucket.put_object(Bucket=_BUCKET, Key="raw/streams/invalid.csv", Body=_INVALID_CSV)

    # Valid CSV — no SystemExit
    validate(bucket=_BUCKET, s3_key="raw/streams/valid.csv", s3_client=s3_bucket)

    # Invalid CSV — SystemExit(1)
    with pytest.raises(SystemExit) as exc:
        validate(bucket=_BUCKET, s3_key="raw/streams/invalid.csv", s3_client=s3_bucket)
    assert exc.value.code == 1


# ===========================================================================
# 2. transform_kpis integration — three KPI DataFrames produced from raw inputs
# ===========================================================================
def test_transform_kpis_integration(spark: SparkSession) -> None:
    streams = spark.createDataFrame(
        [
            ("u1", "t1", datetime(2026, 5, 18, 10, 0, 0)),
            ("u2", "t1", datetime(2026, 5, 18, 11, 0, 0)),
            ("u1", "t2", datetime(2026, 5, 18, 12, 0, 0)),
            ("u2", "t3", datetime(2026, 5, 19, 9, 0, 0)),
            ("u1", "t3", datetime(2026, 5, 19, 10, 0, 0)),
        ],
        schema=["user_id", "track_id", "listen_time"],
    )
    songs = spark.createDataFrame(
        [
            ("t1", "Song1", "ArtistA", "pop", 180_000),
            ("t2", "Song2", "ArtistB", "pop", 200_000),
            ("t3", "Song3", "ArtistC", "rock", 240_000),
        ],
        schema=["track_id", "track_name", "artists", "track_genre", "duration_ms"],
    )
    users = spark.createDataFrame([("u1",), ("u2",)], schema=["user_id"])

    enriched = build_enriched(streams, songs, users)
    assert enriched.count() == 5

    genre_kpis = compute_genre_kpis(enriched)
    pop_d1 = next(
        row
        for row in genre_kpis.collect()
        if row["track_genre"] == "pop" and row["listen_date"] == date(2026, 5, 18)
    )
    assert pop_d1["listen_count"] == 3
    assert pop_d1["unique_listeners"] == 2

    top_songs = rank_top_songs(enriched, top_n=3)
    assert set(top_songs.columns) == {
        "track_genre",
        "listen_date",
        "rank",
        "track_id",
        "track_name",
        "artists",
        "play_count",
    }
    assert top_songs.count() > 0

    top_genres = rank_top_genres(genre_kpis, top_n=5)
    assert set(top_genres.columns) == {
        "listen_date",
        "rank",
        "track_genre",
        "listen_count",
    }
    assert top_genres.count() > 0


# ===========================================================================
# 3. ingest_to_dynamodb integration — three Parquet-shaped DataFrames -> items
# ===========================================================================
def test_ingest_to_dynamodb_integration(kpi_table: boto3.resource) -> None:
    genre_kpis = pd.DataFrame(
        [
            {
                "track_genre": "pop",
                "listen_date": date(2024, 1, 15),
                "listen_count": 4521,
                "unique_listeners": 312,
                "total_listening_time_ms": 13_254_678,
                "avg_listening_time_per_user_ms": 42467.56,
            }
        ]
    )
    top_songs = pd.DataFrame(
        [
            {
                "track_genre": "pop",
                "listen_date": date(2024, 1, 15),
                "rank": 1,
                "track_id": "t-001",
                "track_name": "Hit",
                "artists": "Star",
                "play_count": 99,
            }
        ]
    )
    top_genres = pd.DataFrame(
        [
            {
                "listen_date": date(2024, 1, 15),
                "rank": 1,
                "track_genre": "pop",
                "listen_count": 4521,
            }
        ]
    )

    ingest_dataframes(
        table=kpi_table,
        genre_kpis=genre_kpis,
        top_songs=top_songs,
        top_genres=top_genres,
    )

    response = kpi_table.get_item(Key={"pk": "GENRE_KPI#pop#2024-01-15", "sk": "METADATA"})
    item = response["Item"]
    assert int(item["listen_count"]) == 4521
    assert int(item["unique_listeners"]) == 312
    assert int(item["total_listening_time_ms"]) == 13_254_678
    assert float(item["avg_listening_time_per_user_ms"]) == 42467.56

    # All three pk prefixes are present
    pks = {item["pk"] for item in kpi_table.scan()["Items"]}
    assert "GENRE_KPI#pop#2024-01-15" in pks
    assert "TOP_SONGS#pop#2024-01-15" in pks
    assert "TOP_GENRES#2024-01-15" in pks


# ===========================================================================
# 4. archive_files integration — copy + delete, idempotent on re-run
# ===========================================================================
def test_archive_files_integration(s3_bucket: boto3.client) -> None:
    s3_bucket.put_object(Bucket=_BUCKET, Key="raw/streams/streams1.csv", Body=_VALID_CSV)

    archive(bucket=_BUCKET, s3_key="raw/streams/streams1.csv", s3_client=s3_bucket)

    archived = s3_bucket.get_object(Bucket=_BUCKET, Key="archive/streams/streams1.csv")
    assert archived["Body"].read() == _VALID_CSV
    assert s3_bucket.list_objects_v2(Bucket=_BUCKET, Prefix="raw/streams/").get("KeyCount", 0) == 0

    # Re-running with the source already gone is a no-op that doesn't raise
    archive(bucket=_BUCKET, s3_key="raw/streams/streams1.csv", s3_client=s3_bucket)


# ===========================================================================
# 5. Multi-file concurrent — two files in sequence, DynamoDB items are additive
# ===========================================================================
def test_pipeline_multi_file_concurrent(spark: SparkSession, kpi_table: boto3.resource) -> None:
    songs = spark.createDataFrame(
        [
            ("t1", "Song1", "A", "pop", 100_000),
            ("t2", "Song2", "B", "rock", 100_000),
        ],
        schema=["track_id", "track_name", "artists", "track_genre", "duration_ms"],
    )
    users = spark.createDataFrame([("u1",), ("u2",)], schema=["user_id"])

    # --- File 1: day 2024-01-15 ---
    streams_d1 = spark.createDataFrame(
        [
            ("u1", "t1", datetime(2024, 1, 15, 10, 0, 0)),
            ("u2", "t1", datetime(2024, 1, 15, 11, 0, 0)),
        ],
        schema=["user_id", "track_id", "listen_time"],
    )
    enriched_d1 = build_enriched(streams_d1, songs, users)
    ingest_dataframes(
        table=kpi_table,
        genre_kpis=compute_genre_kpis(enriched_d1).toPandas(),
        top_songs=rank_top_songs(enriched_d1, top_n=3).toPandas(),
        top_genres=rank_top_genres(compute_genre_kpis(enriched_d1), top_n=5).toPandas(),
    )

    # --- File 2: day 2024-01-16 (different date — fully additive) ---
    streams_d2 = spark.createDataFrame(
        [
            ("u1", "t2", datetime(2024, 1, 16, 9, 0, 0)),
            ("u2", "t2", datetime(2024, 1, 16, 10, 0, 0)),
        ],
        schema=["user_id", "track_id", "listen_time"],
    )
    enriched_d2 = build_enriched(streams_d2, songs, users)
    ingest_dataframes(
        table=kpi_table,
        genre_kpis=compute_genre_kpis(enriched_d2).toPandas(),
        top_songs=rank_top_songs(enriched_d2, top_n=3).toPandas(),
        top_genres=rank_top_genres(compute_genre_kpis(enriched_d2), top_n=5).toPandas(),
    )

    pks = {item["pk"] for item in kpi_table.scan()["Items"]}
    assert "GENRE_KPI#pop#2024-01-15" in pks
    assert "GENRE_KPI#rock#2024-01-16" in pks
    assert "TOP_GENRES#2024-01-15" in pks
    assert "TOP_GENRES#2024-01-16" in pks
